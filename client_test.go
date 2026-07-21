package amqpx

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/quarks-tech/amqpx/connpool"
)

type testLimiter struct {
	err     error
	allowed int
}

func (l *testLimiter) Allow() error {
	l.allowed++
	return l.err
}

func (*testLimiter) ReportResult(error) {}

type testTimeoutError struct{}

func (testTimeoutError) Error() string { return "timeout" }

func (testTimeoutError) Timeout() bool { return true }

type nilableTimeoutError struct {
	timeout bool
}

func (*nilableTimeoutError) Error() string { return "timeout" }

func (e *nilableTimeoutError) Timeout() bool { return e.timeout }

type callbackLimiter struct {
	report func(error)
}

func (*callbackLimiter) Allow() error { return nil }

func (l *callbackLimiter) ReportResult(err error) { l.report(err) }

type deadlineClearGateConn struct {
	net.Conn

	clearStarted      chan struct{}
	cancelDeadlineSet chan struct{}
	closeCalled       chan struct{}
	allowClear        chan struct{}
	clearStartedOnce  sync.Once
	cancelSetOnce     sync.Once
	closeOnce         sync.Once
	allowClearOnce    sync.Once
}

func newDeadlineClearGateConn(conn net.Conn) *deadlineClearGateConn {
	return &deadlineClearGateConn{
		Conn:              conn,
		clearStarted:      make(chan struct{}),
		cancelDeadlineSet: make(chan struct{}),
		closeCalled:       make(chan struct{}),
		allowClear:        make(chan struct{}),
	}
}

func (c *deadlineClearGateConn) SetDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		c.clearStartedOnce.Do(func() { close(c.clearStarted) })
		<-c.allowClear
	} else {
		c.cancelSetOnce.Do(func() { close(c.cancelDeadlineSet) })
		// Model the legitimate race in which the clearing deadline is applied
		// before a goroutine blocked in I/O observes the expired deadline.
		return nil
	}
	return c.Conn.SetDeadline(deadline)
}

func (c *deadlineClearGateConn) Close() error {
	c.closeOnce.Do(func() { close(c.closeCalled) })
	return c.Conn.Close()
}

func (c *deadlineClearGateConn) releaseDeadlineClear() {
	c.allowClearOnce.Do(func() { close(c.allowClear) })
}

func writeAMQPTestBytes(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func writeAMQPTestMethodFrame(
	w io.Writer,
	channel, classID, methodID uint16,
	arguments []byte,
) error {
	payload := make([]byte, 4, 4+len(arguments))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	payload = append(payload, arguments...)

	frame := make([]byte, 7, 8+len(payload))
	frame[0] = 1 // AMQP method frame.
	binary.BigEndian.PutUint16(frame[1:3], channel)
	binary.BigEndian.PutUint32(frame[3:7], uint32(len(payload)))
	frame = append(frame, payload...)
	frame = append(frame, 0xce)
	return writeAMQPTestBytes(w, frame)
}

func readAMQPTestMethodFrame(
	r io.Reader,
	wantChannel, wantClassID, wantMethodID uint16,
) error {
	var header [7]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	if header[0] != 1 {
		return fmt.Errorf("AMQP frame type = %d, want method frame", header[0])
	}
	if channel := binary.BigEndian.Uint16(header[1:3]); channel != wantChannel {
		return fmt.Errorf("AMQP frame channel = %d, want %d", channel, wantChannel)
	}

	payloadSize := binary.BigEndian.Uint32(header[3:7])
	if payloadSize < 4 || payloadSize > 1<<20 {
		return fmt.Errorf("invalid AMQP method payload size %d", payloadSize)
	}
	payloadAndEnd := make([]byte, int(payloadSize)+1)
	if _, err := io.ReadFull(r, payloadAndEnd); err != nil {
		return err
	}
	if payloadAndEnd[len(payloadAndEnd)-1] != 0xce {
		return errors.New("invalid AMQP frame terminator")
	}
	payload := payloadAndEnd[:len(payloadAndEnd)-1]
	if classID := binary.BigEndian.Uint16(payload[0:2]); classID != wantClassID {
		return fmt.Errorf("AMQP class = %d, want %d", classID, wantClassID)
	}
	if methodID := binary.BigEndian.Uint16(payload[2:4]); methodID != wantMethodID {
		return fmt.Errorf("AMQP method = %d, want %d", methodID, wantMethodID)
	}
	return nil
}

func appendAMQPTestLongString(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

func serveAMQPTestConnectionUntilChannelOpen(conn net.Conn) error {
	var protocolHeader [8]byte
	if _, err := io.ReadFull(conn, protocolHeader[:]); err != nil {
		return fmt.Errorf("read protocol header: %w", err)
	}
	if got, want := string(protocolHeader[:]), "AMQP\x00\x00\x09\x01"; got != want {
		return fmt.Errorf("protocol header = %q, want %q", got, want)
	}

	startArguments := []byte{0, 9}
	startArguments = binary.BigEndian.AppendUint32(startArguments, 0) // Empty server properties table.
	startArguments = appendAMQPTestLongString(startArguments, "PLAIN")
	startArguments = appendAMQPTestLongString(startArguments, "en_US")
	if err := writeAMQPTestMethodFrame(conn, 0, 10, 10, startArguments); err != nil {
		return fmt.Errorf("write connection.start: %w", err)
	}
	if err := readAMQPTestMethodFrame(conn, 0, 10, 11); err != nil {
		return fmt.Errorf("read connection.start-ok: %w", err)
	}

	tuneArguments := make([]byte, 8)
	binary.BigEndian.PutUint16(tuneArguments[0:2], 0)
	binary.BigEndian.PutUint32(tuneArguments[2:6], 128*1024)
	binary.BigEndian.PutUint16(tuneArguments[6:8], 0)
	if err := writeAMQPTestMethodFrame(conn, 0, 10, 30, tuneArguments); err != nil {
		return fmt.Errorf("write connection.tune: %w", err)
	}
	if err := readAMQPTestMethodFrame(conn, 0, 10, 31); err != nil {
		return fmt.Errorf("read connection.tune-ok: %w", err)
	}
	if err := readAMQPTestMethodFrame(conn, 0, 10, 40); err != nil {
		return fmt.Errorf("read connection.open: %w", err)
	}
	if err := writeAMQPTestMethodFrame(conn, 0, 10, 41, []byte{0}); err != nil {
		return fmt.Errorf("write connection.open-ok: %w", err)
	}
	if err := readAMQPTestMethodFrame(conn, 1, 20, 10); err != nil {
		return fmt.Errorf("read channel.open: %w", err)
	}
	return nil
}

func TestConnectionURL(t *testing.T) {
	tests := map[string]string{
		"":                                   "amqp://",
		"guest:guest@localhost:5672/":        "amqp://guest:guest@localhost:5672/",
		"amqp://guest:guest@localhost:5672/": "amqp://guest:guest@localhost:5672/",
		"amqps://rabbitmq.example/":          "amqps://rabbitmq.example/",
	}

	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			if got := connectionURL(input); got != want {
				t.Fatalf("connectionURL(%q) = %q, want %q", input, got, want)
			}
		})
	}
}

func TestNewClientAcceptsNilConfig(t *testing.T) {
	client := NewClient(nil)
	t.Cleanup(func() { _ = client.Close() })

	if client.config == nil {
		t.Fatal("NewClient(nil) retained a nil config")
	}
	if client.config.MaxRetries != 3 {
		t.Fatalf("MaxRetries = %d, want 3", client.config.MaxRetries)
	}
}

func TestNewClientSnapshotsConfig(t *testing.T) {
	cfg := &Config{
		Address:         "rabbitmq:5672",
		MaxRetries:      -1,
		MinRetryBackoff: -1,
		MaxRetryBackoff: -1,
	}

	client := NewClient(cfg)
	t.Cleanup(func() { _ = client.Close() })

	if cfg.MaxRetries != -1 || cfg.MinRetryBackoff != -1 || cfg.MaxRetryBackoff != -1 {
		t.Fatalf("NewClient mutated caller config: %+v", cfg)
	}
	if client.config == cfg {
		t.Fatal("client retained caller config pointer")
	}

	cfg.Address = "changed:5672"
	if client.config.Address != "rabbitmq:5672" {
		t.Fatalf("client address changed with caller config: %q", client.config.Address)
	}
}

func TestNewClientClonesMutableAMQPConfigContainers(t *testing.T) {
	auth := &amqp.PlainAuth{Username: "original", Password: "secret"}
	tlsConfig := &tls.Config{ServerName: "original.example"}
	cfg := &Config{AMQP: amqp.Config{
		SASL:            []amqp.Authentication{auth},
		Properties:      amqp.Table{"product": "original"},
		TLSClientConfig: tlsConfig,
	}}

	client := NewClient(cfg)
	t.Cleanup(func() { _ = client.Close() })

	auth.Username = "changed"
	auth.Password = ""
	cfg.AMQP.SASL[0] = &amqp.PlainAuth{Username: "changed"}
	cfg.AMQP.Properties["product"] = "changed"
	cfg.AMQP.TLSClientConfig.ServerName = "changed.example"

	clientAuth := client.config.AMQP.SASL[0].(*amqp.PlainAuth)
	if clientAuth == auth {
		t.Fatal("client retained caller PlainAuth pointer")
	}
	if clientAuth.Username != "original" || clientAuth.Password != "secret" {
		t.Fatalf("client PlainAuth = %+v, want original credentials", clientAuth)
	}
	if got := client.config.AMQP.Properties["product"]; got != "original" {
		t.Fatalf("client property = %v, want original", got)
	}
	if got := client.config.AMQP.TLSClientConfig.ServerName; got != "original.example" {
		t.Fatalf("client TLS server name = %q, want original.example", got)
	}
	if cfg.AMQP.Dial != nil {
		t.Fatal("NewClient installed its default dialer into caller config")
	}
}

func TestCloneAMQPConfigProducesIndependentContainers(t *testing.T) {
	base := amqp.Config{
		SASL:            []amqp.Authentication{&amqp.PlainAuth{Username: "original", Password: "secret"}},
		Properties:      amqp.Table{"product": "original"},
		TLSClientConfig: &tls.Config{ServerName: "original.example"},
	}
	first := cloneAMQPConfig(base)
	second := cloneAMQPConfig(base)

	firstAuth := first.SASL[0].(*amqp.PlainAuth)
	secondAuth := second.SASL[0].(*amqp.PlainAuth)
	baseAuth := base.SASL[0].(*amqp.PlainAuth)
	firstAuth.Password = ""
	first.Properties["product"] = "changed"
	first.TLSClientConfig.ServerName = "changed.example"

	if firstAuth == baseAuth || secondAuth == baseAuth || firstAuth == secondAuth {
		t.Fatal("PlainAuth pointer is shared between config clones")
	}
	if got := baseAuth.Password; got != "secret" {
		t.Fatalf("base PlainAuth password = %q, want it unchanged", got)
	}
	if got := secondAuth.Password; got != "secret" {
		t.Fatalf("second PlainAuth password = %q, want it unchanged", got)
	}
	if got := second.Properties["product"]; got != "original" {
		t.Fatalf("second Properties value = %v, want original", got)
	}
	if got := second.TLSClientConfig.ServerName; got != "original.example" {
		t.Fatalf("second TLS ServerName = %q, want original.example", got)
	}
}

func TestCloneAMQPConfigClonesAMQPlainCredentials(t *testing.T) {
	baseAuth := &amqp.AMQPlainAuth{Username: "original", Password: "secret"}
	first := cloneAMQPConfig(amqp.Config{SASL: []amqp.Authentication{baseAuth}})
	second := cloneAMQPConfig(amqp.Config{SASL: []amqp.Authentication{baseAuth}})

	firstAuth := first.SASL[0].(*amqp.AMQPlainAuth)
	secondAuth := second.SASL[0].(*amqp.AMQPlainAuth)
	firstAuth.Password = ""

	if firstAuth == baseAuth || secondAuth == baseAuth || firstAuth == secondAuth {
		t.Fatal("AMQPlainAuth pointer is shared between config clones")
	}
	if got := baseAuth.Password; got != "secret" {
		t.Fatalf("base AMQPlainAuth password = %q, want it unchanged", got)
	}
	if got := secondAuth.Password; got != "secret" {
		t.Fatalf("second AMQPlainAuth password = %q, want it unchanged", got)
	}
}

func TestNegativeRetrySettingsDisableRetriesAndBackoff(t *testing.T) {
	limiterErr := errors.New("not allowed")
	limiter := &testLimiter{err: limiterErr}
	client := NewClient(&Config{
		MaxRetries:      -99,
		MinRetryBackoff: -2 * time.Second,
		MaxRetryBackoff: -3 * time.Second,
		Limiter:         limiter,
	})
	t.Cleanup(func() { _ = client.Close() })

	if client.config.MaxRetries != 0 {
		t.Fatalf("MaxRetries = %d, want 0", client.config.MaxRetries)
	}
	if client.config.MinRetryBackoff != 0 || client.config.MaxRetryBackoff != 0 {
		t.Fatalf("retry backoff = (%s, %s), want both disabled", client.config.MinRetryBackoff, client.config.MaxRetryBackoff)
	}

	err := client.Process(context.Background(), func(context.Context, *connpool.Conn) error {
		t.Fatal("command must not run when limiter rejects it")
		return nil
	})
	if !errors.Is(err, limiterErr) {
		t.Fatalf("Process() error = %v, want %v", err, limiterErr)
	}
	if limiter.allowed != 1 {
		t.Fatalf("Allow calls = %d, want exactly one initial attempt", limiter.allowed)
	}
}

func TestReleaseConnReturnsLeaseBeforeReportingLimiterResult(t *testing.T) {
	pool := connpool.New(&connpool.Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	cn, err := pool.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	reentered := make(chan error, 1)
	client := &Client{config: &Config{}, connPool: pool}
	client.config.Limiter = &callbackLimiter{report: func(error) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		reentrantConn, getErr := pool.Get(ctx)
		if getErr == nil {
			pool.Put(ctx, reentrantConn)
		}
		reentered <- getErr
	}}

	client.releaseConn(context.Background(), cn, nil)
	if err := <-reentered; err != nil {
		t.Fatalf("Limiter.ReportResult could not reenter the released pool: %v", err)
	}
}

func TestNewClientRejectsAMQPRecovery(t *testing.T) {
	defer func() {
		got := recover()
		if got == nil {
			t.Fatal("NewClient accepted experimental AMQP recovery")
		}
		if !strings.Contains(fmt.Sprint(got), "recovery") {
			t.Fatalf("panic = %q, want a clear recovery error", got)
		}
	}()

	_ = NewClient(&Config{AMQP: amqp.Config{Recovery: &amqp.Recovery{}}})
}

func TestNewClientRejectsInvalidConfigClearly(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "negative pool size", cfg: Config{PoolSize: -1}, want: "PoolSize"},
		{name: "negative minimum idle", cfg: Config{MinIdleConns: -1}, want: "MinIdleConns"},
		{name: "negative pool timeout", cfg: Config{PoolTimeout: -1}, want: "PoolTimeout"},
		{name: "negative dial timeout", cfg: Config{DialTimeout: -1}, want: "DialTimeout"},
		{name: "minimum idle exceeds pool", cfg: Config{PoolSize: 1, MinIdleConns: 2}, want: "MinIdleConns"},
		{
			name: "maximum backoff below minimum",
			cfg: Config{
				MinRetryBackoff: time.Second,
				MaxRetryBackoff: 500 * time.Millisecond,
			},
			want: "MaxRetryBackoff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				got := recover()
				if got == nil {
					t.Fatalf("NewClient(%+v) did not panic", tt.cfg)
				}
				if !strings.Contains(fmt.Sprint(got), tt.want) {
					t.Fatalf("panic = %q, want it to identify %s", got, tt.want)
				}
			}()

			client := NewClient(&tt.cfg)
			_ = client.Close()
		})
	}
}

func TestProcessCancelsDialPromptly(t *testing.T) {
	dialStarted := make(chan struct{}, 1)
	releaseDial := make(chan struct{})
	commandCalled := make(chan struct{}, 1)
	released := false
	release := func() {
		if !released {
			close(releaseDial)
			released = true
		}
	}
	defer release()

	client := NewClient(&Config{
		Address:    "rabbitmq:5672",
		MaxRetries: -1,
		PoolSize:   1,
		AMQP: amqp.Config{Dial: func(string, string) (net.Conn, error) {
			select {
			case dialStarted <- struct{}{}:
			default:
			}
			<-releaseDial
			return nil, errors.New("dial released")
		}},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- client.Process(ctx, func(context.Context, *connpool.Conn) error {
			commandCalled <- struct{}{}
			return nil
		})
	}()

	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Process() error = %v, want context.Canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		release()
		<-result
		t.Fatal("Process did not return promptly after dial cancellation")
	}

	select {
	case <-commandCalled:
		t.Fatal("command ran after its context was canceled during dial")
	default:
	}
}

func TestDialCancellationClosesHandshakeTransport(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	ctx, cancel := context.WithCancel(t.Context())
	resultCh := make(chan dialResult, 1)
	go func() {
		resultCh <- dialAMQP(ctx, "amqp://guest:guest@localhost:5672/", amqp.Config{
			Dial: func(string, string) (net.Conn, error) {
				return clientConn, nil
			},
		}, 5*time.Second)
	}()

	if err := serverConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	var protocolHeader [8]byte
	if _, err := io.ReadFull(serverConn, protocolHeader[:]); err != nil {
		t.Fatalf("reading AMQP protocol header: %v", err)
	}
	if err := serverConn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clearing read deadline: %v", err)
	}
	cancel()

	select {
	case result := <-resultCh:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("dialAMQP() error = %v, want context.Canceled", result.err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("dialAMQP did not return promptly after handshake cancellation")
	}

	peerClosed := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, readErr := serverConn.Read(buf[:])
		peerClosed <- readErr
	}()
	select {
	case <-peerClosed:
	case <-time.After(250 * time.Millisecond):
		_ = serverConn.Close()
		<-peerClosed
		t.Fatal("canceled dial left its handshake transport open")
	}
}

func TestDialCancellationSurvivesUpstreamDeadlineClear(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	gatedConn := newDeadlineClearGateConn(clientConn)
	t.Cleanup(func() {
		gatedConn.releaseDeadlineClear()
		_ = gatedConn.Close()
		_ = serverConn.Close()
	})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveAMQPTestConnectionUntilChannelOpen(serverConn)
	}()

	ctx, cancel := context.WithCancel(t.Context())
	resultCh := make(chan dialResult, 1)
	go func() {
		resultCh <- dialAMQP(ctx, "amqp://guest:guest@localhost:5672/", amqp.Config{
			Dial: func(string, string) (net.Conn, error) {
				return gatedConn, nil
			},
		}, 5*time.Second)
	}()

	select {
	case <-gatedConn.clearStarted:
	case <-time.After(time.Second):
		t.Fatal("upstream did not try to clear the handshake deadline")
	}
	cancel()
	select {
	case <-gatedConn.cancelDeadlineSet:
	case <-gatedConn.closeCalled:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt the handshake transport")
	}
	gatedConn.releaseDeadlineClear()

	select {
	case result := <-resultCh:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("dialAMQP() error = %v, want context.Canceled", result.err)
		}
	case <-time.After(250 * time.Millisecond):
		_ = serverConn.Close()
		<-resultCh
		t.Fatal("upstream deadline clearing undid dial cancellation")
	}

	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("test AMQP server did not stop after cancellation")
	}
}

func TestDialTimeoutBoundsInitialChannelOpen(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- serveAMQPTestConnectionUntilChannelOpen(serverConn)
	}()

	const dialTimeout = 100 * time.Millisecond
	resultCh := make(chan dialResult, 1)
	go func() {
		conn, ch, err := dialAMQPContext(
			context.Background(),
			"amqp://guest:guest@localhost:5672/",
			amqp.Config{Dial: func(string, string) (net.Conn, error) {
				return clientConn, nil
			}},
			dialTimeout,
		)
		resultCh <- dialResult{conn: conn, ch: ch, err: err}
	}()

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("test AMQP handshake: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not reach channel.open")
	}

	select {
	case result := <-resultCh:
		if result.conn != nil || result.ch != nil {
			t.Fatal("timed-out dial returned a connection or channel")
		}
		if _, ok := errors.AsType[timeoutError](result.err); !ok {
			t.Fatalf("dialAMQPContext() error = %v, want a timeout error", result.err)
		}
		if errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("dial timeout was confused with the caller context deadline: %v", result.err)
		}
	case <-time.After(500 * time.Millisecond):
		_ = serverConn.Close()
		<-resultCh
		t.Fatal("DialTimeout did not bound the initial channel.open")
	}
}

func TestRunCommandWithContextDoesNotWaitForStuckCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	closeDeadline := make(chan time.Time, 1)
	defer close(releaseCommand)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(deadline time.Time) error {
				closeDeadline <- deadline
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runCommandWithContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("runCommandWithContext waited for a command that ignored cancellation")
	}

	select {
	case deadline := <-closeDeadline:
		if time.Until(deadline) > time.Second {
			t.Fatalf("close deadline %s is not bounded", deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("connection close was not requested")
	}
}

func TestRunCommandWithContextDoesNotStartAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	commandCalled := make(chan struct{}, 1)
	closeCalled := make(chan struct{}, 1)
	err := runCommandWithContext(
		ctx,
		func() error {
			commandCalled <- struct{}{}
			return nil
		},
		func(time.Time) error {
			closeCalled <- struct{}{}
			return nil
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runCommandWithContext() error = %v, want context.Canceled", err)
	}
	select {
	case <-commandCalled:
		t.Fatal("command started with an already canceled context")
	default:
	}
	select {
	case <-closeCalled:
	case <-time.After(time.Second):
		t.Fatal("connection close was not requested")
	}
}

func TestCloseConnectionWithDeadlineBoundsWait(t *testing.T) {
	closeStarted := make(chan time.Time, 1)
	releaseClose := make(chan struct{})
	closeFinished := make(chan struct{})
	released := false
	release := func() {
		if !released {
			close(releaseClose)
			released = true
		}
	}
	defer release()

	startedAt := time.Now()
	closeConnectionWithDeadline(func(deadline time.Time) error {
		closeStarted <- deadline
		<-releaseClose
		close(closeFinished)
		return nil
	})
	elapsed := time.Since(startedAt)
	if elapsed > cancelCloseTimeout+250*time.Millisecond {
		release()
		<-closeFinished
		t.Fatalf("close wait = %s, want a bounded wait near %s", elapsed, cancelCloseTimeout)
	}

	select {
	case deadline := <-closeStarted:
		if deadline.Before(startedAt) || deadline.After(startedAt.Add(cancelCloseTimeout+50*time.Millisecond)) {
			release()
			<-closeFinished
			t.Fatalf("close deadline = %s, want a deadline near %s", deadline, startedAt.Add(cancelCloseTimeout))
		}
	case <-time.After(time.Second):
		t.Fatal("connection close was not started")
	}

	release()
	select {
	case <-closeFinished:
	case <-time.After(time.Second):
		t.Fatal("connection close did not finish after it was released")
	}
}

func TestIsBadConnErrKeepsApplicationErrors(t *testing.T) {
	if isBadConnErr(errors.New("application failure")) {
		t.Fatal("application error marked a healthy connection as bad")
	}
}

func TestShouldRetryWrappedErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "EOF", err: fmt.Errorf("dial: %w", io.EOF), want: true},
		{name: "unexpected EOF", err: fmt.Errorf("dial: %w", io.ErrUnexpectedEOF), want: true},
		{name: "timeout", err: fmt.Errorf("dial: %w", testTimeoutError{}), want: true},
		{name: "canceled", err: fmt.Errorf("command: %w", context.Canceled), want: false},
		{name: "deadline", err: fmt.Errorf("command: %w", context.DeadlineExceeded), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRetry(tt.err); got != tt.want {
				t.Fatalf("shouldRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldRetryHandlesTypedNilTimeoutError(t *testing.T) {
	var timeoutErr *nilableTimeoutError
	var err error = timeoutErr
	if !shouldRetry(err) {
		t.Fatal("typed-nil timeout error was not retryable")
	}
}

func TestAMQPClassificationHandlesTypedNilError(t *testing.T) {
	var amqpErr *amqp.Error
	var err error = amqpErr

	if isBadConnErr(err) {
		t.Fatal("typed nil AMQP error marked connection as bad")
	}
	if shouldRetry(err) {
		t.Fatal("typed nil AMQP error marked retryable")
	}
}

func TestIsBadConnErrHandlesAMQPError(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "direct",
			err:  &amqp.Error{Code: amqp.ConnectionForced},
		},
		{
			name: "wrapped",
			err:  fmt.Errorf("wrapped: %w", &amqp.Error{Code: amqp.ChannelError}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBadConnErr(tt.err); !got {
				t.Fatal("isBadConnErr() = false, want true")
			}
		})
	}
}

func TestShouldRetryHandlesAMQPError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "direct connection error",
			err:  &amqp.Error{Code: amqp.ConnectionForced},
			want: true,
		},
		{
			name: "wrapped channel error",
			err:  fmt.Errorf("wrapped: %w", &amqp.Error{Code: amqp.ChannelError}),
			want: true,
		},
		{
			name: "wrapped internal error",
			err:  fmt.Errorf("wrapped: %w", &amqp.Error{Code: amqp.InternalError}),
			want: true,
		},
		{
			name: "non-retryable AMQP error",
			err:  &amqp.Error{Code: amqp.NotFound},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRetry(tt.err); got != tt.want {
				t.Fatalf("shouldRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}
