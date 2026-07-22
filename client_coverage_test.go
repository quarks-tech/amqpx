package amqpx

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/quarks-tech/amqpx/connpool"
)

type sequenceLimiter struct {
	errs     []error
	allowed  int
	reported []error
}

func (l *sequenceLimiter) Allow() error {
	index := l.allowed
	l.allowed++
	if index < len(l.errs) {
		return l.errs[index]
	}
	return nil
}

func (l *sequenceLimiter) ReportResult(err error) {
	l.reported = append(l.reported, err)
}

func newCoverageClient(t *testing.T, cfg *Config) *Client {
	t.Helper()

	pool := connpool.New(&connpool.Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	// The zero-value upstream connection is an intentionally inert test double:
	// amqp091-go cannot close it because its lifecycle is uninitialized.
	// This pool starts no background workers and owns no external resources.
	return &Client{config: cfg, connPool: pool}
}

func TestProcessRetriesLimiterTimeoutThenSucceeds(t *testing.T) {
	limiter := &sequenceLimiter{errs: []error{testTimeoutError{}, testTimeoutError{}}}
	client := newCoverageClient(t, &Config{
		MaxRetries:      2,
		MinRetryBackoff: 0,
		MaxRetryBackoff: 0,
		Limiter:         limiter,
	})

	commandCalls := 0
	err := client.Process(context.Background(), func(context.Context, *connpool.Conn) error {
		commandCalls++
		return nil
	})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if limiter.allowed != 3 {
		t.Fatalf("Allow calls = %d, want 3", limiter.allowed)
	}
	if commandCalls != 1 {
		t.Fatalf("command calls = %d, want 1", commandCalls)
	}
	if len(limiter.reported) != 1 || limiter.reported[0] != nil {
		t.Fatalf("reported results = %v, want [nil]", limiter.reported)
	}
}

func TestProcessStopsAtRetryLimit(t *testing.T) {
	limiter := &sequenceLimiter{errs: []error{
		testTimeoutError{},
		testTimeoutError{},
		testTimeoutError{},
		testTimeoutError{},
	}}
	client := newCoverageClient(t, &Config{MaxRetries: 2, Limiter: limiter})

	err := client.Process(context.Background(), func(context.Context, *connpool.Conn) error {
		t.Fatal("command ran while limiter rejected every attempt")
		return nil
	})
	if _, ok := errors.AsType[testTimeoutError](err); !ok {
		t.Fatalf("Process() error = %v, want timeout error", err)
	}
	if limiter.allowed != 3 {
		t.Fatalf("Allow calls = %d, want initial attempt plus 2 retries", limiter.allowed)
	}
}

func TestProcessReturnsCommandErrorAndReusesConnection(t *testing.T) {
	client := newCoverageClient(t, &Config{})
	wantErr := errors.New("application failure")

	err := client.Process(context.Background(), func(context.Context, *connpool.Conn) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Process() error = %v, want %v", err, wantErr)
	}
	if got := client.connPool.IdleLen(); got != 1 {
		t.Fatalf("idle connections = %d, want application failure to preserve the connection", got)
	}
}

func TestProcessDiscardsBadConnectionBeforeRetry(t *testing.T) {
	var dialCalls int
	pool := connpool.New(&connpool.Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			dialCalls++
			return newClosedClientAMQPConnection(t), nil, nil
		},
	})
	t.Cleanup(func() { _ = pool.Close() })
	client := &Client{
		config: &Config{
			MaxRetries:      1,
			MinRetryBackoff: 0,
			MaxRetryBackoff: 0,
		},
		connPool: pool,
	}

	attempts := 0
	err := client.Process(context.Background(), func(context.Context, *connpool.Conn) error {
		attempts++
		if attempts == 1 {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if attempts != 2 || dialCalls != 2 {
		t.Fatalf("attempts/dials = %d/%d, want 2/2", attempts, dialCalls)
	}
	if stats := pool.Stats(); stats.TotalConns != 1 || stats.IdleConns != 1 || stats.Misses != 2 {
		t.Fatalf("Stats() = %+v, want two dials and one replacement idle connection", stats)
	}
}

func newClosedClientAMQPConnection(t *testing.T) *amqp.Connection {
	t.Helper()

	client, server := net.Pipe()
	if err := server.Close(); err != nil {
		t.Fatalf("close in-memory AMQP peer: %v", err)
	}
	conn, err := amqp.Open(client, amqp.Config{})
	if conn == nil {
		t.Fatalf("amqp.Open() connection = nil, error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !conn.IsClosed() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !conn.IsClosed() {
		t.Fatal("in-memory AMQP connection did not observe its closed peer")
	}
	return conn
}

func TestRetryBackoffBounds(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		if got := retryBackoff(4, 0, time.Second); got != 0 {
			t.Fatalf("retryBackoff() = %s, want 0", got)
		}
	})

	t.Run("jittered", func(t *testing.T) {
		const (
			minBackoff = 10 * time.Millisecond
			maxBackoff = 500 * time.Millisecond
		)
		for range 100 {
			got := retryBackoff(2, minBackoff, maxBackoff)
			if got < minBackoff || got >= 5*minBackoff {
				t.Fatalf("retryBackoff() = %s, want [%s, %s)", got, minBackoff, 5*minBackoff)
			}
		}
	})

	t.Run("capped", func(t *testing.T) {
		const maxBackoff = 25 * time.Millisecond
		if got := retryBackoff(8, maxBackoff, maxBackoff); got != maxBackoff {
			t.Fatalf("retryBackoff() = %s, want cap %s", got, maxBackoff)
		}
	})

	t.Run("overflow", func(t *testing.T) {
		const maxBackoff = time.Second
		if got := retryBackoff(63, time.Millisecond, maxBackoff); got != maxBackoff {
			t.Fatalf("retryBackoff() = %s, want overflow cap %s", got, maxBackoff)
		}
	})

	t.Run("negative retry panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("retryBackoff() did not panic for a negative retry")
			}
		}()
		_ = retryBackoff(-1, time.Millisecond, time.Second)
	})
}

func TestSleepWithContext(t *testing.T) {
	if err := sleepWithContext(context.Background(), 0); err != nil {
		t.Fatalf("zero-duration sleep error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepWithContext(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled sleep error = %v, want context.Canceled", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sleepWithContext(ctx, time.Hour) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled sleep error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sleep did not stop after context cancellation")
	}
}

func TestRunCommandWithContextSynchronousPaths(t *testing.T) {
	wantErr := errors.New("command error")
	closeCalls := 0
	err := runCommandWithContext(
		context.Background(),
		cancelPolicy{},
		func() error { return wantErr },
		func(time.Time) error {
			closeCalls++
			return nil
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("runCommandWithContext() error = %v, want %v", err, wantErr)
	}
	if closeCalls != 0 {
		t.Fatalf("close calls = %d, want 0", closeCalls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runCommandWithContext(ctx, cancelPolicy{}, func() error { return nil }, func(time.Time) error {
		t.Fatal("successful command closed its connection")
		return nil
	}); err != nil {
		t.Fatalf("runCommandWithContext() error = %v", err)
	}
}

func TestConnectionSetupTimeoutErrorContract(t *testing.T) {
	err := connectionSetupTimeoutError{timeout: 250 * time.Millisecond}
	if got := err.Error(); got != "amqpx: connection setup timed out after 250ms" {
		t.Fatalf("Error() = %q", got)
	}
	if !err.Timeout() || !err.Temporary() {
		t.Fatal("connection setup timeout must retain net.Error compatibility")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("errors.Is(%v, os.ErrDeadlineExceeded) = false", err)
	}
	var netErr net.Error = err
	if !netErr.Timeout() {
		t.Fatal("net.Error.Timeout() = false")
	}
}

func TestDialContextErrorPrefersCallerCancellation(t *testing.T) {
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	cancelCaller()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), time.Hour)
	defer cancelDial()
	if err := dialContextError(callerCtx, dialCtx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("dialContextError() = %v, want caller cancellation", err)
	}

	timedOutCtx, cancelTimeout := context.WithTimeout(context.Background(), 0)
	defer cancelTimeout()
	if err := dialContextError(context.Background(), timedOutCtx, 2*time.Second); err == nil {
		t.Fatal("dialContextError() returned nil for expired dial context")
	} else if timeoutErr, ok := errors.AsType[connectionSetupTimeoutError](err); !ok || timeoutErr.timeout != 2*time.Second {
		t.Fatalf("dialContextError() = %v, want 2s setup timeout", err)
	}
}

func TestDialHelpersHandleCancellationAndRawTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, ch, err := dialAMQPContext(ctx, "amqp://unused", amqp.Config{}, time.Second)
	if conn != nil || ch != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("dialAMQPContext() = (%v, %v, %v), want canceled without resources", conn, ch, err)
	}

	resultCh := make(chan dialResult, 1)
	want := dialResult{err: io.EOF}
	sendDialResult(context.Background(), resultCh, want)
	if got := <-resultCh; !errors.Is(got.err, io.EOF) {
		t.Fatalf("sendDialResult() delivered %v, want EOF", got.err)
	}

	canceledCtx, cancelSend := context.WithCancel(context.Background())
	cancelSend()
	sendDialResult(canceledCtx, make(chan dialResult), dialResult{})

	clientConn, peerConn := net.Pipe()
	if err := peerConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	closeDialResult(dialResult{}, clientConn)
	var buf [1]byte
	if _, err := peerConn.Read(buf[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("peer read error = %v, want EOF from closed raw transport", err)
	}
	_ = peerConn.Close()
}

func TestOpenAMQPConnectionRejectsMalformedURL(t *testing.T) {
	result := openAMQPConnection(":// malformed", amqp.Config{})
	if result.conn != nil || result.ch != nil || result.err == nil {
		t.Fatalf("openAMQPConnection() = %+v, want parse failure without resources", result)
	}
	if errors.Unwrap(result.err) == nil {
		t.Fatal("dial error did not preserve its cause")
	}
}
