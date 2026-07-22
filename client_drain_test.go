package amqpx

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/quarks-tech/amqpx/connpool"
)

// Drain mode: after cancellation the command keeps its connection and its
// clean return is honored — no force-close, nil error.
func TestRunCommandWithContextDrainWaitsForCleanReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 5 * time.Second},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		t.Fatalf("returned %v before the command finished draining", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCommand)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("error = %v, want nil (clean drain)", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return after the command drained")
	}
	select {
	case <-closeCalled:
		t.Fatal("connection was closed during a clean drain")
	default:
	}
}

// Drain mode: the command's own error passes through verbatim.
func TestRunCommandWithContextDrainReturnsCommandErrorVerbatim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	cmdErr := errors.New("handler failed mid-drain")

	// Gate the command's return on releaseCommand so cancellation always
	// lands BEFORE the error is produced — otherwise the command can win the
	// race and the test exercises the ordinary errCh path, not drain mode.
	releaseCommand := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 5 * time.Second},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return cmdErr
			},
			func(time.Time) error { return nil },
		)
	}()

	<-commandStarted
	cancel()

	// Drain mode must be entered before the command produces its error: with
	// the command still blocked, only ctx.Done() is ready, so no result may
	// arrive yet.
	select {
	case err := <-result:
		t.Fatalf("returned %v before the command produced its error", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCommand)

	select {
	case err := <-result:
		if !errors.Is(err, cmdErr) {
			t.Fatalf("error = %v, want the command's own error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return the command error")
	}
}

// Drain mode backstop: a command that ignores cancellation is force-closed at
// the drain deadline; the error matches BOTH ErrDrainTimeout and the ctx error.
func TestRunCommandWithContextDrainTimeoutForceCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	defer close(releaseCommand)
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 50 * time.Millisecond},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("error = %v, want ErrDrainTimeout", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled preserved for planned-shutdown checks", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain timeout did not fire")
	}
	select {
	case <-closeCalled:
	case <-time.After(time.Second):
		t.Fatal("connection was not force-closed at the drain deadline")
	}
}

// Negative drainTimeout waits forever: no timer, no close, command owns exit.
func TestRunCommandWithContextDrainNegativeWaitsForever(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: -1},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		t.Fatalf("returned %v; negative drain timeout must wait for the command", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseCommand)
	if err := <-result; err != nil {
		t.Fatalf("error = %v, want nil", err)
	}
	select {
	case <-closeCalled:
		t.Fatal("connection was closed despite wait-forever drain")
	default:
	}
}

// Drain mode inherits the pre-cancel guard: an already-canceled ctx never
// starts the command (identical to abort mode).
func TestRunCommandWithContextDrainDoesNotStartAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	commandCalled := make(chan struct{}, 1)
	err := runCommandWithContext(
		ctx,
		cancelPolicy{drainTimeout: time.Second},
		func() error {
			commandCalled <- struct{}{}
			return nil
		},
		func(time.Time) error { return nil },
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	select {
	case <-commandCalled:
		t.Fatal("command started with an already canceled context")
	default:
	}
}

// newDrainTestClient builds a Client over a fake pool whose dialer returns an
// in-memory connection. Unlike the bare `&amqp.Connection{}` fixture used by
// other fake-pool tests, the retry test below drives a real bad-connection
// close (io.EOF -> isBadConnErr -> conn.CloseDeadline), and amqp091-go panics
// closing a zero-value, never-dialed *amqp.Connection. So this dialer instead
// returns a real connection over an already-closed net.Pipe peer (same trick
// as newClosedClientAMQPConnection in client_coverage_test.go), which
// CloseDeadline can close safely.
func newDrainTestClient(drainTimeout time.Duration) *Client {
	cfg := &Config{DrainTimeout: drainTimeout}
	cfg.complete()
	return &Client{
		config: cfg,
		connPool: connpool.New(&connpool.Options{
			PoolSize:    1,
			PoolTimeout: time.Second,
			Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
				return newClosedDrainTestAMQPConnection(), nil, nil
			},
		}),
	}
}

// newClosedDrainTestAMQPConnection dials a real amqp.Connection whose peer is
// already gone, so it settles into a closed state that CloseDeadline can
// handle without panicking (see newDrainTestClient).
func newClosedDrainTestAMQPConnection() *amqp.Connection {
	client, server := net.Pipe()
	_ = server.Close()

	conn, _ := amqp.Open(client, amqp.Config{})

	deadline := time.Now().Add(time.Second)
	for conn != nil && !conn.IsClosed() && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	return conn
}

// End to end: cancel mid-command → command sees its own canceled ctx, drains,
// returns nil → ProcessWithDrain returns nil (no ctx.Err substitution).
func TestProcessWithDrainCleanDrain(t *testing.T) {
	client := newDrainTestClient(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	commandStarted := make(chan struct{})
	go func() {
		<-commandStarted
		cancel()
	}()

	sawCancel := false
	err := client.ProcessWithDrain(ctx, func(cmdCtx context.Context, _ *connpool.Conn) error {
		close(commandStarted)
		<-cmdCtx.Done() // the command receives the SAME ctx and observes shutdown
		sawCancel = true
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessWithDrain() error = %v, want nil (clean drain)", err)
	}
	if !sawCancel {
		t.Fatal("command did not observe the canceled ctx")
	}
}

// Pre-canceled ctx: the command never runs (parity with Process).
func TestProcessWithDrainPreCanceledDoesNotRunCommand(t *testing.T) {
	client := newDrainTestClient(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	commandCalled := false
	err := client.ProcessWithDrain(ctx, func(context.Context, *connpool.Conn) error {
		commandCalled = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ProcessWithDrain() error = %v, want context.Canceled", err)
	}
	if commandCalled {
		t.Fatal("command ran with an already canceled ctx")
	}
}

// The shared retry loop still applies: a retryable failure re-runs the
// command (consumer re-subscribe), and the retried attempt then drains clean.
func TestProcessWithDrainRetriesRetryableThenDrains(t *testing.T) {
	client := newDrainTestClient(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	secondAttemptStarted := make(chan struct{})
	go func() {
		<-secondAttemptStarted
		cancel()
	}()

	err := client.ProcessWithDrain(ctx, func(cmdCtx context.Context, _ *connpool.Conn) error {
		attempts++
		if attempts == 1 {
			return io.EOF // retryable per shouldRetry
		}
		close(secondAttemptStarted)
		<-cmdCtx.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessWithDrain() error = %v, want nil", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one retry then clean drain)", attempts)
	}
}

// A retryable-classified error returned DURING drain must pass through
// verbatim: the shutdown ctx is already canceled, so a retry attempt's
// backoff sleep would consume the cancellation and substitute
// context.Canceled for the command's real error — and re-running a command
// that already drained is wrong regardless.
func TestProcessWithDrainRetryableErrorDuringDrainReturnsVerbatim(t *testing.T) {
	client := newDrainTestClient(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	commandStarted := make(chan struct{})
	go func() {
		<-commandStarted
		cancel()
	}()

	err := client.ProcessWithDrain(ctx, func(cmdCtx context.Context, _ *connpool.Conn) error {
		attempts++
		close(commandStarted)
		<-cmdCtx.Done()
		return io.EOF // retryable-classified, returned mid-drain
	})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ProcessWithDrain() error = %v, want io.EOF verbatim", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("ProcessWithDrain() error = %v, must not be masked by context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry after drain)", attempts)
	}
}
