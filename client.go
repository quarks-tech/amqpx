package amqpx

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"math/rand/v2"

	"github.com/quarks-tech/amqpx/connpool"
)

// Command performs one AMQP operation attempt on an exclusively leased
// connection and channel. A Command must not retain conn after returning.
// Under Process it should stop promptly when ctx is canceled; under
// ProcessWithDrain it owns its shutdown instead — observe ctx, stop intake,
// drain, and return within Config.DrainTimeout.
type Command func(ctx context.Context, conn *connpool.Conn) error

// Client executes AMQP commands with connection pooling and bounded retries.
type Client struct {
	config   *Config
	connPool *connpool.ConnPool
}

const cancelCloseTimeout = 100 * time.Millisecond

// ErrDrainTimeout reports that a ProcessWithDrain command did not finish
// within Config.DrainTimeout after its context was canceled; the borrowed
// connection was force-closed as a backstop. It is returned joined with the
// context error, so errors.Is(err, context.Canceled) also holds.
var ErrDrainTimeout = errors.New("amqpx: drain timeout exceeded")

// cancelPolicy selects what happens to the borrowed connection when ctx
// cancels mid-command. The zero value is abort mode (Process): force-close
// the connection immediately and return ctx.Err(). A non-zero drainTimeout
// waits that long for the command to return on its own before the
// force-close backstop fires; a negative value waits forever.
type cancelPolicy struct {
	drainTimeout time.Duration
}

// NewClient creates a client from a snapshot of config. A nil config selects
// all defaults. NewClient panics for invalid settings and when the
// experimental amqp091-go recovery feature is enabled, because recovery is not
// compatible with this client's pool lifecycle.
func NewClient(config *Config) *Client {
	cfg := &Config{}
	if config != nil {
		*cfg = *config
		cfg.AMQP = cloneAMQPConfig(config.AMQP)
	}
	if cfg.AMQP.Recovery != nil {
		panic("amqpx: experimental AMQP recovery is not supported")
	}
	cfg.complete()
	if err := cfg.validate(); err != nil {
		panic(err)
	}

	return &Client{
		config:   cfg,
		connPool: newConnPool(cfg),
	}
}

func cloneAMQPConfig(cfg amqp.Config) amqp.Config {
	cfg.SASL = slices.Clone(cfg.SASL)
	for i, authentication := range cfg.SASL {
		switch authentication := authentication.(type) {
		case *amqp.PlainAuth:
			if authentication != nil {
				clone := *authentication
				cfg.SASL[i] = &clone
			}
		case *amqp.AMQPlainAuth:
			if authentication != nil {
				clone := *authentication
				cfg.SASL[i] = &clone
			}
		}
	}
	cfg.Properties = maps.Clone(cfg.Properties)
	if cfg.TLSClientConfig != nil {
		cfg.TLSClientConfig = cfg.TLSClientConfig.Clone()
	}
	return cfg
}

func (c *Client) getConn(ctx context.Context) (*connpool.Conn, error) {
	if c.config.Limiter != nil {
		err := c.config.Limiter.Allow()
		if err != nil {
			return nil, err
		}
	}

	conn, err := c.connPool.Get(ctx)
	if err != nil {
		if c.config.Limiter != nil {
			c.config.Limiter.ReportResult(err)
		}
		return nil, err
	}

	return conn, nil
}

func (c *Client) releaseConn(ctx context.Context, conn *connpool.Conn, err error) {
	if isBadConnErr(err) {
		// Bound the AMQP close handshake before handing the connection back to
		// the pool. Remove remains synchronous for connpool callers, but the
		// close it performs is then an immediate no-op for this client.
		closeConnectionWithDeadline(conn.CloseDeadline)
		c.connPool.Remove(ctx, conn, err)
	} else {
		c.connPool.Put(ctx, conn)
	}

	if c.config.Limiter != nil {
		c.config.Limiter.ReportResult(err)
	}
}

func (c *Client) withConn(ctx context.Context, fn Command, policy cancelPolicy) error {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}

	defer func() {
		c.releaseConn(ctx, conn, err)
	}()

	err = runCommandWithContext(
		ctx,
		policy,
		func() error {
			return fn(ctx, conn)
		},
		func(deadline time.Time) error {
			return conn.CloseDeadline(deadline)
		},
	)
	return err
}

func runCommandWithContext(
	ctx context.Context,
	policy cancelPolicy,
	run func() error,
	closeConn func(time.Time) error,
) error {
	if err := ctx.Err(); err != nil {
		closeConnectionWithDeadline(closeConn)
		return err
	}
	if ctx.Done() == nil {
		return run()
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- run()
	}()

	select {
	case <-ctx.Done():
		if policy.drainTimeout != 0 {
			return drainCommand(ctx.Err(), policy.drainTimeout, errCh, closeConn)
		}
		closeConnectionWithDeadline(closeConn)
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// drainCommand waits for an already-canceled command to finish on its own:
// the command owns its shutdown (observe ctx, stop intake, drain, return)
// and its error is returned verbatim. A bounded wait force-closes the
// connection at the deadline as the backstop, so a wedged drain cannot hang
// shutdown forever; a negative timeout waits forever.
func drainCommand(
	ctxErr error,
	timeout time.Duration,
	errCh <-chan error,
	closeConn func(time.Time) error,
) error {
	if timeout < 0 {
		return <-errCh
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-errCh:
		return err
	case <-timer.C:
		closeConnectionWithDeadline(closeConn)
		return errors.Join(ErrDrainTimeout, ctxErr)
	}
}

func closeConnectionWithDeadline(closeConn func(time.Time) error) {
	deadline := time.Now().Add(cancelCloseTimeout)
	closeCh := make(chan struct{})
	go func() {
		_ = closeConn(deadline)
		close(closeCh)
	}()

	timer := time.NewTimer(cancelCloseTimeout)
	defer timer.Stop()
	select {
	case <-closeCh:
	case <-timer.C:
	}
}

// Process executes cmd and retries retryable transport and AMQP failures up to
// Config.MaxRetries times. Context cancellation stops acquisition, backoff, and
// waiting for a command attempt.
func (c *Client) Process(ctx context.Context, cmd Command) error {
	return c.process(ctx, cmd, cancelPolicy{})
}

// ProcessWithDrain executes cmd like Process, but for long-lived commands
// (consumers): when ctx cancels mid-command the borrowed connection is NOT
// closed — the command receives the same, now-canceled ctx, owns its shutdown
// (stop intake, drain in-flight work, return), and its error is returned
// verbatim (nil = clean drain). If the command does not return within
// Config.DrainTimeout of the cancellation, the connection is force-closed as
// a backstop and the call returns ErrDrainTimeout joined with the context
// error.
//
// A clean drain should return nil, not ctx.Err(): returning the cancellation
// error makes the release classifier discard a healthy connection. Broker
// operations performed DURING drain (final acks, dead-letter publishes) must
// detach from the canceled ctx locally via context.WithoutCancel.
func (c *Client) ProcessWithDrain(ctx context.Context, cmd Command) error {
	return c.process(ctx, cmd, cancelPolicy{drainTimeout: c.config.DrainTimeout})
}

func (c *Client) process(ctx context.Context, cmd Command, policy cancelPolicy) error {
	for attempt := 0; ; attempt++ {
		retry, err := c.doProcess(ctx, cmd, attempt, policy)
		if err == nil || !retry || attempt >= c.config.MaxRetries {
			return err
		}
	}
}

func (c *Client) doProcess(ctx context.Context, cmd Command, attempt int, policy cancelPolicy) (bool, error) {
	if attempt > 0 {
		if err := sleepWithContext(ctx, c.retryBackoff(attempt)); err != nil {
			return false, err
		}
	}

	err := c.withConn(ctx, cmd, policy)
	if err == nil {
		return false, nil
	}
	// Drain mode after cancellation: never retry. The next attempt's backoff
	// sleep would consume the canceled ctx and substitute context.Canceled
	// for the command's verbatim error, and re-running a command that
	// already drained is wrong regardless — the shutdown is in progress.
	if policy.drainTimeout != 0 && ctx.Err() != nil {
		return false, err
	}

	retry := shouldRetry(err)
	return retry, err
}

func (c *Client) retryBackoff(attempt int) time.Duration {
	return retryBackoff(attempt, c.config.MinRetryBackoff, c.config.MaxRetryBackoff)
}

// Close closes the pool and all connections owned by the client.
func (c *Client) Close() error {
	return c.connPool.Close()
}

func retryBackoff(retry int, minBackoff, maxBackoff time.Duration) time.Duration {
	if retry < 0 {
		panic("amqpx: not reached")
	}
	if minBackoff == 0 {
		return 0
	}

	d := minBackoff << uint(retry)
	if d < minBackoff {
		return maxBackoff
	}

	//nolint:gosec // Retry jitter does not require cryptographic randomness.
	d = minBackoff + time.Duration(rand.Int64N(int64(d)))

	if d > maxBackoff || d < minBackoff {
		d = maxBackoff
	}

	return d
}

func sleepWithContext(ctx context.Context, dur time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if dur <= 0 {
		return nil
	}

	t := time.NewTimer(dur)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isBadConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if _, ok := errors.AsType[timeoutError](err); ok {
		return true
	}
	if amqpErr, ok := errors.AsType[*amqp.Error](err); ok && amqpErr != nil {
		return true
	}

	return false
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if _, ok := errors.AsType[timeoutError](err); ok {
		return true
	}

	if amqpErr, ok := errors.AsType[*amqp.Error](err); ok && amqpErr != nil {
		switch amqpErr.Code {
		case amqp.ConnectionForced, amqp.ChannelError, amqp.InternalError:
			return true
		}
	}

	return false
}

type timeoutError interface {
	error
	Timeout() bool
}
