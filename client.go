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
// connection and channel. A Command must not retain conn after returning and
// should stop promptly when ctx is canceled.
type Command func(ctx context.Context, conn *connpool.Conn) error

// Client executes AMQP commands with connection pooling and bounded retries.
type Client struct {
	config   *Config
	connPool *connpool.ConnPool
}

const cancelCloseTimeout = 100 * time.Millisecond

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

func (c *Client) withConn(ctx context.Context, fn Command) error {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}

	defer func() {
		c.releaseConn(ctx, conn, err)
	}()

	err = runCommandWithContext(
		ctx,
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
		closeConnectionWithDeadline(closeConn)
		return ctx.Err()
	case err := <-errCh:
		return err
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
	for attempt := 0; ; attempt++ {
		retry, err := c.doProcess(ctx, cmd, attempt)
		if err == nil || !retry || attempt >= c.config.MaxRetries {
			return err
		}
	}
}

func (c *Client) doProcess(ctx context.Context, cmd Command, attempt int) (bool, error) {
	if attempt > 0 {
		if err := sleepWithContext(ctx, c.retryBackoff(attempt)); err != nil {
			return false, err
		}
	}

	err := c.withConn(ctx, cmd)
	if err == nil {
		return false, nil
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
