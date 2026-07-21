package amqpx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/quarks-tech/amqpx/connpool"
)

// Limiter controls whether a command attempt may proceed and observes the
// result of each allowed attempt.
type Limiter interface {
	// Allow returns nil if operation is allowed or an error otherwise.
	// If operation is allowed client must ReportResult of the operation
	// whether it is a success or a failure.
	Allow() error
	// ReportResult reports the result of the previously allowed operation.
	// nil indicates a success, non-nil error usually indicates a failure.
	ReportResult(result error)
}

// Config controls AMQP connection setup, retry behavior, and connection
// pooling. NewClient copies Config and its top-level mutable AMQP containers;
// values referenced inside interfaces, maps, and slices remain caller-owned.
type Config struct {
	// Address is an AMQP URI with an optional amqp:// or amqps:// scheme.
	Address string

	// AMQP configures the underlying amqp091-go connection. Experimental
	// connection recovery must remain disabled because the pool owns connection
	// replacement.
	AMQP amqp.Config

	// Maximum number of retries before giving up.
	// Default is 3 retries; any negative value disables retries.
	MaxRetries int
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; any negative value disables backoff.
	MinRetryBackoff time.Duration
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; any negative value disables backoff.
	// When enabled, it must not be less than MinRetryBackoff.
	MaxRetryBackoff time.Duration

	// Dial timeout for establishing a new connection and its initial channel.
	// Default is 5 seconds. Negative values are invalid. A custom AMQP.Dial
	// callback cannot itself be interrupted before it returns a transport.
	DialTimeout time.Duration

	// Type of connection pool.
	// true for FIFO pool, false for LIFO pool.
	// Note that fifo has higher overhead compared to lifo.
	PoolFIFO bool
	// Maximum number of socket connections.
	// Default is one connection per available CPU as reported by runtime.GOMAXPROCS.
	// Negative values are invalid.
	PoolSize int
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	// It must be between zero and PoolSize.
	MinIdleConns int
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is 1 second. Negative values are invalid.
	PoolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is disabled. A negative value also disables the check.
	IdleTimeout time.Duration
	// Frequency of idle checks made by idle connections reaper.
	// Default is 1 minute. A negative value disables the idle connections reaper,
	// but idle connections are still discarded by the client
	// if IdleTimeout is set.
	IdleCheckFrequency time.Duration

	// Limiter can implement a circuit breaker or rate limiter.
	Limiter Limiter
}

func (c *Config) complete() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}

	if c.PoolSize == 0 {
		c.PoolSize = runtime.GOMAXPROCS(0)
	}

	if c.PoolTimeout == 0 {
		c.PoolTimeout = time.Second
	}

	if c.IdleCheckFrequency == 0 {
		c.IdleCheckFrequency = time.Minute
	}

	switch {
	case c.MaxRetries < 0:
		c.MaxRetries = 0
	case c.MaxRetries == 0:
		c.MaxRetries = 3
	}
	switch {
	case c.MinRetryBackoff < 0:
		c.MinRetryBackoff = 0
	case c.MinRetryBackoff == 0:
		c.MinRetryBackoff = 8 * time.Millisecond
	}
	switch {
	case c.MaxRetryBackoff < 0:
		c.MaxRetryBackoff = 0
	case c.MaxRetryBackoff == 0:
		c.MaxRetryBackoff = 512 * time.Millisecond
	}
}

func (c *Config) validate() error {
	switch {
	case c.PoolSize < 0:
		return fmt.Errorf("amqpx: PoolSize must not be negative")
	case c.MinIdleConns < 0:
		return fmt.Errorf("amqpx: MinIdleConns must not be negative")
	case c.MinIdleConns > c.PoolSize:
		return fmt.Errorf("amqpx: MinIdleConns must not exceed PoolSize")
	case c.PoolTimeout < 0:
		return fmt.Errorf("amqpx: PoolTimeout must not be negative")
	case c.DialTimeout < 0:
		return fmt.Errorf("amqpx: DialTimeout must not be negative")
	case c.MinRetryBackoff > 0 && c.MaxRetryBackoff > 0 && c.MaxRetryBackoff < c.MinRetryBackoff:
		return fmt.Errorf("amqpx: MaxRetryBackoff must not be less than MinRetryBackoff")
	default:
		return nil
	}
}

func newConnPool(cfg *Config) *connpool.ConnPool {
	return connpool.New(&connpool.Options{
		DialerContext: func(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
			return dialAMQPContext(ctx, connectionURL(cfg.Address), cfg.AMQP, cfg.DialTimeout)
		},
		PoolFIFO:           cfg.PoolFIFO,
		PoolSize:           cfg.PoolSize,
		MinIdleConns:       cfg.MinIdleConns,
		MaxConnAge:         cfg.MaxConnAge,
		PoolTimeout:        cfg.PoolTimeout,
		IdleTimeout:        cfg.IdleTimeout,
		IdleCheckFrequency: cfg.IdleCheckFrequency,
	})
}

func connectionURL(address string) string {
	lowerAddress := strings.ToLower(address)
	if strings.HasPrefix(lowerAddress, "amqp://") || strings.HasPrefix(lowerAddress, "amqps://") {
		return address
	}
	return "amqp://" + address
}

type dialResult struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	err  error
}

type connectionSetupTimeoutError struct {
	timeout time.Duration
}

func (e connectionSetupTimeoutError) Error() string {
	return fmt.Sprintf("amqpx: connection setup timed out after %s", e.timeout)
}

func (connectionSetupTimeoutError) Unwrap() error { return os.ErrDeadlineExceeded }

func (connectionSetupTimeoutError) Timeout() bool { return true }

// Temporary preserves net.Error compatibility. New code should use Timeout.
func (connectionSetupTimeoutError) Temporary() bool { return true }

func dialAMQPContext(
	ctx context.Context,
	address string,
	cfg amqp.Config,
	dialTimeout time.Duration,
) (*amqp.Connection, *amqp.Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	if cfg.Dial == nil {
		result := dialAMQP(dialCtx, address, cfg, dialTimeout)
		if err := dialContextError(ctx, dialCtx, dialTimeout); err != nil {
			closeDialResult(result, nil)
			return nil, nil, err
		}
		return result.conn, result.ch, result.err
	}

	resultCh := make(chan dialResult)
	go func() {
		sendDialResult(dialCtx, resultCh, dialAMQP(dialCtx, address, cfg, dialTimeout))
	}()

	select {
	case <-dialCtx.Done():
		return nil, nil, dialContextError(ctx, dialCtx, dialTimeout)
	case result := <-resultCh:
		if err := dialContextError(ctx, dialCtx, dialTimeout); err != nil {
			closeDialResult(result, nil)
			return nil, nil, err
		}
		return result.conn, result.ch, result.err
	}
}

func dialContextError(ctx, dialCtx context.Context, dialTimeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(dialCtx.Err(), context.DeadlineExceeded) {
		return connectionSetupTimeoutError{timeout: dialTimeout}
	}
	return dialCtx.Err()
}

func dialAMQP(ctx context.Context, address string, cfg amqp.Config, dialTimeout time.Duration) dialResult {
	cfg = cloneAMQPConfig(cfg)
	originalDial := cfg.Dial
	var rawConn net.Conn
	var stopCancelClose func() bool
	cfg.Dial = func(network, address string) (net.Conn, error) {
		var conn net.Conn
		var err error
		if originalDial == nil {
			dialer := &net.Dialer{Timeout: dialTimeout}
			conn, err = dialer.DialContext(ctx, network, address)
		} else {
			conn, err = originalDial(network, address)
		}
		if err != nil {
			return nil, err
		}
		rawConn = conn

		if err := ctx.Err(); err != nil {
			_ = conn.Close()
			return nil, err
		}
		if originalDial == nil {
			if err := conn.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
				_ = conn.Close()
				return nil, err
			}
		}
		stopCancelClose = context.AfterFunc(ctx, func() {
			_ = conn.Close()
		})
		return conn, nil
	}

	result := openAMQPConnection(address, cfg)
	if stopCancelClose != nil {
		stopCancelClose()
	}

	if err := ctx.Err(); err != nil {
		closeDialResult(result, rawConn)
		return dialResult{err: err}
	}
	if result.err != nil && rawConn != nil {
		_ = rawConn.Close()
	}
	return result
}

func openAMQPConnection(address string, cfg amqp.Config) dialResult {
	conn, err := amqp.DialConfig(address, cfg)
	if err != nil {
		return dialResult{err: fmt.Errorf("amqpx: create connection: %w", err)}
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.CloseDeadline(time.Now().Add(cancelCloseTimeout))
		return dialResult{err: fmt.Errorf("amqpx: create channel: %w", err)}
	}

	return dialResult{conn: conn, ch: ch}
}

func closeDialResult(result dialResult, rawConn net.Conn) {
	if result.conn != nil {
		_ = result.conn.CloseDeadline(time.Now().Add(cancelCloseTimeout))
		return
	}
	if rawConn != nil {
		_ = rawConn.Close()
	}
}

func sendDialResult(ctx context.Context, resultCh chan<- dialResult, result dialResult) {
	select {
	case resultCh <- result:
	case <-ctx.Done():
		if result.conn != nil {
			_ = result.conn.CloseDeadline(time.Now().Add(cancelCloseTimeout))
		}
	}
}
