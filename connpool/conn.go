package connpool

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Conn owns one AMQP connection and its associated channel.
type Conn struct {
	amqpConn          *amqp.Connection
	amqpChannel       *amqp.Channel
	pooled            bool
	createdAt         time.Time
	usedAt            atomic.Int64
	leaseState        atomic.Uint32
	closeStarted      atomic.Bool
	closeNotifyOnce   sync.Once
	closeNotifyErr    error
	closeFunc         func() error
	closeDeadlineFunc func(time.Time) error
}

const (
	leaseUnmanaged uint32 = iota
	leaseActive
	leaseReturned
)

// NewConn wraps an AMQP connection and channel.
func NewConn(amqpConn *amqp.Connection, amqpCh *amqp.Channel) *Conn {
	conn := &Conn{
		amqpConn:    amqpConn,
		amqpChannel: amqpCh,
		createdAt:   time.Now(),
	}

	conn.SetUsedAt(time.Now())

	return conn
}

// UsedAt reports when the connection was last used or returned to the pool.
func (c *Conn) UsedAt() time.Time {
	return time.Unix(0, c.usedAt.Load())
}

// SetUsedAt updates the connection's last-use timestamp.
func (c *Conn) SetUsedAt(tm time.Time) {
	c.usedAt.Store(tm.UnixNano())
}

// Channel returns the connection's AMQP channel and records its use.
func (c *Conn) Channel() *amqp.Channel {
	c.SetUsedAt(time.Now())

	return c.amqpChannel
}

// NotifyClose registers receiver for underlying AMQP connection close events.
func (c *Conn) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	c.SetUsedAt(time.Now())

	return c.amqpConn.NotifyClose(receiver)
}

// IsClosed reports the underlying AMQP connection state without changing its
// last-use timestamp.
func (c *Conn) IsClosed() bool {
	if c.closeStarted.Load() || c.amqpConn == nil || c.amqpConn.IsClosed() {
		return true
	}
	return c.amqpChannel != nil && c.amqpChannel.IsClosed()
}

// Close closes the underlying AMQP connection.
func (c *Conn) Close() error {
	if !c.claimClose() {
		return amqp.ErrClosed
	}
	return c.closeUnderlying()
}

func (c *Conn) closeUnderlying() error {
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return c.amqpConn.Close()
}

// CloseDeadline closes the underlying AMQP connection using deadline for the
// close handshake.
func (c *Conn) CloseDeadline(deadline time.Time) error {
	if !c.claimClose() {
		return amqp.ErrClosed
	}
	return c.closeUnderlyingDeadline(deadline)
}

func (c *Conn) closeUnderlyingDeadline(deadline time.Time) error {
	if c.closeDeadlineFunc != nil {
		return c.closeDeadlineFunc(deadline)
	}
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return c.amqpConn.CloseDeadline(deadline)
}

func (c *Conn) claimClose() bool {
	return c.closeStarted.CompareAndSwap(false, true)
}

func (c *Conn) closeForPool(onClose func(*Conn) error) error {
	claimed := c.claimClose()
	onCloseErr := c.runOnClose(onClose)
	if !claimed {
		return errors.Join(onCloseErr, amqp.ErrClosed)
	}
	return errors.Join(onCloseErr, c.closeUnderlying())
}

// tryCloseForPool closes only when this caller can claim ownership. It is used
// by release paths that must not wait for another operation's OnClose callback.
func (c *Conn) tryCloseForPool(onClose func(*Conn) error) error {
	if !c.claimClose() {
		return amqp.ErrClosed
	}
	return errors.Join(c.runOnClose(onClose), c.closeUnderlying())
}

func (c *Conn) tryCloseForPoolDeadline(onClose func(*Conn) error, deadline time.Time) error {
	if !c.claimClose() {
		return amqp.ErrClosed
	}
	return errors.Join(c.runOnClose(onClose), c.closeUnderlyingDeadline(deadline))
}

func (c *Conn) activateLease() {
	c.leaseState.Store(leaseActive)
}

func (c *Conn) releaseLease() (releaseTurn, firstReturn bool) {
	for {
		state := c.leaseState.Load()
		if state == leaseReturned {
			return false, false
		}
		if c.leaseState.CompareAndSwap(state, leaseReturned) {
			return state == leaseActive, true
		}
	}
}

func (c *Conn) runOnClose(fn func(*Conn) error) error {
	c.closeNotifyOnce.Do(func() {
		if fn != nil {
			c.closeNotifyErr = fn(c)
		}
	})
	return c.closeNotifyErr
}
