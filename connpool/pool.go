package connpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrClosed is returned when an operation cannot run because the pool is closed.
	ErrClosed = errors.New("amqpx: connection pool is closed")

	// ErrPoolTimeout is returned when Get times out waiting for a pool turn.
	ErrPoolTimeout = errors.New("amqpx: connection pool timeout")
)

var timers = sync.Pool{
	New: func() any {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

const (
	defaultMinIdleRetryDelay = time.Second
	discardCloseTimeout      = 100 * time.Millisecond
)

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // number of total connections in the pool
	IdleConns  uint32 // number of idle connections in the pool
	StaleConns uint32 // number of stale connections removed from the pool
}

// Pooler is the connection-pool contract implemented by ConnPool.
type Pooler interface {
	NewConn() (*Conn, error)
	CloseConn(*Conn) error

	Get(context.Context) (*Conn, error)
	Put(context.Context, *Conn)
	Remove(context.Context, *Conn, error)

	Len() int
	IdleLen() int
	Stats() *Stats

	Close() error
}

// Options configures a ConnPool. New validates and copies Options, so later
// caller mutations do not affect the pool.
type Options struct {
	// Dialer creates an AMQP connection and channel. It is retained for
	// compatibility and cannot be interrupted by context cancellation.
	Dialer func() (*amqp.Connection, *amqp.Channel, error)
	// DialerContext creates an AMQP connection and channel with cancellation.
	// When set, it takes precedence over Dialer.
	DialerContext func(context.Context) (*amqp.Connection, *amqp.Channel, error)
	// OnClose runs at most once when the pool processes a connection close. Its
	// error is joined with the connection close error. It must return promptly.
	OnClose func(*Conn) error
	// PoolFIFO selects FIFO idle checkout; false selects LIFO.
	PoolFIFO bool
	// PoolSize is the positive maximum number of pooled leases and dials.
	PoolSize int
	// MinIdleConns is the desired prewarmed idle count, from zero to PoolSize.
	MinIdleConns int
	// MaxConnAge retires connections at or beyond this age when positive.
	MaxConnAge time.Duration
	// PoolTimeout bounds how long Get waits for a pool turn and must not be negative.
	PoolTimeout time.Duration
	// IdleTimeout retires connections idle for at least this duration when positive.
	IdleTimeout time.Duration
	// IdleCheckFrequency controls background idle reaping when positive.
	IdleCheckFrequency time.Duration
}

type lastDialErrorWrap struct {
	err error
}

// ConnPool is a concurrency-safe bounded pool of AMQP connections.
type ConnPool struct {
	opt *Options

	dialErrorsNum atomic.Uint32

	lastDialError atomic.Value

	queue chan struct{}

	connsMu      sync.Mutex
	conns        []*Conn
	idleConns    []*Conn
	poolSize     int
	idleConnsLen int
	pendingIdle  int

	stats Stats

	minIdleRetryScheduled atomic.Bool
	minIdleRetryDelay     time.Duration

	closedFlag atomic.Bool
	closedCh   chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

var _ Pooler = (*ConnPool)(nil)

// New validates and copies opt, starts optional prewarming and idle reaping,
// and returns a connection pool. It panics for nil or invalid options.
func New(opt *Options) *ConnPool {
	if opt == nil {
		panic("amqpx: nil connection pool options")
	}
	if opt.PoolSize <= 0 {
		panic("amqpx: connection pool size must be positive")
	}
	if opt.MinIdleConns < 0 || opt.MinIdleConns > opt.PoolSize {
		panic("amqpx: minimum idle connections must be between zero and pool size")
	}
	if opt.PoolTimeout < 0 {
		panic("amqpx: connection pool timeout must not be negative")
	}
	if opt.Dialer == nil && opt.DialerContext == nil {
		panic("amqpx: connection pool requires a dialer")
	}

	optCopy := *opt
	opt = &optCopy
	ctx, cancel := context.WithCancel(context.Background())
	p := &ConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
		closedCh:  make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,

		minIdleRetryDelay: defaultMinIdleRetryDelay,
	}

	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()

	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

func (p *ConnPool) checkMinIdleConns() {
	if p.opt.MinIdleConns == 0 || p.closed() {
		return
	}
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen+p.pendingIdle < p.opt.MinIdleConns {
		select {
		case p.queue <- struct{}{}:
			p.poolSize++
			p.pendingIdle++

			go func() {
				err := p.addIdleConn()
				retry := false
				if err != nil {
					p.connsMu.Lock()
					if !p.closed() {
						p.poolSize--
						p.pendingIdle--
						retry = true
					}
					p.connsMu.Unlock()
				}
				p.freeTurn()
				if retry {
					p.scheduleMinIdleRetry()
				}
			}()
		default:
			return
		}
	}
}

func (p *ConnPool) addIdleConn() error {
	cn, err := p.dialConn(p.ctx, true)
	if err != nil {
		return err
	}

	p.connsMu.Lock()
	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		p.connsMu.Unlock()
		_ = cn.CloseDeadline(time.Now().Add(discardCloseTimeout))
		return ErrClosed
	}

	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	p.pendingIdle--
	p.idleConnsLen++
	p.connsMu.Unlock()
	return nil
}

// NewConn creates an unpooled connection without acquiring a pool turn. The
// caller can release it with CloseConn, Put, or Remove.
func (p *ConnPool) NewConn() (*Conn, error) {
	return p.newConn(p.ctx, false)
}

func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		p.connsMu.Unlock()
		_ = cn.CloseDeadline(time.Now().Add(discardCloseTimeout))
		return nil, ErrClosed
	}

	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}
	p.connsMu.Unlock()

	return cn, nil
}

func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if p.dialErrorsNum.Load() >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	amqpConn, amqpCh, err := p.dial(ctx)
	if err != nil {
		if p.closed() {
			return nil, ErrClosed
		}
		if ctx != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
		}
		p.setLastDialError(err)
		if p.dialErrorsNum.Add(1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}
	p.dialErrorsNum.Store(0)

	cn := NewConn(amqpConn, amqpCh)
	cn.pooled = pooled
	return cn, nil
}

func (p *ConnPool) dial(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
	if p.opt.DialerContext == nil {
		return p.opt.Dialer()
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dialCtx, cancel := context.WithCancel(ctx)
	stopPoolCancel := context.AfterFunc(p.ctx, cancel)
	defer func() {
		stopPoolCancel()
		cancel()
	}()

	return p.opt.DialerContext(dialCtx)
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, _, err := p.dial(p.ctx)
		if err != nil {
			if p.closed() {
				return
			}
			p.setLastDialError(err)
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-p.closedCh:
				timer.Stop()
				return
			}
			continue
		}

		p.dialErrorsNum.Store(0)
		_ = conn.CloseDeadline(time.Now().Add(discardCloseTimeout))
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get returns an idle connection or creates a new one. A successful checkout
// owns one pool turn until exactly one Put, Remove, or CloseConn call.
func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}

	for {
		p.connsMu.Lock()
		cn, err := p.popIdle()
		p.connsMu.Unlock()

		if err != nil {
			p.freeTurn()
			return nil, err
		}

		if cn == nil {
			break
		}

		if cn.IsClosed() {
			p.discardPoppedConn(ctx, cn)
			continue
		}

		if p.isStaleConn(cn) {
			p.discardPoppedConn(ctx, cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		cn.activateLease()
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	newcn, err := p.newConn(ctx, true)
	if err != nil {
		p.freeTurn()
		p.maintainMinIdleConns()
		return nil, err
	}

	newcn.activateLease()
	return newcn, nil
}

func (p *ConnPool) getTurn() error {
	if p.closed() {
		return ErrClosed
	}

	select {
	case p.queue <- struct{}{}:
		if p.closed() {
			p.freeTurn()
			return ErrClosed
		}
		return nil
	case <-p.closedCh:
		return ErrClosed
	}
}

func (p *ConnPool) waitTurn(ctx context.Context) error {
	if p.closed() {
		return ErrClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closedCh:
		return ErrClosed
	default:
	}

	select {
	case p.queue <- struct{}{}:
		if p.closed() {
			p.freeTurn()
			return ErrClosed
		}
		return nil
	case <-p.closedCh:
		return ErrClosed
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)
	defer timers.Put(timer)

	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-p.closedCh:
		timer.Stop()
		return ErrClosed
	case p.queue <- struct{}{}:
		timer.Stop()
		if p.closed() {
			p.freeTurn()
			return ErrClosed
		}
		return nil
	case <-timer.C:
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

func (p *ConnPool) freeTurn() {
	<-p.queue
}

func (p *ConnPool) popIdle() (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	n := len(p.idleConns)
	if n == 0 {
		return nil, nil
	}

	var cn *Conn
	if p.opt.PoolFIFO {
		cn = p.idleConns[0]
		copy(p.idleConns, p.idleConns[1:])
		p.idleConns[n-1] = nil
		p.idleConns = p.idleConns[:n-1]
	} else {
		idx := n - 1
		cn = p.idleConns[idx]
		p.idleConns[idx] = nil
		p.idleConns = p.idleConns[:idx]
	}
	p.idleConnsLen--
	p.checkMinIdleConns()
	return cn, nil
}

// Put returns a checked-out connection to the pool. Duplicate returns are
// ignored. Unpooled connections are removed and closed.
func (p *ConnPool) Put(ctx context.Context, cn *Conn) {
	releaseTurn, firstReturn := cn.releaseLease()
	if !firstReturn {
		return
	}

	if !cn.pooled {
		removed := p.removeConnWithLock(cn)
		if releaseTurn {
			p.freeTurn()
		}
		if removed {
			_ = p.closeConn(cn)
		}
		p.maintainMinIdleConns()
		return
	}

	var removed bool
	p.connsMu.Lock()
	if p.closed() || cn.closeStarted.Load() {
		removed = p.removeConn(cn)
	} else if p.hasConn(cn) {
		cn.SetUsedAt(time.Now())
		p.idleConns = append(p.idleConns, cn)
		p.idleConnsLen++
	}
	p.connsMu.Unlock()
	if releaseTurn {
		p.freeTurn()
	}
	if removed {
		_ = p.tryCloseConn(cn)
	}
	p.maintainMinIdleConns()
}

// Remove releases a checked-out connection, removes it from the pool, and
// closes it before returning. Duplicate returns are ignored.
func (p *ConnPool) Remove(ctx context.Context, cn *Conn, reason error) {
	releaseTurn, firstReturn := cn.releaseLease()
	if !firstReturn {
		return
	}

	removed := p.removeConnWithLock(cn)
	if releaseTurn {
		p.freeTurn()
	}
	if removed {
		_ = p.closeConn(cn)
	}
	p.maintainMinIdleConns()
}

// CloseConn removes and closes an active, idle, or unpooled connection. It
// releases an active connection's pool turn.
func (p *ConnPool) CloseConn(cn *Conn) error {
	releaseTurn, _ := cn.releaseLease()
	removed := p.removeConnWithLock(cn)
	if releaseTurn {
		p.freeTurn()
	}
	if !removed {
		return nil
	}
	err := p.closeConn(cn)
	p.maintainMinIdleConns()
	return err
}

func (p *ConnPool) removeConnWithLock(cn *Conn) bool {
	p.connsMu.Lock()
	removed := p.removeConn(cn)
	p.connsMu.Unlock()
	return removed
}

func (p *ConnPool) removeConn(cn *Conn) bool {
	for i, c := range p.conns {
		if c == cn {
			last := len(p.conns) - 1
			copy(p.conns[i:], p.conns[i+1:])
			p.conns[last] = nil
			p.conns = p.conns[:last]

			for idleIndex, idleConn := range p.idleConns {
				if idleConn == cn {
					idleLast := len(p.idleConns) - 1
					copy(p.idleConns[idleIndex:], p.idleConns[idleIndex+1:])
					p.idleConns[idleLast] = nil
					p.idleConns = p.idleConns[:idleLast]
					p.idleConnsLen--
					break
				}
			}
			if cn.pooled {
				p.poolSize--
			}
			return true
		}
	}
	return false
}

func (p *ConnPool) hasConn(cn *Conn) bool {
	for _, candidate := range p.conns {
		if candidate == cn {
			return true
		}
	}
	return false
}

func (p *ConnPool) maintainMinIdleConns() {
	if p.closed() {
		return
	}
	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()
}

func (p *ConnPool) discardPoppedConn(ctx context.Context, cn *Conn) {
	if p.removeConnWithLock(cn) {
		p.closeDiscardedConn(ctx, cn)
	}
	p.maintainMinIdleConns()
}

func (p *ConnPool) closeDiscardedConn(ctx context.Context, cn *Conn) {
	deadline := time.Now().Add(discardCloseTimeout)
	done := make(chan struct{})
	go func() {
		_ = cn.tryCloseForPoolDeadline(p.opt.OnClose, deadline)
		close(done)
	}()

	timer := time.NewTimer(discardCloseTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-ctx.Done():
	case <-p.closedCh:
	case <-timer.C:
	}
}

func (p *ConnPool) scheduleMinIdleRetry() {
	if p.closed() || !p.minIdleRetryScheduled.CompareAndSwap(false, true) {
		return
	}

	go func() {
		timer := time.NewTimer(p.minIdleRetryDelay)
		defer timer.Stop()

		select {
		case <-timer.C:
			p.minIdleRetryScheduled.Store(false)
			p.maintainMinIdleConns()
		case <-p.closedCh:
			p.minIdleRetryScheduled.Store(false)
		}
	}()
}

func (p *ConnPool) closeConn(cn *Conn) error {
	return cn.closeForPool(p.opt.OnClose)
}

func (p *ConnPool) tryCloseConn(cn *Conn) error {
	return cn.tryCloseForPool(p.opt.OnClose)
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

// Stats returns a point-in-time snapshot of pool counters.
func (p *ConnPool) Stats() *Stats {
	p.connsMu.Lock()
	totalLen, idleLen := len(p.conns), p.idleConnsLen
	p.connsMu.Unlock()

	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(totalLen),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return p.closedFlag.Load()
}

// Filter closes connections selected by fn. Predicates and close callbacks run
// without holding the pool mutex.
func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	p.connsMu.Lock()
	conns := append([]*Conn(nil), p.conns...)
	p.connsMu.Unlock()

	var filterErr error
	for _, cn := range conns {
		if fn(cn) {
			filterErr = errors.Join(filterErr, p.closeConn(cn))
		}
	}
	return filterErr
}

// Close marks the pool closed, cancels context-aware dials, wakes pool waiters,
// and closes all currently registered connections.
func (p *ConnPool) Close() error {
	if !p.closedFlag.CompareAndSwap(false, true) {
		return ErrClosed
	}
	close(p.closedCh)
	p.cancel()

	p.connsMu.Lock()
	conns := p.conns
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.pendingIdle = 0
	p.connsMu.Unlock()

	var closeErr error
	for _, cn := range conns {
		closeErr = errors.Join(closeErr, p.closeConn(cn))
	}
	return closeErr
}

func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			_, err := p.ReapStaleConns()
			if err != nil {
				continue
			}
		case <-p.closedCh:
			return
		}
	}
}

// ReapStaleConns removes and closes currently stale idle connections.
func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	defer func() {
		atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	}()

	for {
		if err := p.getTurn(); err != nil {
			return n, err
		}

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()

		p.freeTurn()

		if cn != nil {
			p.maintainMinIdleConns()
			_ = p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	return n, nil
}

func (p *ConnPool) reapStaleConn() *Conn {
	for index, cn := range p.idleConns {
		if !p.isStaleConn(cn) {
			continue
		}

		last := len(p.idleConns) - 1
		copy(p.idleConns[index:], p.idleConns[index+1:])
		p.idleConns[last] = nil
		p.idleConns = p.idleConns[:last]
		p.idleConnsLen--
		p.removeConn(cn)
		return cn
	}

	return nil
}

func (p *ConnPool) isStaleConn(cn *Conn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		return true
	}

	return false
}
