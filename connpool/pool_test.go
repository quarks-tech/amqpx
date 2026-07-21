package connpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type observedContext struct {
	context.Context
	doneCalled chan struct{}
	once       sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneCalled) })
	return nil
}

func TestCloseCancelsContextDialer(t *testing.T) {
	dialStarted := make(chan struct{})
	dialCanceled := make(chan struct{})
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Hour,
		DialerContext: func(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
			close(dialStarted)
			<-ctx.Done()
			close(dialCanceled)
			return nil, nil, ctx.Err()
		},
	})

	getDone := make(chan error, 1)
	go func() {
		_, err := p.Get(context.Background())
		getDone <- err
	}()

	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case <-dialCanceled:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the context-aware dialer")
	}

	select {
	case err := <-getDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Get() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after Close")
	}
}

func TestCloseWakesGetWaitingForPoolTurn(t *testing.T) {
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Hour,
		DialerContext: func(context.Context) (*amqp.Connection, *amqp.Channel, error) {
			close(dialStarted)
			<-releaseDial
			return nil, nil, errors.New("dial released")
		},
	})

	firstGetDone := make(chan error, 1)
	go func() {
		_, err := p.Get(context.Background())
		firstGetDone <- err
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("first Get did not occupy the pool turn")
	}

	secondGetDone := make(chan error, 1)
	secondCtx := &observedContext{
		Context:    context.Background(),
		doneCalled: make(chan struct{}),
	}
	go func() {
		_, err := p.Get(secondCtx)
		secondGetDone <- err
	}()
	select {
	case <-secondCtx.doneCalled:
	case <-time.After(time.Second):
		t.Fatal("second Get did not enter the pool wait")
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case err := <-secondGetDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiting Get() error = %v, want ErrClosed", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(releaseDial)
		t.Fatal("Close did not wake Get waiting for a pool turn")
	}

	close(releaseDial)
	select {
	case <-firstGetDone:
	case <-time.After(time.Second):
		t.Fatal("first Get did not finish after releasing its dialer")
	}
}

func TestDoublePutReturnsWithoutCorruptingPool(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
	})

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	p.Put(context.Background(), cn)

	secondPutDone := make(chan struct{})
	go func() {
		p.Put(context.Background(), cn)
		close(secondPutDone)
	}()

	select {
	case <-secondPutDone:
	case <-time.After(250 * time.Millisecond):
		// Unblock the buggy implementation so the test does not leak a goroutine.
		p.queue <- struct{}{}
		<-secondPutDone
		t.Fatal("second Put blocked trying to release the same pool turn twice")
	}

	if got := p.IdleLen(); got != 1 {
		t.Fatalf("IdleLen() = %d, want 1 after duplicate Put", got)
	}
}

func TestPutOfNewConnDoesNotReleaseUnownedPoolTurn(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
	})

	cn, err := p.NewConn()
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	closed := make(chan struct{})
	cn.closeFunc = func() error {
		close(closed)
		return nil
	}

	putDone := make(chan struct{})
	go func() {
		p.Put(context.Background(), cn)
		close(putDone)
	}()

	select {
	case <-putDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Put(NewConn()) blocked releasing a pool turn it never acquired")
	}
	select {
	case <-closed:
	default:
		t.Fatal("Put(NewConn()) did not close the unpooled connection")
	}
	if got := len(p.queue); got != 0 {
		t.Fatalf("pool has %d held turns after Put(NewConn()), want 0", got)
	}
}

func TestPutAfterCloseDoesNotRepopulatePool(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
	})

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	var closeCalls atomic.Int32
	cn.closeFunc = func() error {
		closeCalls.Add(1)
		return nil
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	p.Put(context.Background(), cn)

	if got := p.IdleLen(); got != 0 {
		t.Fatalf("IdleLen() = %d after Put on closed pool, want 0", got)
	}
	if got := p.Len(); got != 0 {
		t.Fatalf("Len() = %d after Put on closed pool, want 0", got)
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("connection close calls = %d, want 1", got)
	}
}

func TestFilterPredicateRunsOutsidePoolLock(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
	})
	if _, err := p.NewConn(); err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}

	filterDone := make(chan error, 1)
	go func() {
		filterDone <- p.Filter(func(*Conn) bool {
			return p.Len() < 0
		})
	}()

	select {
	case err := <-filterDone:
		if err != nil {
			t.Fatalf("Filter() error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Filter predicate deadlocked while calling back into the pool")
	}
}

func TestCloseRunsCallbackOutsideLockAndCombinesErrors(t *testing.T) {
	errOnClose := errors.New("on close")
	errConnectionClose := errors.New("connection close")
	var p *ConnPool
	p = New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
		OnClose: func(*Conn) error {
			_ = p.Len()
			return errOnClose
		},
	})

	cn, err := p.NewConn()
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	cn.closeFunc = func() error { return errConnectionClose }

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()

	select {
	case err := <-closeDone:
		if !errors.Is(err, errOnClose) {
			t.Errorf("Close() error = %v, want OnClose error", err)
		}
		if !errors.Is(err, errConnectionClose) {
			t.Errorf("Close() error = %v, want connection close error", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close deadlocked while OnClose called back into the pool")
	}
}

func TestUsedAtPreservesSubsecondPrecision(t *testing.T) {
	cn := NewConn(&amqp.Connection{}, nil)
	want := time.Unix(1_700_000_000, 123_456_789)
	cn.SetUsedAt(want)

	if got := cn.UsedAt(); !got.Equal(want) {
		t.Fatalf("UsedAt() = %v, want %v", got, want)
	}
}

func TestIsClosedDoesNotChangeIdleTimestamp(t *testing.T) {
	cn := NewConn(&amqp.Connection{}, nil)
	want := time.Unix(1_700_000_000, 123_456_789)
	cn.SetUsedAt(want)

	_ = cn.IsClosed()

	if got := cn.UsedAt(); !got.Equal(want) {
		t.Fatalf("UsedAt() after IsClosed = %v, want unchanged %v", got, want)
	}
}

func TestIsClosedHandlesMissingChannel(t *testing.T) {
	cn := NewConn(&amqp.Connection{}, nil)
	if cn.IsClosed() {
		t.Fatal("IsClosed() = true for an open test connection without a channel")
	}
}

func TestPutRecordsWhenConnectionBecameIdle(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
	})
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	old := time.Now().Add(-time.Hour)
	cn.SetUsedAt(old)

	p.Put(context.Background(), cn)

	if got := cn.UsedAt(); !got.After(old) {
		t.Fatalf("UsedAt() after Put = %v, want after %v", got, old)
	}
}

func TestGetDiscardsConnectionThatExpiredWhileIdle(t *testing.T) {
	var dials atomic.Int32
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		IdleTimeout: time.Minute,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			dials.Add(1)
			return &amqp.Connection{}, nil, nil
		},
	})

	stale, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	var staleCloseCalls atomic.Int32
	stale.closeFunc = func() error {
		staleCloseCalls.Add(1)
		return nil
	}
	p.Put(context.Background(), stale)
	stale.SetUsedAt(time.Now().Add(-2 * time.Minute))

	fresh, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	if fresh == stale {
		t.Fatal("Get returned a connection that had expired while idle")
	}
	deadline := time.Now().Add(time.Second)
	for staleCloseCalls.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := staleCloseCalls.Load(); got != 1 {
		t.Fatalf("stale connection close calls = %d, want 1", got)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dial calls = %d, want 2", got)
	}
}

func TestSuccessfulDialResetsConsecutiveDialErrors(t *testing.T) {
	errDial := errors.New("dial failed")
	probeStarted := make(chan struct{})
	var probeOnce sync.Once
	var calls atomic.Int32
	p := New(&Options{
		PoolSize:    2,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			switch calls.Add(1) {
			case 1:
				return nil, nil, errDial
			case 2:
				return &amqp.Connection{}, nil, nil
			case 3:
				return nil, nil, errDial
			default:
				probeOnce.Do(func() { close(probeStarted) })
				return nil, nil, errDial
			}
		},
	})

	if _, err := p.Get(context.Background()); !errors.Is(err, errDial) {
		t.Fatalf("first Get() error = %v, want dial failure", err)
	}
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }
	if _, err := p.Get(context.Background()); !errors.Is(err, errDial) {
		t.Fatalf("third Get() error = %v, want dial failure", err)
	}

	select {
	case <-probeStarted:
		_ = p.Close()
		t.Fatal("a successful dial did not reset the consecutive-error threshold")
	case <-time.After(100 * time.Millisecond):
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestCloseWakesBlockedReaper(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Hour,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }

	reaperStarted := make(chan struct{})
	reaperDone := make(chan error, 1)
	go func() {
		close(reaperStarted)
		_, err := p.ReapStaleConns()
		reaperDone <- err
	}()
	<-reaperStarted

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-reaperDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ReapStaleConns() error = %v, want ErrClosed", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close did not wake reaper waiting for a pool turn")
	}
}

func TestMinIdleDialReservesPoolCapacity(t *testing.T) {
	minIdleDialStarted := make(chan struct{})
	releaseMinIdleDial := make(chan struct{})
	foregroundDialStarted := make(chan struct{}, 1)
	var calls atomic.Int32
	p := New(&Options{
		PoolSize:     1,
		MinIdleConns: 1,
		PoolTimeout:  time.Second,
		DialerContext: func(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
			if calls.Add(1) == 1 {
				close(minIdleDialStarted)
				<-releaseMinIdleDial
				return nil, nil, ctx.Err()
			}
			select {
			case foregroundDialStarted <- struct{}{}:
			default:
			}
			return nil, nil, errors.New("unexpected foreground dial")
		},
	})
	<-minIdleDialStarted
	if got := p.IdleLen(); got != 0 {
		t.Fatalf("IdleLen() = %d while prewarm dial is in flight, want 0", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.Get(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get() error = %v, want context deadline while min-idle dial owns capacity", err)
	}
	select {
	case <-foregroundDialStarted:
		t.Fatal("Get started another dial beyond PoolSize while min-idle dial was active")
	default:
	}

	_ = p.Close()
	close(releaseMinIdleDial)
}

func TestNewCopiesOptions(t *testing.T) {
	opt := &Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, errors.New("unused")
		},
	}
	p := New(opt)
	opt.PoolSize = 99
	opt.PoolTimeout = 99 * time.Hour

	if got := p.opt.PoolSize; got != 1 {
		t.Fatalf("pool size changed with caller-owned Options: got %d, want 1", got)
	}
	if got := p.opt.PoolTimeout; got != time.Second {
		t.Fatalf("pool timeout changed with caller-owned Options: got %v, want %v", got, time.Second)
	}
}

func TestNewRejectsNegativePoolTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New accepted a negative PoolTimeout")
		}
	}()

	_ = New(&Options{
		PoolSize:    1,
		PoolTimeout: -time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, errors.New("unused")
		},
	})
}

func TestMinIdleReservationIsReleasedWhenDialerReturnsErrClosed(t *testing.T) {
	probeStarted := make(chan struct{})
	p := New(&Options{
		PoolSize:     1,
		MinIdleConns: 1,
		PoolTimeout:  time.Second,
		DialerContext: func(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
			select {
			case <-probeStarted:
				<-ctx.Done()
				return nil, nil, ctx.Err()
			default:
				close(probeStarted)
				return nil, nil, ErrClosed
			}
		},
	})
	t.Cleanup(func() { _ = p.Close() })

	deadline := time.Now().Add(time.Second)
	for len(p.queue) != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	p.connsMu.Lock()
	poolSize, idleLen := p.poolSize, p.idleConnsLen
	p.connsMu.Unlock()
	if poolSize != 0 || idleLen != 0 {
		t.Fatalf("failed min-idle dial left reservations: poolSize=%d idleConnsLen=%d", poolSize, idleLen)
	}
}

func TestOnCloseRunsOncePerConnection(t *testing.T) {
	var calls atomic.Int32
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
		OnClose: func(*Conn) error {
			calls.Add(1)
			return nil
		},
	})

	cn, err := p.NewConn()
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }
	if err := p.Filter(func(candidate *Conn) bool { return candidate == cn }); err != nil {
		t.Fatalf("Filter() error = %v", err)
	}
	_ = p.CloseConn(cn)

	if got := calls.Load(); got != 1 {
		t.Fatalf("OnClose calls = %d, want exactly one", got)
	}
}

func TestFilterClaimsConnectionBeforeOnCloseCallback(t *testing.T) {
	onCloseStarted := make(chan struct{})
	releaseOnClose := make(chan struct{})
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
		OnClose: func(*Conn) error {
			close(onCloseStarted)
			<-releaseOnClose
			return nil
		},
	})

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }
	filterDone := make(chan error, 1)
	go func() {
		filterDone <- p.Filter(func(candidate *Conn) bool { return candidate == cn })
	}()
	<-onCloseStarted

	putDone := make(chan struct{})
	go func() {
		p.Put(context.Background(), cn)
		close(putDone)
	}()
	deadline := time.Now().Add(time.Second)
	for p.Len() != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := p.IdleLen(); got != 0 {
		close(releaseOnClose)
		<-filterDone
		<-putDone
		t.Fatalf("IdleLen() = %d while Filter is closing the connection, want 0", got)
	}
	select {
	case <-putDone:
	case <-time.After(250 * time.Millisecond):
		close(releaseOnClose)
		<-filterDone
		t.Fatal("Put waited for the close already owned by Filter")
	}

	close(releaseOnClose)
	if err := <-filterDone; err != nil {
		t.Fatalf("Filter() error = %v", err)
	}
}

func TestGetDoesNotLeaseIdleConnectionBeingFiltered(t *testing.T) {
	onCloseStarted := make(chan struct{})
	releaseOnClose := make(chan struct{})
	var dials atomic.Int32
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			dials.Add(1)
			return &amqp.Connection{}, nil, nil
		},
		OnClose: func(*Conn) error {
			close(onCloseStarted)
			<-releaseOnClose
			return nil
		},
	})

	closingConn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	closingConn.closeFunc = func() error { return nil }
	p.Put(context.Background(), closingConn)

	filterDone := make(chan error, 1)
	go func() {
		filterDone <- p.Filter(func(candidate *Conn) bool { return candidate == closingConn })
	}()
	<-onCloseStarted

	getResult := make(chan *Conn, 1)
	getErr := make(chan error, 1)
	go func() {
		cn, getError := p.Get(context.Background())
		getResult <- cn
		getErr <- getError
	}()
	select {
	case cn := <-getResult:
		if err := <-getErr; err != nil {
			t.Fatalf("second Get() error = %v", err)
		}
		if cn == closingConn {
			close(releaseOnClose)
			<-filterDone
			t.Fatal("Get leased a connection while Filter was closing it")
		}
	case <-time.After(250 * time.Millisecond):
		close(releaseOnClose)
		<-filterDone
		t.Fatal("Get waited for an unrelated close callback instead of redialing")
	}

	close(releaseOnClose)
	if err := <-filterDone; err != nil {
		t.Fatalf("Filter() error = %v", err)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dial calls = %d, want 2", got)
	}
}

func TestGetContextBoundsStaleConnectionClose(t *testing.T) {
	var dials atomic.Int32
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		MaxConnAge:  time.Nanosecond,
		DialerContext: func(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
			if dials.Add(1) == 1 {
				return &amqp.Connection{}, nil, nil
			}
			<-ctx.Done()
			return nil, nil, ctx.Err()
		},
	})

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	closeFinished := make(chan struct{})
	var closeStartOnce sync.Once
	blockClose := func() error {
		closeStartOnce.Do(func() { close(closeStarted) })
		<-releaseClose
		close(closeFinished)
		return nil
	}
	cn.closeFunc = blockClose
	cn.closeDeadlineFunc = func(time.Time) error { return blockClose() }
	p.Put(context.Background(), cn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	type result struct {
		conn *Conn
		err  error
	}
	resultCh := make(chan result, 1)
	startedAt := time.Now()
	go func() {
		got, getErr := p.Get(ctx)
		resultCh <- result{conn: got, err: getErr}
	}()

	select {
	case got := <-resultCh:
		if got.conn != nil {
			close(releaseClose)
			<-closeFinished
			t.Fatal("Get returned a connection after its context expired")
		}
		if !errors.Is(got.err, context.DeadlineExceeded) {
			close(releaseClose)
			<-closeFinished
			t.Fatalf("Get() error = %v, want context deadline exceeded", got.err)
		}
	case <-time.After(250 * time.Millisecond):
		close(releaseClose)
		<-closeFinished
		<-resultCh
		t.Fatal("Get waited for a stale connection close after its context expired")
	}
	if elapsed := time.Since(startedAt); elapsed > 200*time.Millisecond {
		close(releaseClose)
		<-closeFinished
		t.Fatalf("Get returned after %s, want context-bounded cleanup", elapsed)
	}
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		close(releaseClose)
		t.Fatal("stale connection close was not started")
	}

	close(releaseClose)
	select {
	case <-closeFinished:
	case <-time.After(time.Second):
		t.Fatal("stale connection close did not finish after it was released")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestRemoveWaitsForConnectionClose(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	var startOnce sync.Once
	blockClose := func() error {
		startOnce.Do(func() { close(closeStarted) })
		<-releaseClose
		return nil
	}
	cn.closeFunc = blockClose
	cn.closeDeadlineFunc = func(time.Time) error { return blockClose() }

	removeDone := make(chan struct{})
	go func() {
		p.Remove(context.Background(), cn, errors.New("discard"))
		close(removeDone)
	}()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		close(releaseClose)
		t.Fatal("Remove did not start closing the discarded connection")
	}
	select {
	case <-removeDone:
		close(releaseClose)
		t.Fatal("Remove returned before the connection was closed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseClose)
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("Remove did not return after the connection was closed")
	}
}

func TestPutDoesNotReinsertUntrackedConnection(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return nil, nil, nil
		},
	})

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !p.removeConnWithLock(cn) {
		t.Fatal("test setup did not remove checked-out connection")
	}
	p.Put(context.Background(), cn)

	if got := p.IdleLen(); got != 0 {
		t.Fatalf("IdleLen() = %d after returning an untracked connection, want 0", got)
	}
}

func TestMinIdleRetriesAfterTransientDialFailure(t *testing.T) {
	var calls atomic.Int32
	p := New(&Options{
		PoolSize:    2,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			if calls.Add(1) == 1 {
				return nil, nil, errors.New("transient dial failure")
			}
			return nil, nil, nil
		},
	})
	p.minIdleRetryDelay = 5 * time.Millisecond
	p.opt.MinIdleConns = 1
	p.maintainMinIdleConns()

	deadline := time.Now().Add(time.Second)
	for (calls.Load() < 2 || p.IdleLen() != 1) && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("dial calls = %d, want a retry after the transient failure", got)
	}
	if got := p.IdleLen(); got != 1 {
		t.Fatalf("IdleLen() = %d after successful retry, want 1", got)
	}
}

func TestReapStaleConnsScansPastFreshConnectionForMaxAge(t *testing.T) {
	p := New(&Options{
		PoolSize:    2,
		PoolTimeout: time.Second,
		MaxConnAge:  time.Minute,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})

	oldConn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	freshConn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	oldConn.createdAt = time.Now().Add(-2 * time.Minute)
	freshConn.createdAt = time.Now()
	oldConn.closeFunc = func() error { return nil }
	freshConn.closeFunc = func() error { return nil }

	// Return the fresh connection first so it sits ahead of the older one.
	p.Put(context.Background(), freshConn)
	p.Put(context.Background(), oldConn)

	n, err := p.ReapStaleConns()
	if err != nil {
		t.Fatalf("ReapStaleConns() error = %v", err)
	}
	if n != 1 {
		t.Fatalf("ReapStaleConns() = %d, want 1", n)
	}
	if got := p.IdleLen(); got != 1 {
		t.Fatalf("IdleLen() = %d after reaping, want 1", got)
	}
}

func TestCanceledDialDoesNotPoisonSubsequentGet(t *testing.T) {
	var calls atomic.Int32
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		DialerContext: func(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
			if calls.Add(1) == 1 {
				<-ctx.Done()
				return nil, nil, ctx.Err()
			}
			return nil, nil, nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := p.Get(ctx)
		firstResult <- err
	}()
	deadline := time.Now().Add(time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	cancel()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Get() error = %v, want context.Canceled", err)
	}

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("second Get() error = %v, want a fresh dial", err)
	}
	if cn == nil || calls.Load() != 2 {
		t.Fatalf("second Get() connection=%v dial calls=%d, want a new connection and two calls", cn, calls.Load())
	}
}

func TestCloseInterruptsTryDialBackoff(t *testing.T) {
	dialAttempted := make(chan struct{}, 1)
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			dialAttempted <- struct{}{}
			return nil, nil, errors.New("dial failed")
		},
	})

	done := make(chan struct{})
	go func() {
		p.tryDial()
		close(done)
	}()
	<-dialAttempted
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("tryDial remained asleep after pool shutdown")
	}
}

func TestCloseConnReleasesActiveLease(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }

	if err := p.CloseConn(cn); err != nil {
		t.Fatalf("CloseConn() error = %v", err)
	}
	if got := len(p.queue); got != 0 {
		t.Fatalf("held pool turns = %d after CloseConn(active), want 0", got)
	}
}

func TestCloseConnRemovesIdleConnection(t *testing.T) {
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }
	p.Put(context.Background(), cn)

	if err := p.CloseConn(cn); err != nil {
		t.Fatalf("CloseConn() error = %v", err)
	}
	if got := p.IdleLen(); got != 0 {
		t.Fatalf("IdleLen() = %d after CloseConn(idle), want 0", got)
	}
	if got := p.Len(); got != 0 {
		t.Fatalf("Len() = %d after CloseConn(idle), want 0", got)
	}
}

func TestRemoveRefillsMinIdleAfterReleasingTurn(t *testing.T) {
	dials := make(chan struct{}, 2)
	p := New(&Options{
		PoolSize:     1,
		MinIdleConns: 1,
		PoolTimeout:  time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			dials <- struct{}{}
			return &amqp.Connection{}, nil, nil
		},
	})
	<-dials
	deadline := time.Now().Add(time.Second)
	for p.IdleLen() != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if got := p.IdleLen(); got != 1 {
		t.Fatalf("IdleLen() = %d after prewarm, want 1", got)
	}

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }
	p.Remove(context.Background(), cn, errors.New("discard"))

	select {
	case <-dials:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("minimum idle connection was not refilled after releasing the active turn")
	}
}

func TestConcurrentCloseDoesNotWaitForCloseDeadline(t *testing.T) {
	deadlineCloseStarted := make(chan struct{})
	releaseDeadlineClose := make(chan struct{})
	cn := &Conn{
		closeDeadlineFunc: func(time.Time) error {
			close(deadlineCloseStarted)
			<-releaseDeadlineClose
			return nil
		},
	}

	deadlineCloseDone := make(chan error, 1)
	go func() {
		deadlineCloseDone <- cn.CloseDeadline(time.Now().Add(time.Second))
	}()
	<-deadlineCloseStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- cn.Close() }()
	select {
	case err := <-closeDone:
		if !errors.Is(err, amqp.ErrClosed) {
			t.Fatalf("concurrent Close() error = %v, want amqp.ErrClosed", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(releaseDeadlineClose)
		t.Fatal("concurrent Close waited for CloseDeadline")
	}

	close(releaseDeadlineClose)
	if err := <-deadlineCloseDone; err != nil {
		t.Fatalf("CloseDeadline() error = %v", err)
	}
}
