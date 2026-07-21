package connpool

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type coverageObservedContext struct {
	context.Context
	observed chan struct{}
}

func (c *coverageObservedContext) Done() <-chan struct{} {
	select {
	case c.observed <- struct{}{}:
	default:
	}
	return nil
}

func newCoveragePool(t *testing.T, size int, fifo bool, timeout time.Duration) *ConnPool {
	t.Helper()

	p := New(&Options{
		PoolFIFO:    fifo,
		PoolSize:    size,
		PoolTimeout: timeout,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})
	t.Cleanup(func() {
		if err := p.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("Close() error = %v", err)
		}
	})
	return p
}

func mustGetCoverageConn(t *testing.T, p *ConnPool) *Conn {
	t.Helper()

	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	cn.closeFunc = func() error { return nil }
	return cn
}

func waitForCoverageCondition(t *testing.T, condition func() bool, failure string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for !condition() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !condition() {
		t.Fatal(failure)
	}
}

func TestWaitTurnTimeoutUpdatesStatsSnapshot(t *testing.T) {
	p := newCoveragePool(t, 1, false, 0)

	cn := mustGetCoverageConn(t, p)
	p.Put(context.Background(), cn)
	cn = mustGetCoverageConn(t, p)

	if err := p.waitTurn(context.Background()); !errors.Is(err, ErrPoolTimeout) {
		t.Fatalf("waitTurn() error = %v, want ErrPoolTimeout", err)
	}

	stats := p.Stats()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Timeouts != 1 {
		t.Fatalf("Stats() counters = hits:%d misses:%d timeouts:%d, want 1/1/1", stats.Hits, stats.Misses, stats.Timeouts)
	}
	if stats.TotalConns != 1 || stats.IdleConns != 0 || stats.StaleConns != 0 {
		t.Fatalf("Stats() sizes = total:%d idle:%d stale:%d, want 1/0/0", stats.TotalConns, stats.IdleConns, stats.StaleConns)
	}

	p.Put(context.Background(), cn)
	if stats = p.Stats(); stats.TotalConns != 1 || stats.IdleConns != 1 {
		t.Fatalf("Stats() after Put = total:%d idle:%d, want 1/1", stats.TotalConns, stats.IdleConns)
	}
}

func TestWaitTurnAcceptsReleasedTurn(t *testing.T) {
	p := newCoveragePool(t, 1, false, time.Hour)
	if err := p.getTurn(); err != nil {
		t.Fatalf("getTurn() error = %v", err)
	}

	ctx := &coverageObservedContext{
		Context:  context.Background(),
		observed: make(chan struct{}, 2),
	}
	result := make(chan error, 1)
	go func() { result <- p.waitTurn(ctx) }()

	for i := 0; i < 2; i++ {
		select {
		case <-ctx.observed:
		case <-time.After(time.Second):
			t.Fatalf("waitTurn inspected the waiting context %d times, want 2", i)
		}
	}
	p.freeTurn()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("waitTurn() error = %v after a turn was released", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitTurn did not accept the released turn")
	}
	if got := len(p.queue); got != 1 {
		t.Fatalf("held turns = %d after handoff, want 1", got)
	}
	p.freeTurn()
}

func TestWaitTurnHonorsCanceledContext(t *testing.T) {
	p := newCoveragePool(t, 1, false, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.waitTurn(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitTurn() error = %v, want context.Canceled", err)
	}
}

func TestPoolIdleCheckoutOrdering(t *testing.T) {
	tests := []struct {
		name string
		fifo bool
		want []int
	}{
		{name: "LIFO", fifo: false, want: []int{2, 1, 0}},
		{name: "FIFO", fifo: true, want: []int{0, 1, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newCoveragePool(t, 3, tt.fifo, time.Second)
			initial := make([]*Conn, 3)
			for i := range initial {
				initial[i] = mustGetCoverageConn(t, p)
			}
			for _, cn := range initial {
				p.Put(context.Background(), cn)
			}

			checkedOut := make([]*Conn, 0, len(initial))
			for _, wantIndex := range tt.want {
				got := mustGetCoverageConn(t, p)
				checkedOut = append(checkedOut, got)
				if got != initial[wantIndex] {
					t.Fatalf("Get() returned connection %p, want initial[%d] %p", got, wantIndex, initial[wantIndex])
				}
			}
			for _, cn := range checkedOut {
				p.Put(context.Background(), cn)
			}
		})
	}
}

func TestCircuitReturnsLastDialErrorAndRecovers(t *testing.T) {
	errDial := errors.New("dial failed")
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	var releaseProbeOnce sync.Once
	releaseRecoveryProbe := func() { releaseProbeOnce.Do(func() { close(releaseProbe) }) }
	closedProbe := newClosedCoverageAMQPConnection(t)
	var calls atomic.Int32

	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			switch calls.Add(1) {
			case 1:
				return nil, nil, errDial
			case 2:
				close(probeStarted)
				<-releaseProbe
				return closedProbe, nil, nil
			default:
				return &amqp.Connection{}, nil, nil
			}
		},
	})
	t.Cleanup(func() {
		if err := p.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("Close() error = %v", err)
		}
	})
	t.Cleanup(releaseRecoveryProbe)

	if _, err := p.Get(context.Background()); !errors.Is(err, errDial) {
		t.Fatalf("first Get() error = %v, want dial error", err)
	}
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("circuit recovery probe did not start")
	}

	if _, err := p.Get(context.Background()); !errors.Is(err, errDial) {
		t.Fatalf("open-circuit Get() error = %v, want last dial error", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("dial calls with circuit open = %d, want 2 including recovery probe", got)
	}
	releaseRecoveryProbe()
	waitForCoverageCondition(t, func() bool { return p.dialErrorsNum.Load() == 0 }, "successful recovery probe did not reset the circuit")

	cn := mustGetCoverageConn(t, p)
	if got := calls.Load(); got != 3 {
		t.Fatalf("dial calls after circuit recovery = %d, want 3", got)
	}
	p.Put(context.Background(), cn)
}

func newClosedCoverageAMQPConnection(t *testing.T) *amqp.Connection {
	t.Helper()

	client, server := net.Pipe()
	if err := server.Close(); err != nil {
		t.Fatalf("close in-memory AMQP peer: %v", err)
	}
	cn, err := amqp.Open(client, amqp.Config{})
	if cn == nil {
		t.Fatalf("amqp.Open() connection = nil, error = %v", err)
	}
	waitForCoverageCondition(t, cn.IsClosed, "failed in-memory AMQP connection did not close")
	return cn
}

func TestLateSuccessfulDialIsRejectedAfterPoolShutdown(t *testing.T) {
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	var releaseDialOnce sync.Once
	release := func() { releaseDialOnce.Do(func() { close(releaseDial) }) }
	lateConn := newClosedCoverageAMQPConnection(t)
	p := New(&Options{
		PoolSize:    1,
		PoolTimeout: time.Second,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			close(dialStarted)
			<-releaseDial
			return lateConn, nil, nil
		},
	})
	t.Cleanup(func() {
		release()
		_ = p.Close()
	})

	result := make(chan error, 1)
	go func() {
		_, err := p.NewConn()
		result <- err
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("late dial did not start")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	release()

	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("late NewConn() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late successful dial was not discarded after shutdown")
	}
}

func TestReaperExitsOnCloseAndDoubleCloseReturnsErrClosed(t *testing.T) {
	p := New(&Options{
		PoolSize:           1,
		PoolTimeout:        time.Second,
		IdleTimeout:        time.Hour,
		IdleCheckFrequency: time.Hour,
		Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
			return &amqp.Connection{}, nil, nil
		},
	})

	done := make(chan struct{})
	go func() {
		p.reaper(time.Hour)
		close(done)
	}()
	if err := p.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reaper did not exit after Close")
	}
	if err := p.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close() error = %v, want ErrClosed", err)
	}
}

func TestConnChannelAndNotifyCloseRecordUse(t *testing.T) {
	amqpConn := &amqp.Connection{}
	amqpChannel := &amqp.Channel{}
	cn := NewConn(amqpConn, amqpChannel)
	old := time.Unix(1, 0)

	cn.SetUsedAt(old)
	if got := cn.Channel(); got != amqpChannel {
		t.Fatalf("Channel() = %p, want %p", got, amqpChannel)
	}
	if got := cn.UsedAt(); !got.After(old) {
		t.Fatalf("UsedAt() after Channel = %v, want after %v", got, old)
	}

	cn.SetUsedAt(old)
	receiver := make(chan *amqp.Error, 1)
	if got := cn.NotifyClose(receiver); got != receiver {
		t.Fatal("NotifyClose() did not return the registered receiver")
	}
	if got := cn.UsedAt(); !got.After(old) {
		t.Fatalf("UsedAt() after NotifyClose = %v, want after %v", got, old)
	}
}

func TestConnCloseOperationsClaimOnce(t *testing.T) {
	var closeCalls atomic.Int32
	cn := &Conn{closeFunc: func() error {
		closeCalls.Add(1)
		return nil
	}}
	if err := cn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := cn.Close(); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("second Close() error = %v, want amqp.ErrClosed", err)
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("close calls = %d, want 1", got)
	}

	deadlineConn := &Conn{closeDeadlineFunc: func(time.Time) error { return nil }}
	if err := deadlineConn.CloseDeadline(time.Now()); err != nil {
		t.Fatalf("CloseDeadline() error = %v", err)
	}
	if err := deadlineConn.CloseDeadline(time.Now()); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("second CloseDeadline() error = %v, want amqp.ErrClosed", err)
	}
}

func TestNewRejectsOtherInvalidOptions(t *testing.T) {
	dialer := func() (*amqp.Connection, *amqp.Channel, error) { return nil, nil, nil }
	tests := []struct {
		name string
		opt  *Options
	}{
		{name: "nil options"},
		{name: "zero pool size", opt: &Options{Dialer: dialer}},
		{name: "negative minimum idle", opt: &Options{PoolSize: 1, MinIdleConns: -1, Dialer: dialer}},
		{name: "minimum idle exceeds pool size", opt: &Options{PoolSize: 1, MinIdleConns: 2, Dialer: dialer}},
		{name: "missing dialer", opt: &Options{PoolSize: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("New() did not panic")
				}
			}()
			_ = New(tt.opt)
		})
	}
}
