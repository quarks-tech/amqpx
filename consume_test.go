package amqpx

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/quarks-tech/amqpx/connpool"
)

func TestWaitForConsumerStopCancelsConsumerWhenWorkerFails(t *testing.T) {
	workerFailed := make(chan struct{})
	close(workerFailed)

	var cancelCalled atomic.Bool
	err := waitForConsumerStop(
		t.Context(),
		workerFailed,
		make(chan struct{}),
		func() error {
			cancelCalled.Store(true)
			return nil
		},
		make(chan *amqp.Error),
	)
	if err != nil {
		t.Fatalf("waitForConsumerStop() error = %v, want nil", err)
	}
	if !cancelCalled.Load() {
		t.Fatal("waitForConsumerStop did not cancel the consumer")
	}
}

// Regression test for the typed-nil bug: a clean AMQP shutdown CLOSES the
// notify channel without sending; the nil *amqp.Error must not become a
// non-nil error interface.
func TestWaitForConsumerStopCleanNotifyCloseReturnsNil(t *testing.T) {
	notifyClose := make(chan *amqp.Error)
	close(notifyClose)

	var cancelCalled atomic.Bool
	err := waitForConsumerStop(
		t.Context(),
		make(chan struct{}),
		make(chan struct{}),
		func() error {
			cancelCalled.Store(true)
			return nil
		},
		notifyClose,
	)
	if err != nil {
		t.Fatalf("waitForConsumerStop() error = %v (typed-nil *amqp.Error?), want nil", err)
	}
	if cancelCalled.Load() {
		t.Fatal("waitForConsumerStop canceled the consumer on a clean close")
	}
}

func TestWaitForConsumerStopNotifyCloseError(t *testing.T) {
	notifyClose := make(chan *amqp.Error, 1)
	connErr := &amqp.Error{Code: amqp.ConnectionForced, Reason: "broker restart"}
	notifyClose <- connErr

	err := waitForConsumerStop(
		t.Context(),
		make(chan struct{}),
		make(chan struct{}),
		func() error { return nil },
		notifyClose,
	)
	if !errors.Is(err, connErr) {
		t.Fatalf("waitForConsumerStop() error = %v, want %v", err, connErr)
	}
}

func TestWaitForConsumerStopWorkerDoneNeedsNoCancel(t *testing.T) {
	workerDone := make(chan struct{})
	close(workerDone)

	var cancelCalled atomic.Bool
	err := waitForConsumerStop(
		t.Context(),
		make(chan struct{}),
		workerDone,
		func() error {
			cancelCalled.Store(true)
			return nil
		},
		make(chan *amqp.Error),
	)
	if err != nil {
		t.Fatalf("waitForConsumerStop() error = %v, want nil", err)
	}
	if cancelCalled.Load() {
		t.Fatal("waitForConsumerStop canceled the consumer after a completed worker")
	}
}

func TestWaitForConsumerStopShutdownCancelsConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var cancelCalled atomic.Bool
	err := waitForConsumerStop(
		ctx,
		make(chan struct{}),
		make(chan struct{}),
		func() error {
			cancelCalled.Store(true)
			return nil
		},
		make(chan *amqp.Error),
	)
	if err != nil {
		t.Fatalf("waitForConsumerStop() error = %v, want nil", err)
	}
	if !cancelCalled.Load() {
		t.Fatal("waitForConsumerStop did not cancel the consumer on shutdown")
	}
}

func TestDrainDeliveriesSignalsFailureAndContinuesDraining(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 2)
	deliveries <- amqp.Delivery{DeliveryTag: 1}
	deliveries <- amqp.Delivery{DeliveryTag: 2}
	close(deliveries)

	wantErr := errors.New("ack failed")
	workerFailed := make(chan struct{})
	handled := 0
	err := drainDeliveries(
		t.Context(),
		t.Context(),
		deliveries,
		workerFailed,
		func(context.Context, *amqp.Delivery) error {
			handled++
			return wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("drainDeliveries() error = %v, want %v", err, wantErr)
	}
	if handled != 1 {
		t.Fatalf("handled deliveries = %d, want 1", handled)
	}

	select {
	case <-workerFailed:
	default:
		t.Fatal("drainDeliveries did not signal the handling failure")
	}
}

// recordingAcknowledger records each delivery's disposition so a drain can be
// checked for deliveries it consumed without accounting for them.
type recordingAcknowledger struct {
	acked    []uint64
	nacked   []uint64
	rejected []uint64
	requeued []uint64
}

func (a *recordingAcknowledger) Ack(tag uint64, _ bool) error {
	a.acked = append(a.acked, tag)
	return nil
}

func (a *recordingAcknowledger) Nack(tag uint64, _, requeue bool) error {
	a.nacked = append(a.nacked, tag)
	if requeue {
		a.requeued = append(a.requeued, tag)
	}
	return nil
}

func (a *recordingAcknowledger) Reject(tag uint64, requeue bool) error {
	a.rejected = append(a.rejected, tag)
	if requeue {
		a.requeued = append(a.requeued, tag)
	}
	return nil
}

// A delivery the drain reads but never hands to handle must be requeued, not
// silently consumed. Those deliveries are already prefetched, so the broker counts
// them outstanding; leaving them unacked strands them on a channel that a
// non-retryable handle error returns to the connection pool, invisible to every
// consumer until that connection is torn down.
func TestDrainDeliveriesRequeuesDeliveriesItDoesNotHandle(t *testing.T) {
	ack := &recordingAcknowledger{}

	deliveries := make(chan amqp.Delivery, 4)
	for tag := uint64(1); tag <= 4; tag++ {
		deliveries <- amqp.Delivery{DeliveryTag: tag, Acknowledger: ack}
	}
	close(deliveries)

	wantErr := errors.New("handle failed")
	err := drainDeliveries(
		t.Context(),
		t.Context(),
		deliveries,
		make(chan struct{}),
		func(context.Context, *amqp.Delivery) error { return wantErr },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("drainDeliveries() error = %v, want %v", err, wantErr)
	}

	// Tag 1 reached handle, so its disposition is handle's to own; 2-4 never did.
	if len(ack.requeued) != 3 {
		t.Fatalf("requeued = %v, want the 3 unhandled deliveries (2, 3, 4)", ack.requeued)
	}
	for i, tag := range []uint64{2, 3, 4} {
		if ack.requeued[i] != tag {
			t.Fatalf("requeued = %v, want [2 3 4]", ack.requeued)
		}
	}
	if len(ack.acked) != 0 || len(ack.rejected) != 0 {
		t.Fatalf("amqpx acked/rejected on handle's behalf: acked=%v rejected=%v", ack.acked, ack.rejected)
	}
}

// The same guarantee on the cancellation path: a delivery taken off the channel and
// then abandoned because the group is stopping is just as stranded as one abandoned
// after a failure.
//
// Both select cases are ready, so the loop may take either. Assert the invariant per
// run — a delivery is either still queued or requeued, never silently consumed — and
// repeat so the dequeuing branch is certain to occur; a conditional assertion would
// pass on roughly half of all runs with the requeue removed.
func TestDrainDeliveriesRequeuesOnGroupCancellation(t *testing.T) {
	const runs = 200

	dequeued := 0

	for i := range runs {
		ack := &recordingAcknowledger{}

		groupCtx, cancelGroup := context.WithCancel(t.Context())
		cancelGroup()

		deliveries := make(chan amqp.Delivery, 1)
		deliveries <- amqp.Delivery{DeliveryTag: 7, Acknowledger: ack}

		_ = drainDeliveries(
			groupCtx,
			t.Context(),
			deliveries,
			make(chan struct{}),
			func(context.Context, *amqp.Delivery) error {
				t.Fatal("handler called after group cancellation")
				return nil
			},
		)

		if len(deliveries) == 1 {
			if len(ack.requeued) != 0 {
				t.Fatalf("run %d: delivery left queued AND requeued (%v): it would be delivered twice", i, ack.requeued)
			}
			continue
		}

		dequeued++
		if len(ack.requeued) != 1 || ack.requeued[0] != 7 {
			t.Fatalf("run %d: delivery was taken off the channel but requeued = %v, want [7]", i, ack.requeued)
		}
	}

	if dequeued == 0 {
		t.Skipf("the dequeuing branch was never taken across %d runs; the requeue path went unexercised", runs)
	}
}

// A clean drain must not requeue anything: every delivery reaches handle, which owns
// the disposition.
func TestDrainDeliveriesRequeuesNothingOnCleanDrain(t *testing.T) {
	ack := &recordingAcknowledger{}

	deliveries := make(chan amqp.Delivery, 3)
	for tag := uint64(1); tag <= 3; tag++ {
		deliveries <- amqp.Delivery{DeliveryTag: tag, Acknowledger: ack}
	}
	close(deliveries)

	handled := 0
	err := drainDeliveries(
		t.Context(),
		t.Context(),
		deliveries,
		make(chan struct{}),
		func(_ context.Context, d *amqp.Delivery) error {
			handled++
			return d.Ack(false)
		},
	)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("drainDeliveries() error = %v, want io.ErrUnexpectedEOF", err)
	}
	if handled != 3 {
		t.Fatalf("handled = %d, want 3", handled)
	}
	if len(ack.requeued) != 0 {
		t.Fatalf("requeued = %v on a clean drain, want none", ack.requeued)
	}
	if len(ack.acked) != 3 {
		t.Fatalf("acked = %v, want all three handled by handle", ack.acked)
	}
}

func TestDrainDeliveriesReturnsUnexpectedEOFWhenStreamCloses(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	close(deliveries)

	err := drainDeliveries(
		t.Context(),
		t.Context(),
		deliveries,
		make(chan struct{}),
		func(context.Context, *amqp.Delivery) error {
			t.Fatal("handler called for a closed delivery stream")
			return nil
		},
	)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("drainDeliveries() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestDrainDeliveriesDoesNotHandleAfterGroupCancellation(t *testing.T) {
	handled := 0

	for range 100 {
		groupCtx, cancelGroup := context.WithCancel(t.Context())
		cancelGroup()

		deliveries := make(chan amqp.Delivery, 1)
		deliveries <- amqp.Delivery{}

		_ = drainDeliveries(
			groupCtx,
			t.Context(),
			deliveries,
			make(chan struct{}),
			func(context.Context, *amqp.Delivery) error {
				handled++
				return nil
			},
		)
	}

	if handled != 0 {
		t.Fatalf("handled deliveries after cancellation = %d, want 0", handled)
	}
}

// Clean shutdown: cmdCtx cancels, the watcher cancels the consumer, the
// broker flushes and closes the stream — nil, no error.
func TestRunConsumeLoopCleanShutdownReturnsNil(t *testing.T) {
	cmdCtx, cancel := context.WithCancel(t.Context())
	deliveries := make(chan amqp.Delivery)
	var cancelCalled atomic.Bool

	result := make(chan error, 1)
	go func() {
		result <- runConsumeLoop(cmdCtx, deliveries, make(chan *amqp.Error),
			func() error {
				cancelCalled.Store(true)
				close(deliveries) // broker closes the stream after Cancel
				return nil
			},
			func(context.Context, *amqp.Delivery) error { return nil },
		)
	}()

	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runConsumeLoop() error = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runConsumeLoop did not return after shutdown drain")
	}
	if !cancelCalled.Load() {
		t.Fatal("consumer was not canceled on shutdown")
	}
}

// Handler failure: first error stops handling, the consumer is canceled, the
// remaining deliveries drain unhandled, and the handler's error is returned.
func TestRunConsumeLoopReturnsHandlerErrorAndDrains(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 3)
	deliveries <- amqp.Delivery{DeliveryTag: 1}
	deliveries <- amqp.Delivery{DeliveryTag: 2}
	deliveries <- amqp.Delivery{DeliveryTag: 3}

	wantErr := errors.New("ack failed")
	handled := 0
	err := runConsumeLoop(t.Context(), deliveries, make(chan *amqp.Error),
		func() error {
			close(deliveries) // Cancel completes because draining continues
			return nil
		},
		func(context.Context, *amqp.Delivery) error {
			handled++
			return wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("runConsumeLoop() error = %v, want the handler's error", err)
	}
	if handled != 1 {
		t.Fatalf("handled = %d, want 1 (no handling after the first failure)", handled)
	}
}

// Watcher failure (broker-initiated close with a real error) cancels the
// group ctx: with no deliveries ever arriving and the stream never closing,
// the drain loop can ONLY return via groupCtx cancellation — so this test
// returning at all proves the watcher's error canceled the group, and the
// broker error must be what comes back.
func TestRunConsumeLoopWatcherErrorCancelsGroup(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	notifyClose := make(chan *amqp.Error, 1)
	connErr := &amqp.Error{Code: amqp.ConnectionForced, Reason: "broker restart"}
	notifyClose <- connErr

	err := runConsumeLoop(t.Context(), deliveries, notifyClose,
		func() error { return nil },
		func(context.Context, *amqp.Delivery) error { return nil },
	)
	if !errors.Is(err, connErr) {
		t.Fatalf("runConsumeLoop() error = %v, want the broker error", err)
	}
}

// Unexpected stream closure (no shutdown, no failure) surfaces
// io.ErrUnexpectedEOF for the retry loop to classify.
func TestRunConsumeLoopUnexpectedCloseReturnsEOF(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	close(deliveries)

	err := runConsumeLoop(t.Context(), deliveries, make(chan *amqp.Error),
		func() error { return nil },
		func(context.Context, *amqp.Delivery) error { return nil },
	)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("runConsumeLoop() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// A handler blocked mid-delivery observes the group cancellation LIVE when
// the watcher fails: handlingStarted gates the broker-error send, so the
// delivery is always picked up before the watcher can fire — deterministic,
// leak-free ordering.
func TestRunConsumeLoopHandlerObservesLiveCancellation(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{DeliveryTag: 1}
	notifyClose := make(chan *amqp.Error, 1)
	connErr := &amqp.Error{Code: amqp.ConnectionForced, Reason: "broker restart"}

	handlingStarted := make(chan struct{})
	go func() {
		<-handlingStarted
		notifyClose <- connErr
	}()

	sawLiveCancel := false
	err := runConsumeLoop(t.Context(), deliveries, notifyClose,
		func() error { return nil },
		func(ctx context.Context, _ *amqp.Delivery) error {
			close(handlingStarted)
			<-ctx.Done() // must be woken by the watcher's cancelGroup, not return early
			sawLiveCancel = true
			return nil
		},
	)
	if !errors.Is(err, connErr) {
		t.Fatalf("runConsumeLoop() error = %v, want the broker error", err)
	}
	if !sawLiveCancel {
		t.Fatal("handler did not observe the live group cancellation")
	}
}

func TestConsumeWithDrainValidatesSpec(t *testing.T) {
	client := &Client{config: &Config{}}
	okHandle := func(context.Context, *connpool.Conn, *amqp.Delivery) error { return nil }

	cases := map[string]struct {
		spec    ConsumeSpec
		handle  func(context.Context, *connpool.Conn, *amqp.Delivery) error
		wantSub string
	}{
		"empty queue":   {ConsumeSpec{ConsumerTag: "tag", Prefetch: 1}, okHandle, "Queue"},
		"empty tag":     {ConsumeSpec{Queue: "q", Prefetch: 1}, okHandle, "ConsumerTag"},
		"zero prefetch": {ConsumeSpec{Queue: "q", ConsumerTag: "tag"}, okHandle, "Prefetch"},
		"neg prefetch":  {ConsumeSpec{Queue: "q", ConsumerTag: "tag", Prefetch: -1}, okHandle, "Prefetch"},
		"nil handle":    {ConsumeSpec{Queue: "q", ConsumerTag: "tag", Prefetch: 1}, nil, "handle"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := client.ConsumeWithDrain(context.Background(), tc.spec, tc.handle)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error = %v, want mention of %q", err, tc.wantSub)
			}
		})
	}
}
