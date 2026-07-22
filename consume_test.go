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
