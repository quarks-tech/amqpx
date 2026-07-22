package amqpx

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
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
