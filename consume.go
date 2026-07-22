package amqpx

import (
	"context"
	"io"

	amqp "github.com/rabbitmq/amqp091-go"
)

// waitForConsumerStop cancels the AMQP consumer when either application
// shutdown or the delivery worker fails. A completed worker needs no further
// cancellation. The borrowed connection remains owned by the pool and is
// released after the command returns.
func waitForConsumerStop(
	shutdownCtx context.Context,
	workerFailed <-chan struct{},
	workerDone <-chan struct{},
	cancelConsumer func() error,
	notifyClose <-chan *amqp.Error,
) error {
	select {
	case <-shutdownCtx.Done():
		return cancelConsumer()
	case <-workerFailed:
		return cancelConsumer()
	case <-workerDone:
		return nil
	case connErr := <-notifyClose:
		// A clean AMQP shutdown CLOSES the notify channel without sending, so
		// the receive yields a nil *amqp.Error — returning that directly would
		// wrap a nil pointer in a non-nil error interface and make orderly
		// closure look like a failure.
		if connErr != nil {
			return connErr
		}

		return nil
	}
}

// drainDeliveries handles deliveries until the stream closes. After the first
// handling failure it signals the stop watcher and keeps draining without
// handling so Channel.Cancel can complete without blocking. An unexpectedly
// closed stream returns io.ErrUnexpectedEOF — deliberately: shouldRetry
// classifies it retryable, which is what re-subscribes a consumer after a
// broker restart.
func drainDeliveries(
	groupCtx context.Context,
	shutdownCtx context.Context,
	deliveries <-chan amqp.Delivery,
	workerFailed chan<- struct{},
	handle func(ctx context.Context, d *amqp.Delivery) error,
) error {
	var handleErr error

	for {
		select {
		case <-groupCtx.Done():
			return handleErr
		case delivery, ok := <-deliveries:
			if !ok {
				switch {
				case handleErr != nil:
					return handleErr
				case shutdownCtx.Err() != nil:
					return nil
				default:
					return io.ErrUnexpectedEOF
				}
			}
			if groupCtx.Err() != nil {
				return handleErr
			}

			if handleErr != nil {
				continue
			}

			if err := handle(groupCtx, &delivery); err != nil {
				handleErr = err
				close(workerFailed)
			}
		}
	}
}

// runConsumeLoop joins the stop watcher and the drain loop and returns the
// watcher's error if it failed, else the drain result. It replaces
// errgroup.WithContext with a plain join so amqpx stays dependency-free:
// groupCtx is deliberately DETACHED from cmdCtx (in-flight work must not be
// canceled mid-delivery) and is canceled when the watcher fails — handlers
// use groupCtx.Err() != nil to skip acknowledging after a failure.
func runConsumeLoop(
	cmdCtx context.Context,
	deliveries <-chan amqp.Delivery,
	notifyClose <-chan *amqp.Error,
	cancelConsumer func() error,
	handle func(ctx context.Context, d *amqp.Delivery) error,
) error {
	groupCtx, cancelGroup := context.WithCancelCause(context.Background())
	defer cancelGroup(nil)

	workerFailed := make(chan struct{})
	workerDone := make(chan struct{})
	stopResult := make(chan error, 1)

	go func() {
		err := waitForConsumerStop(cmdCtx, workerFailed, workerDone, cancelConsumer, notifyClose)
		if err != nil {
			cancelGroup(err)
		}
		stopResult <- err
	}()

	drainErr := drainDeliveries(groupCtx, cmdCtx, deliveries, workerFailed, handle)
	close(workerDone)

	if stopErr := <-stopResult; stopErr != nil {
		return stopErr
	}
	return drainErr
}
