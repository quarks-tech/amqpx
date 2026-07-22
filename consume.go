package amqpx

import (
	"context"
	"errors"
	"fmt"
	"io"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/quarks-tech/amqpx/connpool"
)

// ConsumeSpec describes one consumer subscription.
type ConsumeSpec struct {
	// Queue is the queue to consume. Required.
	Queue string
	// ConsumerTag identifies the consumer for Channel.Cancel on shutdown. Required.
	ConsumerTag string
	// Prefetch is the Qos prefetch count. Required > 0.
	Prefetch int
}

func (s ConsumeSpec) validate() error {
	switch {
	case s.Queue == "":
		return errors.New("amqpx: ConsumeSpec.Queue must not be empty")
	case s.ConsumerTag == "":
		return errors.New("amqpx: ConsumeSpec.ConsumerTag must not be empty")
	case s.Prefetch <= 0:
		return fmt.Errorf("amqpx: ConsumeSpec.Prefetch must be > 0, got %d", s.Prefetch)
	default:
		return nil
	}
}

// ConsumeWithDrain runs the AMQP consumer loop for spec, delivering each
// message to handle, composed over ProcessWithDrain: cancellation of ctx
// starts a graceful drain (the consumer is canceled, in-flight and buffered
// deliveries finish handling) bounded by Config.DrainTimeout, and a clean
// drain returns nil. Retryable failures — including an unexpectedly closed
// delivery stream, surfaced as io.ErrUnexpectedEOF, and retryable broker
// errors from NotifyClose — re-subscribe through the usual retry loop up to
// Config.MaxRetries.
//
// handle owns ALL per-delivery policy including acknowledgment: amqpx never
// calls Ack or Reject. Manual acknowledgment is mandatory (the consumer is
// opened with autoAck=false). handle's ctx is the consume group's context —
// detached from the shutdown ctx, canceled when the loop is stopping after a
// failure; check ctx.Err() != nil to skip acknowledging then. A handle error
// stops the lane: no further deliveries are handled, the remaining buffered
// ones drain unacknowledged (they redeliver later), and the error is
// returned wrapped as "amqpx: consume drain: ...". Do not retain conn or d
// after returning (the Command contract).
func (c *Client) ConsumeWithDrain(ctx context.Context, spec ConsumeSpec,
	handle func(ctx context.Context, conn *connpool.Conn, d *amqp.Delivery) error) error {
	if err := spec.validate(); err != nil {
		return err
	}
	if handle == nil {
		return errors.New("amqpx: ConsumeWithDrain handle must not be nil")
	}

	return c.ProcessWithDrain(ctx, func(cmdCtx context.Context, conn *connpool.Conn) error {
		return consumeCommand(cmdCtx, conn, spec, handle)
	})
}

// consumeCommand is one subscription attempt on the borrowed connection.
func consumeCommand(cmdCtx context.Context, conn *connpool.Conn, spec ConsumeSpec,
	handle func(ctx context.Context, conn *connpool.Conn, d *amqp.Delivery) error) error {
	channel := conn.Channel()
	if err := channel.Qos(spec.Prefetch, 0, false); err != nil {
		return fmt.Errorf("amqpx: set channel qos: %w", err)
	}

	deliveries, err := channel.Consume(spec.Queue, spec.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("amqpx: consume queue %q: %w", spec.Queue, err)
	}

	notifyClose := channel.NotifyClose(make(chan *amqp.Error, 1))
	cancelConsumer := func() error {
		if cErr := channel.Cancel(spec.ConsumerTag, false); cErr != nil {
			return fmt.Errorf("amqpx: cancel consumer %q: %w", spec.ConsumerTag, cErr)
		}
		return nil
	}

	err = runConsumeLoop(cmdCtx, deliveries, notifyClose, cancelConsumer,
		func(groupCtx context.Context, d *amqp.Delivery) error {
			return handle(groupCtx, conn, d)
		})
	if err != nil {
		return fmt.Errorf("amqpx: consume drain: %w", err)
	}
	return nil
}

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
