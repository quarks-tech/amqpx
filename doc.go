// Package amqpx provides pooled AMQP 0-9-1 connection/channel pairs and
// retrying command execution on top of github.com/rabbitmq/amqp091-go.
//
// A Client is safe for concurrent Process calls after construction, provided
// values referenced through interfaces or pointers in its Config and its
// Limiter, when present, are concurrency-safe. Each Command temporarily borrows
// one connpool.Conn and its AMQP channel. The command must not retain or use
// either object after returning.
//
// Process may execute a command more than once. Applications remain
// responsible for idempotency, delivery guarantees, publisher confirms, and
// consumer acknowledgement policy. Context cancellation removes and starts
// closing the borrowed connection, then lets Process return without waiting
// indefinitely for the command goroutine. Commands must honor the supplied
// context and must not leave work running in the background.
//
// The experimental automatic Recovery feature in amqp091-go v1.12 is not
// supported. NewClient panics when Config.AMQP.Recovery is non-nil so that
// amqpx alone owns connection replacement through pool removal and redial.
package amqpx
