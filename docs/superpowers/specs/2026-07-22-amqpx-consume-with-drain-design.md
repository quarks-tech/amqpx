# amqpx: ConsumeWithDrain — consumer-loop API

**Date:** 2026-07-22 · **Ticket:** MSP-769 · **Release:** v0.3.4

## Problem

`ProcessWithDrain` (v0.3.2) fixed connection lifetime across cancellation, but
the consumer *loop* mechanics still live in protoevent-go's
`internal/amqpxlifecycle`: the 4-way stop select (`WaitForConsumerStop`) and
the drain-so-`Channel.Cancel`-completes read loop (`DrainDeliveries`), plus a
~90-line Qos/Consume/errgroup skeleton duplicated across two receivers.

Two problems with that placement:

1. **Invisible cross-repo coupling.** `DrainDeliveries` returns
   `io.ErrUnexpectedEOF` on unexpected delivery-channel closure *specifically
   because* amqpx's `shouldRetry` classifies it retryable — that is how
   consumers get automatic re-subscribe. Changing amqpx's classification
   table would silently break a contract it cannot see.
2. **Duplication.** The identical skeleton is maintained twice in
   protoevent-go and would be written again by any future amqpx consumer.

amqpx is already channel-aware (`connpool.Conn` carries the `Channel` it
dialed), so the loop mechanics — which are generic AMQP consumer behavior,
not protoevent policy — belong here.

## API

```go
// ConsumeSpec describes one consumer subscription.
type ConsumeSpec struct {
	Queue       string // required; the queue to consume
	ConsumerTag string // required; identifies the consumer for Channel.Cancel on shutdown
	Prefetch    int    // required > 0; Qos prefetch count
}

func (c *Client) ConsumeWithDrain(ctx context.Context, spec ConsumeSpec,
	handle func(ctx context.Context, conn *connpool.Conn, d *amqp.Delivery) error) error
```

- Validation before any I/O: empty `Queue` or `ConsumerTag`, `Prefetch <= 0`,
  or nil `handle` → descriptive error.
- Consume flags are hard-fixed: `autoAck=false` (manual ack IS the drain
  model), `exclusive=false`, `noLocal=false`, `noWait=false`, `args=nil`.
  Widening later is non-breaking; today nothing needs it.
- `handle` owns ALL per-delivery policy including acknowledgment
  (Ack/Reject/publish-then-ack). amqpx never touches Ack/Reject. The Command
  contract applies: do not retain `conn` or `d` after returning.
- A `handle` error stops the lane: no further deliveries are handled, the
  consumer is canceled, remaining buffered deliveries are drained unhandled
  (they stay unacked and redeliver later).

## Semantics

`ConsumeWithDrain` is `ProcessWithDrain` composed with a consume command —
every existing knob applies: retryable failures re-subscribe through the
retry loop (bounded by `Config.MaxRetries` with the usual backoff);
`Config.DrainTimeout` bounds the post-cancel drain with the force-close
backstop; a clean drain returns nil.

Each command attempt, on the borrowed connection's channel:

1. `Qos(spec.Prefetch, 0, false)`
2. `Consume(spec.Queue, spec.ConsumerTag, false, false, false, false, nil)`
3. `errgroup` with two goroutines (both today's protoevent internals, moved
   verbatim as unexported functions):
   - **stop watcher**: one select over {command ctx done, handler failure,
     clean worker completion, broker `NotifyClose`} → `Channel.Cancel(tag,
     false)` where cancellation is needed; a closed notify channel yields a
     nil `*amqp.Error` and MUST be treated as clean closure (the typed-nil
     regression stays pinned by a test).
   - **drain loop**: read deliveries; call `handle` until its first error,
     then keep READING without handling so `Channel.Cancel` can complete;
     on channel close return the handler's error if any, nil when the
     command ctx is done (clean shutdown), else `io.ErrUnexpectedEOF`.
4. The callback's ctx is the errgroup ctx — deliberately detached from the
   shutdown ctx (in-flight work must not be canceled mid-delivery) and
   canceled when the stop watcher exits with an error, preserving the
   `ctx.Err() != nil → skip ack` idiom callers use today.

## Error surface

No new sentinels. Returns:

- validation errors immediately;
- the `handle` error when the lane stopped on a handler failure, wrapped
  `amqpx: consume drain: %w` (`errors.Is/As` still match);
- `io.ErrUnexpectedEOF` (same wrap) when the delivery channel closed
  unexpectedly and retries were exhausted — its retryability is documented
  on `ConsumeWithDrain`, making the classification coupling explicit and
  co-located;
- nil on a clean drain, or on a clean broker close during shutdown;
- `errors.Join(ErrDrainTimeout, ctx.Err())` via the inherited
  `ProcessWithDrain` backstop.

amqpx stays logger-free; all logging remains in callers' handlers.

## Testing

amqpx's rig has no broker (the net.Pipe handshake server does not speak the
consume protocol), so coverage is unit-level and preserved, not reduced:

- all 12 lifecycle tests port from protoevent-go: every stop-select arm
  (including the typed-nil `*amqp.Error` regression), drain-after-failure,
  unexpected-EOF classification, no-handle-after-group-cancellation;
- spec validation table;
- composition test at the consume-command level with a fake delivery channel.

Full broker end-to-end is not added — status quo (protoevent's rabbitmq
module has no live-broker tests either); the e2e gap predates this change.

## Release & migration

- amqpx: implement on `MSP-769`, merge to `main`, tag **v0.3.4**.
- protoevent-go (PR #10, after the tag): bump the require; both receivers'
  `receive()` skeletons collapse to `client.ConsumeWithDrain(shutdownCtx,
  spec, handle)` with their existing ack policies unchanged (root: package
  `doAcknowledge`; parkinglot: `doAcknowledge(ctx, conn, …)` with the
  `WithoutCancel` park publish); `internal/amqpxlifecycle` and its test file
  are deleted; `Setup`/topology stays untouched.

## Out of scope (decided during design)

- Consume-flag passthrough (`Exclusive`, `NoWait`, `Args`, `autoAck`).
- Infinite re-subscribe mode (bounded `MaxRetries` retained; the app's
  supervisor owns persistent-failure policy).
- Disposition enums or split process/acknowledge callbacks — policy lives in
  one raw callback (decided over the enum variant, which would need an
  escape hatch for parking-lot's publish-then-ack anyway).
- Logging/metrics hooks in amqpx.
