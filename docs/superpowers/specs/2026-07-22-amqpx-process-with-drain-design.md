# amqpx: ProcessWithDrain — long-lived command mode

**Date:** 2026-07-22 · **Ticket:** MSP-769 · **Release:** v0.3.2

## Problem

`Client.Process` treats every command as short-lived and abortable: when the
command's ctx cancels mid-run, `runCommandWithContext` (client.go:134-175)
force-closes the borrowed connection within `cancelCloseTimeout` (100ms) and
returns `ctx.Err()` without waiting for the command goroutine. Correct for a
publish; wrong for a RabbitMQ consumer, which on shutdown must do the
opposite of abort: `Channel.Cancel`, finish the in-flight delivery, ack,
drain the prefetch buffer, then release.

protoevent-go works around this today with
`internal/amqpxlifecycle.ProcessWithDrain`, which hands amqpx a
`WithCancel(WithoutCancel(ctx))` ctx so the kill-switch can never fire, and
re-implements shutdown observation inside the command. That shim is the
symptom; the contract belongs in amqpx.

## API (additive; `Process` untouched)

```go
// Config gains one field:
DrainTimeout time.Duration // budget for a ProcessWithDrain command to finish
                           // after ctx cancellation; 0 → default 30s, <0 → wait forever

// Client gains one method:
func (c *Client) ProcessWithDrain(ctx context.Context, cmd Command) error

// New sentinel:
var ErrDrainTimeout = errors.New("amqpx: drain timeout exceeded")
```

`DrainTimeout` is completed in `Config.complete()` (0 → 30s) and validated in
`validate()` alongside the other duration fields. No functional options — the
repo configures via `Config` only, and this stays that way.

## Semantics

- **Acquisition and retry:** identical to `Process`. ctx cancels pool-wait
  and dial promptly; retryable command failures (per `shouldRetry`) re-acquire
  and re-run with the existing backoff — a consumer keeps free re-subscribe
  after a broker restart.
- **ctx canceled before the command starts:** the command never runs; the
  call returns `ctx.Err()` (same as `Process`).
- **ctx cancels mid-command:** the connection is NOT closed. The command
  receives the same, now-canceled ctx and owns its shutdown: observe the
  cancellation, stop intake, drain in-flight work, return. amqpx waits up to
  `DrainTimeout` measured from the cancellation:
  - command returns within budget → its error is returned **verbatim**
    (nil = clean drain). The connection goes through the normal
    `releaseConn` classification, so a clean drain returns a healthy
    connection to the pool.
  - budget expires → the existing bounded force-close
    (`closeConnectionWithDeadline`) fires as the backstop and the call
    returns `errors.Join(ErrDrainTimeout, ctx.Err())`. Both
    `errors.Is(err, ErrDrainTimeout)` and
    `errors.Is(err, context.Canceled)` hold, so planned-shutdown checks in
    callers keep working.
- **Command contract note (documented, not enforced):** a clean drain should
  return `nil`, not `ctx.Err()` — returning the cancellation error makes the
  release classifier (`isBadConnErr`) discard a healthy connection.
- **In-drain broker operations:** the ctx the command holds is canceled, and
  amqp091's `PublishWithContext`/friends fast-path-reject canceled contexts.
  Operations the command must perform *during* drain (final acks, parking-lot
  publishes) detach locally with `context.WithoutCancel` at the call site.
  This is the command author's responsibility and is documented on
  `ProcessWithDrain`.

## Internals

`runCommandWithContext` generalizes over a cancel policy instead of
hard-coding abort:

```go
type cancelPolicy struct{ drainTimeout time.Duration } // zero value = abort (today's behavior)
```

- `Process` threads the abort policy through `doProcess`/`withConn` —
  byte-identical behavior, existing tests must pass unmodified.
- `ProcessWithDrain` reuses the same retry loop with
  `cancelPolicy{drainTimeout: cfg.DrainTimeout}`. In `runCommand`, the
  post-cancel branch arms a drain timer and keeps selecting on the command's
  `errCh`; timer expiry runs `closeConnectionWithDeadline`. The timer is
  stopped on clean return. No goroutines beyond the one that already runs
  the command.
- `releaseConn`, `shouldRetry`, `isBadConnErr`, and all of `connpool` are
  untouched.

## Docs

`doc.go`, README "Context and command contract", and the `Command`/`Process`
doc comments gain the two-mode story: `Process` = abort-on-cancel (existing
text unchanged), `ProcessWithDrain` = command-owns-shutdown, with the drain
budget, the return-nil-on-clean-drain note, and the `WithoutCancel`-for-
in-drain-operations note.

## Testing

New tests in the existing net.Pipe / fake-pool style:

- clean drain: cancel → command returns nil within budget → nil error,
  connection returned to the pool (assert via pool Len/IdleLen);
- drain timeout: stuck command → force-close at ~`DrainTimeout`,
  `errors.Is` both `ErrDrainTimeout` and `context.Canceled`;
- pre-canceled ctx → command never runs;
- retryable error then drain: command fails retryable once, re-runs, then
  drains cleanly on cancel;
- `Config`: `DrainTimeout` defaulting (0 → 30s) and `<0` = wait-forever
  (drain timer never armed);
- regression gate: every existing `Process`/`runCommandWithContext` test
  passes unmodified.

## Release & migration

- amqpx: land on `main`, tag **v0.3.2**.
- protoevent-go (separate PR, after the tag): bump require; replace
  `amqpxlifecycle.ProcessWithDrain(shutdownCtx, r.client.Process, cmd)` with
  `r.client.ProcessWithDrain(shutdownCtx, cmd)` in `receiver.go` and
  `parkinglot/receiver.go`; commands observe their own ctx for shutdown and
  detach in-drain publishes/acks with `context.WithoutCancel`; delete
  `amqpxlifecycle.ProcessWithDrain` + its tests (the package keeps
  `DrainDeliveries` and `WaitForConsumerStop` — genuine consumer
  choreography, not amqpx workarounds); set `Config.DrainTimeout` from the
  receivers' shutdown budget.
- protoevent-amqp-go is retired; no migration there.

## Out of scope

- Two-phase drain signaling (soft signal + hard cancel) — rejected during
  design as API complexity without a driving use case.
- Any change to `Process` semantics, the pool, or retry classification.
