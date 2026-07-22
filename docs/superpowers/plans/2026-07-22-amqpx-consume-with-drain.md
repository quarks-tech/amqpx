# amqpx ConsumeWithDrain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Client.ConsumeWithDrain` — the AMQP consumer loop (Qos, Consume, 4-way stop select, drain-so-Cancel-completes read loop) composed over `ProcessWithDrain` — then migrate protoevent-go's receivers onto it and delete `internal/amqpxlifecycle`.

**Architecture:** New `consume.go` in amqpx root: `waitForConsumerStop` and `drainDeliveries` move verbatim (unexported) from protoevent-go's `amqpxlifecycle`; `runConsumeLoop` joins them with a plain two-goroutine join over `context.WithCancelCause` (errgroup semantics without the `x/sync` dependency); `consumeCommand` does Qos/Consume/NotifyClose wiring; `ConsumeWithDrain` validates the spec and delegates to `ProcessWithDrain`. protoevent-go's receivers collapse to spec + handler callback.

**Tech Stack:** Go 1.26 (amqpx go.mod pins `go 1.26.4` — do NOT bump), sole dep `github.com/rabbitmq/amqp091-go v1.13.0`. Repos: `/Users/filenko/go/src/github.com/quarks-tech/amqpx` (branch `MSP-769`), then `/Users/filenko/go/src/github.com/quarks-tech/protoevent-go` (branch `MSP-769`).

**Spec:** `docs/superpowers/specs/2026-07-22-amqpx-consume-with-drain-design.md`

## Global Constraints

- **No new dependencies** in amqpx — the errgroup wiring from protoevent-go is replaced with a plain goroutine join (Task 2); `go.mod`/`go.sum` must not change.
- `Process`, `ProcessWithDrain`, `cancelPolicy`, `drainCommand`, retry classification: untouched.
- Names fixed by spec: `ConsumeSpec{Queue, ConsumerTag, Prefetch}`, `ConsumeWithDrain`; consume flags hard-fixed `(autoAck=false, exclusive=false, noLocal=false, noWait=false, args=nil)`; wrap text exactly `"amqpx: consume drain: %w"`.
- Error contract: handler error wrapped verbatim-matchable; unexpected delivery-channel closure = `io.ErrUnexpectedEOF` (same wrap) — deliberately retryable per `shouldRetry`; clean drain / clean broker-close-during-shutdown = nil.
- Callback ctx = the group ctx: detached from the command ctx, canceled when the stop watcher fails (preserves the `ctx.Err() != nil → skip ack` idiom).
- Commit messages `MSP-769: <description>` + trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`. TDD per task.
- amqpx work runs from `/Users/filenko/go/src/github.com/quarks-tech/amqpx` on branch `MSP-769`; Task 5 runs in protoevent-go.

---

### Task 1: Loop primitives — waitForConsumerStop + drainDeliveries

**Files:**
- Create: `consume.go` (amqpx repo root)
- Create: `consume_test.go`

**Interfaces:**
- Produces (consumed by Task 2):
  - `func waitForConsumerStop(shutdownCtx context.Context, workerFailed <-chan struct{}, workerDone <-chan struct{}, cancelConsumer func() error, notifyClose <-chan *amqp.Error) error`
  - `func drainDeliveries(groupCtx context.Context, shutdownCtx context.Context, deliveries <-chan amqp.Delivery, workerFailed chan<- struct{}, handle func(ctx context.Context, d *amqp.Delivery) error) error`
- These are verbatim ports of protoevent-go's `internal/amqpxlifecycle` functions (source of truth for behavior: `/Users/filenko/go/src/github.com/quarks-tech/protoevent-go/pkg/transport/rabbitmq/internal/amqpxlifecycle/lifecycle.go`), renamed to unexported and with the `process` parameter renamed `handle`.

- [ ] **Step 1: Write the failing tests**

Create `consume_test.go` — the 8 lifecycle tests ported (package `amqpx`, names/assertions preserved, `DrainDeliveries`→`drainDeliveries` etc.):

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run 'TestWaitForConsumerStop|TestDrainDeliveries' -v .`
Expected: compile errors — `undefined: waitForConsumerStop`, `undefined: drainDeliveries`.

- [ ] **Step 3: Implement the primitives**

Create `consume.go`:

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run 'TestWaitForConsumerStop|TestDrainDeliveries' -count=1 -v .`
Expected: 8× PASS.

- [ ] **Step 5: Commit**

```bash
git add consume.go consume_test.go
git commit -m "MSP-769: port consumer stop/drain primitives from protoevent-go

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: runConsumeLoop — the broker-free composition seam

**Files:**
- Modify: `consume.go` (append)
- Modify: `consume_test.go` (append)

**Interfaces:**
- Consumes: `waitForConsumerStop`, `drainDeliveries` (Task 1, exact signatures above).
- Produces (consumed by Task 3): `func runConsumeLoop(cmdCtx context.Context, deliveries <-chan amqp.Delivery, notifyClose <-chan *amqp.Error, cancelConsumer func() error, handle func(ctx context.Context, d *amqp.Delivery) error) error`

- [ ] **Step 1: Write the failing tests**

Append to `consume_test.go` (add `"time"` to imports):

```go
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

// Watcher failure (broker-initiated close with a real error): the group ctx
// is canceled — the callback's skip-ack idiom observes it — and the broker
// error is returned.
func TestRunConsumeLoopWatcherErrorCancelsGroup(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	notifyClose := make(chan *amqp.Error, 1)
	connErr := &amqp.Error{Code: amqp.ConnectionForced, Reason: "broker restart"}
	notifyClose <- connErr

	groupCanceled := make(chan struct{})
	deliveries <- amqp.Delivery{}
	go func() {
		// Give the watcher its error, then close the stream so drain returns.
		<-groupCanceled
		close(deliveries)
	}()

	err := runConsumeLoop(t.Context(), deliveries, notifyClose,
		func() error { return nil },
		func(ctx context.Context, _ *amqp.Delivery) error {
			// Wait for the group ctx to observe the watcher failure.
			<-ctx.Done()
			close(groupCanceled)
			return nil
		},
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestRunConsumeLoop -v .`
Expected: compile error — `undefined: runConsumeLoop`.

- [ ] **Step 3: Implement**

Append to `consume.go`:

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race -run 'TestRunConsumeLoop|TestWaitForConsumerStop|TestDrainDeliveries' -count=1 -v .`
Expected: 12× PASS.

- [ ] **Step 5: Commit**

```bash
git add consume.go consume_test.go
git commit -m "MSP-769: add runConsumeLoop — dependency-free stop/drain join

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: ConsumeSpec + ConsumeWithDrain + docs

**Files:**
- Modify: `consume.go` (prepend the public API above the primitives)
- Modify: `consume_test.go` (append validation tests)
- Modify: `README.md` (new "Consuming" subsection under the context/command contract section)

**Interfaces:**
- Consumes: `runConsumeLoop` (Task 2), `(*Client).ProcessWithDrain` (existing).
- Produces (consumed by Task 5): `type ConsumeSpec struct { Queue, ConsumerTag string; Prefetch int }` and `func (c *Client) ConsumeWithDrain(ctx context.Context, spec ConsumeSpec, handle func(ctx context.Context, conn *connpool.Conn, d *amqp.Delivery) error) error`.

- [ ] **Step 1: Write the failing tests**

Append to `consume_test.go` (add `"strings"` and `"github.com/quarks-tech/amqpx/connpool"` to imports):

```go
func TestConsumeWithDrainValidatesSpec(t *testing.T) {
	client := &Client{config: &Config{}}
	okHandle := func(context.Context, *connpool.Conn, *amqp.Delivery) error { return nil }

	cases := map[string]struct {
		spec    ConsumeSpec
		handle  func(context.Context, *connpool.Conn, *amqp.Delivery) error
		wantSub string
	}{
		"empty queue":    {ConsumeSpec{ConsumerTag: "tag", Prefetch: 1}, okHandle, "Queue"},
		"empty tag":      {ConsumeSpec{Queue: "q", Prefetch: 1}, okHandle, "ConsumerTag"},
		"zero prefetch":  {ConsumeSpec{Queue: "q", ConsumerTag: "tag"}, okHandle, "Prefetch"},
		"neg prefetch":   {ConsumeSpec{Queue: "q", ConsumerTag: "tag", Prefetch: -1}, okHandle, "Prefetch"},
		"nil handle":     {ConsumeSpec{Queue: "q", ConsumerTag: "tag", Prefetch: 1}, nil, "handle"},
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestConsumeWithDrainValidatesSpec -v .`
Expected: compile errors — `undefined: ConsumeSpec`, `ConsumeWithDrain`.

- [ ] **Step 3: Implement the public API**

Prepend to `consume.go` (below `package amqpx`; imports gain `"errors"`, `"fmt"`, `"github.com/quarks-tech/amqpx/connpool"`):

```go
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
```

- [ ] **Step 4: Add the README section**

In `README.md`, after the "Context and command contract" section's `ProcessWithDrain` paragraph, add:

```markdown
### Consuming

`ConsumeWithDrain` is the consumer loop over `ProcessWithDrain`: it opens the
consumer (`Qos` + `Consume`, manual acknowledgment only), delivers each
message to your handler, and on ctx cancellation cancels the consumer and
drains buffered deliveries before releasing the connection —
`Config.DrainTimeout` bounds the drain. The handler owns all policy,
including `Ack`/`Reject`; its ctx is canceled when the loop is stopping
after a failure (check `ctx.Err()` to skip acknowledging then). Retryable
failures — a broker restart closing the stream, retryable AMQP errors —
re-subscribe through the retry loop up to `Config.MaxRetries`.

    err := client.ConsumeWithDrain(ctx, amqpx.ConsumeSpec{
        Queue:       "orders",
        ConsumerTag: "orders-worker-1",
        Prefetch:    10,
    }, func(ctx context.Context, conn *connpool.Conn, d *amqp.Delivery) error {
        if err := process(d); err != nil {
            return d.Reject(true)
        }
        return d.Ack(false)
    })
```

- [ ] **Step 5: Run the full suite**

Run: `gofmt -l . && go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: gofmt silent; all tests PASS (new validation table + all Task 1/2 tests + entire pre-existing suite). `git diff go.mod go.sum` must be empty (no new deps).

- [ ] **Step 6: Commit**

```bash
git add consume.go consume_test.go README.md
git commit -m "MSP-769: add Client.ConsumeWithDrain consumer-loop API

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Gate, push, release v0.3.4

- [ ] **Step 1: Full gate**

Run: `gofmt -l . && go build ./... && go vet ./... && go test -race -count=1 ./...` — all green; `golangci-lint run ./...` if installed — 0 issues.

- [ ] **Step 2: Push**

```bash
git push -u origin MSP-769
```

- [ ] **Step 3: Release (operator gate — requires merge to main first)**

Merge the PR into `main` per team flow, then:

```bash
git checkout main && git pull
git tag v0.3.4
git push origin v0.3.4
```

---

### Task 5: protoevent-go migration (after the v0.3.4 tag exists)

**Files (repo `/Users/filenko/go/src/github.com/quarks-tech/protoevent-go`, branch `MSP-769`):**
- Modify: `pkg/transport/rabbitmq/go.mod` (+go.sum) — bump `github.com/quarks-tech/amqpx` to `v0.3.4`
- Modify: `pkg/transport/rabbitmq/receiver.go` — replace `Receive`/`receive`
- Modify: `pkg/transport/rabbitmq/parkinglot/receiver.go` — replace `Receive`/`receive`
- Delete: `pkg/transport/rabbitmq/internal/amqpxlifecycle/` (both files)

**Interfaces:**
- Consumes: `amqpx.ConsumeSpec`, `(*amqpx.Client).ConsumeWithDrain` (Task 3 signatures).
- Preserved untouched: `processDelivery`, package-level `doAcknowledge` (root receiver), `(*Receiver).doAcknowledge`/`putIntoParkingLot` (parkinglot), `Setup`/topology, all receiver options.

- [ ] **Step 1: Bump the dependency**

```bash
cd /Users/filenko/go/src/github.com/quarks-tech/protoevent-go/pkg/transport/rabbitmq
go get github.com/quarks-tech/amqpx@v0.3.4 && go mod tidy
```

- [ ] **Step 2: Migrate the root receiver**

In `pkg/transport/rabbitmq/receiver.go`, replace the `Receive` method AND delete the entire `receive` method:

```go
// Receive consumes via amqpx.Client.ConsumeWithDrain (drain-on-cancel mode):
// shutdownCtx cancellation stops connection acquisition, while a running
// consumer drains in-flight and buffered deliveries and returns nil — a
// clean shutdown yields nil, not context.Canceled. The client's
// Config.DrainTimeout bounds the drain; size it to the deployment's
// shutdown budget.
func (r *Receiver) Receive(shutdownCtx context.Context, processor eventbus.Processor) error {
	return r.client.ConsumeWithDrain(shutdownCtx, amqpx.ConsumeSpec{
		Queue:       r.options.incomingQueue,
		ConsumerTag: r.options.consumerTag,
		Prefetch:    r.options.prefetchCount,
	}, func(ctx context.Context, _ *connpool.Conn, d *amqp.Delivery) error {
		dErr := r.processDelivery(d, processor)
		if ctx.Err() != nil {
			return nil // loop is stopping after a failure: skip the ack
		}
		return doAcknowledge(d, dErr, r.options.requeueOnError)
	})
}
```

Update imports: add `"github.com/quarks-tech/amqpx"`; remove `"golang.org/x/sync/errgroup"` and the `amqpxlifecycle` import (keep `amqp`, `connpool`).

- [ ] **Step 3: Migrate the parkinglot receiver**

In `pkg/transport/rabbitmq/parkinglot/receiver.go`, same replacement:

```go
// Receive consumes via amqpx.Client.ConsumeWithDrain (drain-on-cancel mode):
// shutdownCtx cancellation stops connection acquisition, while a running
// consumer drains in-flight and buffered deliveries and returns nil — a
// clean shutdown yields nil, not context.Canceled. The client's
// Config.DrainTimeout bounds the drain; size it to the deployment's
// shutdown budget.
func (r *Receiver) Receive(shutdownCtx context.Context, processor eventbus.Processor) error {
	return r.client.ConsumeWithDrain(shutdownCtx, amqpx.ConsumeSpec{
		Queue:       r.options.incomingQueue,
		ConsumerTag: r.options.consumerTag,
		Prefetch:    r.options.prefetchCount,
	}, func(ctx context.Context, conn *connpool.Conn, d *amqp.Delivery) error {
		dErr := r.processDelivery(d, processor)
		if ctx.Err() != nil {
			return nil // loop is stopping after a failure: skip the ack
		}
		if ackErr := r.doAcknowledge(ctx, conn, d, dErr); ackErr != nil {
			return fmt.Errorf("do acknowledge: %w", ackErr)
		}
		return nil
	})
}
```

Delete the `receive` method. Update imports: add `"github.com/quarks-tech/amqpx"` (already present for `amqpx.Client`); remove `"golang.org/x/sync/errgroup"` and the `amqpxlifecycle` import.

- [ ] **Step 4: Delete the shim package**

```bash
git rm -r pkg/transport/rabbitmq/internal/amqpxlifecycle
```

- [ ] **Step 5: Full gate**

Run from `pkg/transport/rabbitmq`: `gofmt -l . && go build ./... && go vet ./... && go test -race -count=1 ./... && golangci-lint run ./...`
Expected: all green (the parkinglot/sender internal tests are unaffected — they never touched the deleted package). `go mod tidy` must drop `golang.org/x/sync` from go.mod if nothing else uses it (check `grep errgroup -r .` — expect no hits).

- [ ] **Step 6: Commit and push**

```bash
git add -A pkg/transport/rabbitmq
git commit -m "MSP-769: migrate consumers to amqpx v0.3.4 ConsumeWithDrain

Both receivers collapse to ConsumeSpec + their ack-policy callback; the
consumer loop (stop select, drain-so-Cancel-completes, re-subscribe
classification) now lives in amqpx next to the retry table it depends
on. internal/amqpxlifecycle is deleted.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```
