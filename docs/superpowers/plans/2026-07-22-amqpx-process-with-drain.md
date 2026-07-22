# amqpx ProcessWithDrain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a long-lived command mode (`Client.ProcessWithDrain` + `Config.DrainTimeout`) so ctx cancellation stops acquisition but lets a running command drain gracefully, with a bounded force-close backstop.

**Architecture:** Generalize `runCommandWithContext` over a `cancelPolicy` (zero value = today's abort mode); `Process` and the new `ProcessWithDrain` share one retry loop (`process`) that threads the policy through `doProcess`/`withConn`. Pool, release classification, and retry classification are untouched.

**Tech Stack:** Go 1.26 (module pins `go 1.26.4` — do NOT bump in this work), sole dep `github.com/rabbitmq/amqp091-go v1.13.0`. Repo: `/Users/filenko/go/src/github.com/quarks-tech/amqpx`, branch `MSP-769`.

**Spec:** `docs/superpowers/specs/2026-07-22-amqpx-process-with-drain-design.md`

## Global Constraints

- `Process` observable behavior stays byte-identical; existing test assertions must not change (two existing tests get a mechanical call-site update only — an added `cancelPolicy{}` argument).
- No new dependencies. No functional options — configuration via `Config` only.
- Names fixed by spec: `ProcessWithDrain`, `Config.DrainTimeout`, `ErrDrainTimeout`, sentinel text `"amqpx: drain timeout exceeded"`.
- `DrainTimeout` semantics: `0` → default `30 * time.Second` (in `Config.complete()`), negative → wait forever. No `validate()` rule (all values legal after complete).
- Drain-mode error contract: command's error returned verbatim (nil = clean drain); on backstop force-close return `errors.Join(ErrDrainTimeout, ctx.Err())` so `errors.Is` matches both.
- Commit messages: `MSP-769: <description>` (repo convention), trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Run all commands from the repo root `/Users/filenko/go/src/github.com/quarks-tech/amqpx`.

---

### Task 1: Config.DrainTimeout

**Files:**
- Modify: `config.go` (field after `DialTimeout` at ~line 57; default in `complete()` at ~line 92)
- Test: `config_drain_test.go` (new file)

**Interfaces:**
- Produces: `Config.DrainTimeout time.Duration`, completed so that `0 → 30s`, negative preserved. Task 3 reads `c.config.DrainTimeout` after `complete()`.

- [ ] **Step 1: Write the failing tests**

Create `config_drain_test.go`:

```go
package amqpx

import (
	"testing"
	"time"
)

func TestConfigCompleteDefaultsDrainTimeout(t *testing.T) {
	cfg := &Config{}
	cfg.complete()
	if cfg.DrainTimeout != 30*time.Second {
		t.Fatalf("DrainTimeout = %v, want 30s default", cfg.DrainTimeout)
	}
}

func TestConfigCompleteKeepsNegativeDrainTimeout(t *testing.T) {
	cfg := &Config{DrainTimeout: -1}
	cfg.complete()
	if cfg.DrainTimeout != -1 {
		t.Fatalf("DrainTimeout = %v, want -1 (wait forever) preserved", cfg.DrainTimeout)
	}
}

func TestConfigCompleteKeepsExplicitDrainTimeout(t *testing.T) {
	cfg := &Config{DrainTimeout: 5 * time.Second}
	cfg.complete()
	if cfg.DrainTimeout != 5*time.Second {
		t.Fatalf("DrainTimeout = %v, want 5s preserved", cfg.DrainTimeout)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestConfigComplete -v ./...`
Expected: compile error `cfg.DrainTimeout undefined (type *Config has no field or method DrainTimeout)`

- [ ] **Step 3: Add the field and default**

In `config.go`, insert after the `DialTimeout` field (line ~57, before the `// Type of connection pool.` comment):

```go
	// DrainTimeout bounds how long a ProcessWithDrain command may keep its
	// borrowed connection after its context is canceled: the command is
	// expected to observe the cancellation, drain, and return within this
	// budget, after which the connection is force-closed as a backstop.
	// Default is 30 seconds; a negative value waits forever. Ignored by
	// Process (which force-closes on cancellation immediately).
	DrainTimeout time.Duration
```

In `Config.complete()`, insert after the `IdleCheckFrequency` block (line ~107):

```go
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 30 * time.Second
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestConfigComplete -v ./...`
Expected: 3× PASS

- [ ] **Step 5: Commit**

```bash
git add config.go config_drain_test.go
git commit -m "MSP-769: add Config.DrainTimeout (default 30s, negative = wait forever)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: cancelPolicy + drain branch in runCommandWithContext + ErrDrainTimeout

**Files:**
- Modify: `client.go` (sentinel near line 28; `runCommandWithContext` at lines 134-159; new `drainCommand` below it)
- Modify: `client_test.go` — two mechanical call-site updates (`TestRunCommandWithContextDoesNotWaitForStuckCommand` ~line 696, `TestRunCommandWithContextDoesNotStartAfterCancellation` ~line 741): add `cancelPolicy{},` as the second argument. No assertion changes.
- Test: `client_drain_test.go` (new file)

**Interfaces:**
- Produces:
  - `type cancelPolicy struct { drainTimeout time.Duration }` — zero value = abort mode.
  - `runCommandWithContext(ctx context.Context, policy cancelPolicy, run func() error, closeConn func(time.Time) error) error`
  - `var ErrDrainTimeout = errors.New("amqpx: drain timeout exceeded")`
- Consumes: `closeConnectionWithDeadline` (existing, unchanged).

- [ ] **Step 1: Write the failing tests**

Create `client_drain_test.go`:

```go
package amqpx

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Drain mode: after cancellation the command keeps its connection and its
// clean return is honored — no force-close, nil error.
func TestRunCommandWithContextDrainWaitsForCleanReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 5 * time.Second},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		t.Fatalf("returned %v before the command finished draining", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCommand)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("error = %v, want nil (clean drain)", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return after the command drained")
	}
	select {
	case <-closeCalled:
		t.Fatal("connection was closed during a clean drain")
	default:
	}
}

// Drain mode: the command's own error passes through verbatim.
func TestRunCommandWithContextDrainReturnsCommandErrorVerbatim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	cmdErr := errors.New("handler failed mid-drain")

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 5 * time.Second},
			func() error {
				close(commandStarted)
				return cmdErr
			},
			func(time.Time) error { return nil },
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, cmdErr) {
			t.Fatalf("error = %v, want the command's own error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return the command error")
	}
}

// Drain mode backstop: a command that ignores cancellation is force-closed at
// the drain deadline; the error matches BOTH ErrDrainTimeout and the ctx error.
func TestRunCommandWithContextDrainTimeoutForceCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	defer close(releaseCommand)
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: 50 * time.Millisecond},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("error = %v, want ErrDrainTimeout", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled preserved for planned-shutdown checks", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain timeout did not fire")
	}
	select {
	case <-closeCalled:
	case <-time.After(time.Second):
		t.Fatal("connection was not force-closed at the drain deadline")
	}
}

// Negative drainTimeout waits forever: no timer, no close, command owns exit.
func TestRunCommandWithContextDrainNegativeWaitsForever(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandStarted := make(chan struct{})
	releaseCommand := make(chan struct{})
	closeCalled := make(chan struct{}, 1)

	result := make(chan error, 1)
	go func() {
		result <- runCommandWithContext(
			ctx,
			cancelPolicy{drainTimeout: -1},
			func() error {
				close(commandStarted)
				<-releaseCommand
				return nil
			},
			func(time.Time) error {
				closeCalled <- struct{}{}
				return nil
			},
		)
	}()

	<-commandStarted
	cancel()

	select {
	case err := <-result:
		t.Fatalf("returned %v; negative drain timeout must wait for the command", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseCommand)
	if err := <-result; err != nil {
		t.Fatalf("error = %v, want nil", err)
	}
	select {
	case <-closeCalled:
		t.Fatal("connection was closed despite wait-forever drain")
	default:
	}
}

// Drain mode inherits the pre-cancel guard: an already-canceled ctx never
// starts the command (identical to abort mode).
func TestRunCommandWithContextDrainDoesNotStartAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	commandCalled := make(chan struct{}, 1)
	err := runCommandWithContext(
		ctx,
		cancelPolicy{drainTimeout: time.Second},
		func() error {
			commandCalled <- struct{}{}
			return nil
		},
		func(time.Time) error { return nil },
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	select {
	case <-commandCalled:
		t.Fatal("command started with an already canceled context")
	default:
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestRunCommandWithContextDrain -v ./...`
Expected: compile errors — `undefined: cancelPolicy`, `undefined: ErrDrainTimeout`, and wrong argument count for `runCommandWithContext`.

- [ ] **Step 3: Implement**

In `client.go`, add below the `cancelCloseTimeout` const (line ~28):

```go
// ErrDrainTimeout reports that a ProcessWithDrain command did not finish
// within Config.DrainTimeout after its context was canceled; the borrowed
// connection was force-closed as a backstop. It is returned joined with the
// context error, so errors.Is(err, context.Canceled) also holds.
var ErrDrainTimeout = errors.New("amqpx: drain timeout exceeded")

// cancelPolicy selects what happens to the borrowed connection when ctx
// cancels mid-command. The zero value is abort mode (Process): force-close
// the connection immediately and return ctx.Err(). A non-zero drainTimeout
// waits that long for the command to return on its own before the
// force-close backstop fires; a negative value waits forever.
type cancelPolicy struct {
	drainTimeout time.Duration
}
```

Replace `runCommandWithContext` (lines 134-159) with:

```go
func runCommandWithContext(
	ctx context.Context,
	policy cancelPolicy,
	run func() error,
	closeConn func(time.Time) error,
) error {
	if err := ctx.Err(); err != nil {
		closeConnectionWithDeadline(closeConn)
		return err
	}
	if ctx.Done() == nil {
		return run()
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- run()
	}()

	select {
	case <-ctx.Done():
		if policy.drainTimeout != 0 {
			return drainCommand(ctx.Err(), policy.drainTimeout, errCh, closeConn)
		}
		closeConnectionWithDeadline(closeConn)
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// drainCommand waits for an already-canceled command to finish on its own:
// the command owns its shutdown (observe ctx, stop intake, drain, return)
// and its error is returned verbatim. A bounded wait force-closes the
// connection at the deadline as the backstop, so a wedged drain cannot hang
// shutdown forever; a negative timeout waits forever.
func drainCommand(
	ctxErr error,
	timeout time.Duration,
	errCh <-chan error,
	closeConn func(time.Time) error,
) error {
	if timeout < 0 {
		return <-errCh
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-errCh:
		return err
	case <-timer.C:
		closeConnectionWithDeadline(closeConn)
		return errors.Join(ErrDrainTimeout, ctxErr)
	}
}
```

Update the existing caller in `withConn` (line ~122) — add the policy argument as abort mode for now (Task 3 threads the real policy):

```go
	err = runCommandWithContext(
		ctx,
		cancelPolicy{},
		func() error {
			return fn(ctx, conn)
		},
		func(deadline time.Time) error {
			return conn.CloseDeadline(deadline)
		},
	)
```

Update the two existing test call sites in `client_test.go` — in `TestRunCommandWithContextDoesNotWaitForStuckCommand` (~line 705) and `TestRunCommandWithContextDoesNotStartAfterCancellation` (~line 747), insert `cancelPolicy{},` as the second argument of `runCommandWithContext`. Do not touch any assertion.

- [ ] **Step 4: Run the full suite**

Run: `go test ./...`
Expected: all PASS (new drain tests + both pre-existing runCommandWithContext tests with unchanged assertions).

- [ ] **Step 5: Commit**

```bash
git add client.go client_test.go client_drain_test.go
git commit -m "MSP-769: generalize runCommandWithContext over cancelPolicy, add drain mode

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: ProcessWithDrain + policy threading

**Files:**
- Modify: `client.go` (`Process` ~line 180, `doProcess` ~line 189, `withConn` ~line 112)
- Test: append to `client_drain_test.go`

**Interfaces:**
- Consumes: `cancelPolicy`, `runCommandWithContext(ctx, policy, run, closeConn)` from Task 2; `Config.DrainTimeout` from Task 1.
- Produces: `func (c *Client) ProcessWithDrain(ctx context.Context, cmd Command) error` — the public API Task 4 documents and protoevent-go will call.

- [ ] **Step 1: Write the failing tests**

Append to `client_drain_test.go` (imports gain `io` and `github.com/quarks-tech/amqpx/connpool`, plus `amqp "github.com/rabbitmq/amqp091-go"`):

```go
// newDrainTestClient builds a Client over a fake pool whose dialer returns a
// bare in-memory connection (mirrors the existing fake-pool tests).
func newDrainTestClient(drainTimeout time.Duration) *Client {
	cfg := &Config{DrainTimeout: drainTimeout}
	cfg.complete()
	return &Client{
		config: cfg,
		connPool: connpool.New(&connpool.Options{
			PoolSize:    1,
			PoolTimeout: time.Second,
			Dialer: func() (*amqp.Connection, *amqp.Channel, error) {
				return &amqp.Connection{}, nil, nil
			},
		}),
	}
}

// End to end: cancel mid-command → command sees its own canceled ctx, drains,
// returns nil → ProcessWithDrain returns nil (no ctx.Err substitution).
func TestProcessWithDrainCleanDrain(t *testing.T) {
	client := newDrainTestClient(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	commandStarted := make(chan struct{})
	go func() {
		<-commandStarted
		cancel()
	}()

	sawCancel := false
	err := client.ProcessWithDrain(ctx, func(cmdCtx context.Context, _ *connpool.Conn) error {
		close(commandStarted)
		<-cmdCtx.Done() // the command receives the SAME ctx and observes shutdown
		sawCancel = true
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessWithDrain() error = %v, want nil (clean drain)", err)
	}
	if !sawCancel {
		t.Fatal("command did not observe the canceled ctx")
	}
}

// Pre-canceled ctx: the command never runs (parity with Process).
func TestProcessWithDrainPreCanceledDoesNotRunCommand(t *testing.T) {
	client := newDrainTestClient(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	commandCalled := false
	err := client.ProcessWithDrain(ctx, func(context.Context, *connpool.Conn) error {
		commandCalled = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ProcessWithDrain() error = %v, want context.Canceled", err)
	}
	if commandCalled {
		t.Fatal("command ran with an already canceled ctx")
	}
}

// The shared retry loop still applies: a retryable failure re-runs the
// command (consumer re-subscribe), and the retried attempt then drains clean.
func TestProcessWithDrainRetriesRetryableThenDrains(t *testing.T) {
	client := newDrainTestClient(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	secondAttemptStarted := make(chan struct{})
	go func() {
		<-secondAttemptStarted
		cancel()
	}()

	err := client.ProcessWithDrain(ctx, func(cmdCtx context.Context, _ *connpool.Conn) error {
		attempts++
		if attempts == 1 {
			return io.EOF // retryable per shouldRetry
		}
		close(secondAttemptStarted)
		<-cmdCtx.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessWithDrain() error = %v, want nil", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one retry then clean drain)", attempts)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestProcessWithDrain -v ./...`
Expected: compile error `client.ProcessWithDrain undefined`.

- [ ] **Step 3: Implement**

In `client.go`, replace `Process` and `doProcess` (lines ~177-203) with:

```go
// Process executes cmd and retries retryable transport and AMQP failures up to
// Config.MaxRetries times. Context cancellation stops acquisition, backoff, and
// waiting for a command attempt.
func (c *Client) Process(ctx context.Context, cmd Command) error {
	return c.process(ctx, cmd, cancelPolicy{})
}

// ProcessWithDrain executes cmd like Process, but for long-lived commands
// (consumers): when ctx cancels mid-command the borrowed connection is NOT
// closed — the command receives the same, now-canceled ctx, owns its shutdown
// (stop intake, drain in-flight work, return), and its error is returned
// verbatim (nil = clean drain). If the command does not return within
// Config.DrainTimeout of the cancellation, the connection is force-closed as
// a backstop and the call returns ErrDrainTimeout joined with the context
// error.
//
// A clean drain should return nil, not ctx.Err(): returning the cancellation
// error makes the release classifier discard a healthy connection. Broker
// operations performed DURING drain (final acks, dead-letter publishes) must
// detach from the canceled ctx locally via context.WithoutCancel.
func (c *Client) ProcessWithDrain(ctx context.Context, cmd Command) error {
	return c.process(ctx, cmd, cancelPolicy{drainTimeout: c.config.DrainTimeout})
}

func (c *Client) process(ctx context.Context, cmd Command, policy cancelPolicy) error {
	for attempt := 0; ; attempt++ {
		retry, err := c.doProcess(ctx, cmd, attempt, policy)
		if err == nil || !retry || attempt >= c.config.MaxRetries {
			return err
		}
	}
}

func (c *Client) doProcess(ctx context.Context, cmd Command, attempt int, policy cancelPolicy) (bool, error) {
	if attempt > 0 {
		if err := sleepWithContext(ctx, c.retryBackoff(attempt)); err != nil {
			return false, err
		}
	}

	err := c.withConn(ctx, cmd, policy)
	if err == nil {
		return false, nil
	}

	retry := shouldRetry(err)
	return retry, err
}
```

Update `withConn` (line ~112) to accept and forward the policy:

```go
func (c *Client) withConn(ctx context.Context, fn Command, policy cancelPolicy) error {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}

	defer func() {
		c.releaseConn(ctx, conn, err)
	}()

	err = runCommandWithContext(
		ctx,
		policy,
		func() error {
			return fn(ctx, conn)
		},
		func(deadline time.Time) error {
			return conn.CloseDeadline(deadline)
		},
	)
	return err
}
```

(If any existing test calls `withConn`/`doProcess` directly, add the `cancelPolicy{}` argument at those call sites — argument-only, no assertion changes.)

- [ ] **Step 4: Run the full suite**

Run: `go test ./...`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add client.go client_drain_test.go
git commit -m "MSP-769: add Client.ProcessWithDrain sharing the Process retry loop

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Documentation

**Files:**
- Modify: `doc.go` (cancellation paragraph, lines ~12-16)
- Modify: `README.md` ("Context and command contract" section, lines ~145-172)
- Modify: `client.go` (`Command` doc comment, lines 17-19 — method docs were written in Task 3)

**Interfaces:** none (prose only). Do not change any code in this task.

- [ ] **Step 1: Update doc.go**

Read the existing cancellation paragraph (lines ~12-16, beginning "Context cancellation removes and starts closing the borrowed connection…"). Keep it, scoped to Process, and append this paragraph directly after it:

```
ProcessWithDrain inverts the mid-command contract for long-lived commands
(consumers): cancellation still stops acquisition and backoff, but a running
command keeps its connection and owns its shutdown — it must observe ctx,
stop intake, drain in-flight work, and return (nil on a clean drain). If it
does not return within Config.DrainTimeout, the connection is force-closed
and ErrDrainTimeout is returned joined with the context error.
```

- [ ] **Step 2: Update README "Context and command contract"**

In the section at lines ~145-172, after the existing paragraph describing cancel-while-running behavior (lines ~161-163), add:

```markdown
For long-lived commands (consumers), use `ProcessWithDrain`: cancellation
does not close the borrowed connection — the command receives the canceled
context, stops intake, drains in-flight work, and returns (`nil` on a clean
drain; the connection then returns to the pool). `Config.DrainTimeout`
(default 30s, negative = wait forever) bounds the drain; at the deadline the
connection is force-closed and the call returns `ErrDrainTimeout` joined
with the context error. Two rules for drain commands: return `nil` (not
`ctx.Err()`) after a clean drain, and detach in-drain broker operations
(final acks, dead-letter publishes) with `context.WithoutCancel`.
```

- [ ] **Step 3: Update the Command doc comment**

Replace `client.go` lines 17-19:

```go
// Command performs one AMQP operation attempt on an exclusively leased
// connection and channel. A Command must not retain conn after returning.
// Under Process it should stop promptly when ctx is canceled; under
// ProcessWithDrain it owns its shutdown instead — observe ctx, stop intake,
// drain, and return within Config.DrainTimeout.
type Command func(ctx context.Context, conn *connpool.Conn) error
```

- [ ] **Step 4: Verify build and docs render**

Run: `go build ./... && go vet ./... && go doc . Client.ProcessWithDrain`
Expected: build/vet clean; doc output shows the ProcessWithDrain comment.

- [ ] **Step 5: Commit**

```bash
git add doc.go README.md client.go
git commit -m "MSP-769: document the two-mode cancellation contract

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Gate, push, release v0.3.2

**Files:** none new.

- [ ] **Step 1: Full gate**

Run: `gofmt -l . && go build ./... && go vet ./... && go test -count=1 ./...`
Expected: gofmt lists nothing; build/vet clean; all tests PASS. If `golangci-lint` is installed, also run `golangci-lint run ./...` — expect 0 issues.

- [ ] **Step 2: Push the branch**

```bash
git push -u origin MSP-769
```

- [ ] **Step 3: Release (operator gate — requires merge to main first)**

Open/merge the PR into `main` per team flow, then:

```bash
git checkout main && git pull
git tag v0.3.2
git push origin v0.3.2
```

Note: the tag must point at the merged main commit, not the branch. protoevent-go's migration PR (separate plan, per spec §Release & migration) bumps `require github.com/quarks-tech/amqpx v0.3.2` after this exists.
