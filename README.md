# amqpx

`amqpx` is a small pooling and retry layer for
[`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go).
Each pooled item owns one AMQP connection and one channel. Callers borrow that
pair through `Client.Process` for the duration of a command.

The current module requires Go 1.26.4 or newer and uses `amqp091-go` v1.13.0.

> [!IMPORTANT]
> `amqp091-go` v1.13.0 includes experimental automatic connection, channel,
> and topology recovery. `amqpx` does not support that feature: `NewClient`
> panics when `Config.AMQP.Recovery` is non-nil. Leave it nil and let `amqpx`
> discard failed connections and redial through its pool.

## Install

```sh
go get github.com/quarks-tech/amqpx@latest
```

## Quick start

`Config.Address` accepts either an AMQP URI without a scheme or a complete
`amqp://`/`amqps://` URI. `amqpx` adds `amqp://` when the scheme is omitted.

```go
package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/quarks-tech/amqpx"
	"github.com/quarks-tech/amqpx/connpool"
)

func main() {
	client := amqpx.NewClient(&amqpx.Config{
		Address: "guest:guest@localhost:5672/",
	})
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close AMQP client: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Process(ctx, func(ctx context.Context, conn *connpool.Conn) error {
		return conn.Channel().PublishWithContext(
			ctx,
			"",       // exchange
			"events", // routing key
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("hello"),
			},
		)
	})
	if err != nil {
		log.Printf("publish: %v", err)
	}
}
```

`Process` may execute a command more than once. Publishing and most other AMQP
operations are not inherently idempotent, so application-level message IDs,
deduplication, publisher confirms, and acknowledgement policy remain the
caller's responsibility.

## Configuration defaults

`NewClient` accepts a nil config. Otherwise it snapshots the supplied `Config`
and completes defaults on its private copy. The snapshot clones `AMQP.SASL`,
`AMQP.Properties`, and `AMQP.TLSClientConfig`. It also copies built-in
`*amqp.PlainAuth` and `*amqp.AMQPlainAuth` values so amqp091-go v1.13 can erase
its per-connection password after authentication without mutating caller or
future-dial credentials. Custom authentication implementations, nested property
values, functions, and a custom `Limiter` remain shared and must be safe for
concurrent use.

| Field | Zero-value behavior | Special values and notes |
| --- | --- | --- |
| `Address` | Upstream URI defaults: `guest`/`guest` at `localhost:5672`, vhost `/` | URI authority/path, or a complete `amqp://`/`amqps://` URI; an omitted scheme defaults to plaintext `amqp://` |
| `AMQP` | The zero `amqp.Config`; an internal context-aware dialer is used when `AMQP.Dial` is nil | `Recovery` must remain nil |
| `MaxRetries` | 3 retries after the initial attempt | Any negative value disables retries; positive values are used as written |
| `MinRetryBackoff` | 8 ms | Any negative value disables retry delay |
| `MaxRetryBackoff` | 512 ms | Any negative value disables retry delay |
| `DialTimeout` | 5 s | Bounds connection and initial-channel setup; it is separate from the `Process` context |
| `PoolFIFO` | `false` (LIFO) | `true` selects FIFO, which requires shifting the idle queue |
| `PoolSize` | `runtime.GOMAXPROCS(0)` | Maximum number of simultaneous pool leases |
| `MinIdleConns` | 0 | No eager idle connections |
| `MaxConnAge` | 0 | Connection-age retirement disabled |
| `PoolTimeout` | 1 s | Maximum wait for a pool lease when all leases are busy |
| `IdleTimeout` | 0 | Idle-time retirement disabled; negative values also disable it |
| `IdleCheckFrequency` | 1 min | The background reaper runs only when both this and `IdleTimeout` are positive; negative values disable it |
| `Limiter` | nil | No rate limiter or circuit breaker |

`NewClient` panics when `PoolSize`, `MinIdleConns`, `PoolTimeout`, or
`DialTimeout` is negative, or when `MinIdleConns` exceeds `PoolSize`. It also
panics when both retry backoffs are positive and `MaxRetryBackoff` is less than
`MinRetryBackoff`, or when `AMQP.Recovery` is non-nil.

Retry delays use bounded exponential jitter between the configured minimum and
maximum. Setting either retry backoff field to a negative value results in
immediate retries.

## Retry and error behavior

`MaxRetries` counts retries, not total attempts. With the default value of 3,
`Process` can run the command up to four times. Before every retry it waits for
the configured jittered backoff, unless the delay is disabled.

The current retry classifier retries:

- `io.EOF` and `io.ErrUnexpectedEOF`, including wrapped values;
- errors implementing both `error` and `Timeout() bool`, including wrapped
  values; with the current classifier, these are retried whether `Timeout`
  returns true or false;
- `*amqp.Error` values, including wrapped values, with code
  `amqp.ConnectionForced`, `amqp.ChannelError`, or `amqp.InternalError`.

It does not retry `context.Canceled`, `context.DeadlineExceeded`, or errors that
do not match one of the cases above. All of these checks inspect wrapped error
chains.

An error closes the current pooled connection/channel pair with a deadline and
then removes it when its chain contains cancellation, a deadline, EOF, a
timeout error, or any `*amqp.Error`. The client waits at most 100 ms for that
close attempt. Other application errors leave the pair in the pool. A nil
result also returns it to the idle pool.

When `Limiter` is set, each attempt calls `Allow` before acquiring a connection.
An allowed attempt is followed by exactly one `ReportResult`, including when
pool acquisition fails. The limiter implementation must be safe for the same
concurrency as the client.

## Context and command contract

The `Process` context controls waiting for a pool lease, retry backoff, waiting
for connection establishment, and waiting for a command attempt. The same
context is passed to the `Command`. `DialTimeout` separately bounds waiting for
connection and initial-channel setup.

The default path uses `net.Dialer.DialContext`. Cancellation closes the raw
transport so it cannot be undone when amqp091-go clears its handshake deadline
before opening the initial channel. A custom `AMQP.Dial` callback cannot receive
a context, so `amqpx` runs that callback asynchronously. The `Process` context
and `DialTimeout` bound the caller's wait, but a callback that never returns
cannot have its goroutine forcibly stopped. If it eventually returns a
connection after cancellation or timeout, that connection is closed instead of
entering the pool.

When a context expires while a command is running, `amqpx` starts closing the
borrowed connection, waits at most 100 ms for that close attempt, and returns
the context error without waiting indefinitely for the command goroutine. A
command must therefore:

- pass the supplied context to context-aware AMQP methods;
- stop its own blocking work when that context is done or the connection is
  closed;
- never start work that continues to use the borrowed connection after the
  command returns.

For long-lived commands (consumers), use `ProcessWithDrain`: cancellation
does not close the borrowed connection — the command receives the canceled
context, stops intake, drains in-flight work, and returns (`nil` on a clean
drain; the connection then returns to the pool). `Config.DrainTimeout`
(default 30s, negative = wait forever) bounds the drain; at the deadline the
connection is force-closed and the call returns `ErrDrainTimeout` joined
with the context error. Two rules for drain commands: return `nil` (not
`ctx.Err()`) after a clean drain, and detach in-drain broker operations
(final acks, dead-letter publishes) with `context.WithoutCancel`.

Cancellation bounds how long `Process` waits, but it cannot forcibly stop Go
code. A command that ignores these rules may continue running after `Process`
returns; its connection has already been removed from the pool and must not be
used.

## Connection ownership and concurrency

The `*connpool.Conn` and the `*amqp.Channel` returned by `Conn.Channel` are
borrowed values. They are valid only during the `Command` invocation.

- Do not retain either pointer after the command returns.
- Do not call `Close`, `Put`, or `Remove` on the borrowed connection; return an
  error and let `Client` release it.
- Do not launch goroutines that outlive the command or share its channel with
  another command.
- Follow the upstream `amqp091-go` rules for operations on the raw channel.

A client is designed for concurrent `Process` calls. The pool gives each active
command an exclusive connection/channel pair; separate commands can run in
parallel up to the configured pool limit. Custom dialers, authentication
implementations, and limiters must be concurrency-safe.

The `connpool` subpackage is public for advanced use, but most applications
should use `Client.Process` so every successful `Get` is matched with exactly
one `Put` or `Remove`.

## Shutdown

Call `Client.Close` only after application code has stopped starting commands
and all outstanding `Process` calls have returned. `Close` closes all tracked
AMQP connections, including ones currently borrowed, so concurrent shutdown is
not graceful and can interrupt a command.

After shutdown, new pool acquisitions return `connpool.ErrClosed`. Closing the
pool wakes callers waiting for a lease and cancels context-aware connection
dials; those calls also return `connpool.ErrClosed`. A second `Close` returns
`connpool.ErrClosed`. `Close` joins errors returned by close callbacks and AMQP
connection closes, so callers can inspect the result with `errors.Is` or
`errors.As`.

## Automatic recovery must remain disabled

The experimental `amqp091-go` v1.13 automatic connection, channel, and topology
recovery feature must remain disabled. `NewClient` panics when
`Config.AMQP.Recovery` is non-nil:

```go
cfg := amqpx.Config{
	AMQP: amqp.Config{
		Recovery: nil,
	},
}
```

`amqpx` owns connection/channel replacement through pool removal, retry, and
redial. It has not integrated or tested the upstream recovery state machine,
which attempts to reconnect and reuse the same objects. Combining both
lifecycle managers can make pool state and object ownership ambiguous.

## Development

The unit tests and benchmarks are hermetic: they do not require a running
RabbitMQ server.

```sh
go test ./...
go test -race ./...
go test ./... -run '^$' -bench . -benchmem
```

The pool benchmarks compare LIFO and FIFO checkout at several idle depths and
under parallel load. Treat absolute timings as machine-specific; compare
results from the same host and Go toolchain when investigating regressions.
