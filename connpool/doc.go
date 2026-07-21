// Package connpool manages pools of AMQP connection/channel pairs.
//
// Most applications should use the parent amqpx package instead of managing
// leases directly. Direct callers must match every successful Get with exactly
// one Put or Remove and must not use a Conn after releasing it. A borrowed Conn
// and the channel returned by Conn.Channel must not be shared with work that
// outlives the lease. Options must provide DialerContext or the legacy Dialer;
// DialerContext takes precedence and allows Get and Close to cancel dialing.
//
// Closing a pool wakes callers waiting in Get, cancels context-aware dials, and
// closes tracked connections, including borrowed connections. Callers should
// stop new work and release outstanding leases before Close. The package does
// not integrate with amqp091-go's experimental automatic Recovery feature.
// Remove and the other explicit closing operations wait for the close callback
// and connection close they initiate before returning. Get bounds its wait for
// stale-connection cleanup by its context and an internal close deadline; a
// low-level close that ignores that deadline may finish asynchronously.
package connpool
