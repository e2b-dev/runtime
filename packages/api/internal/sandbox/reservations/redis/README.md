# Redis Reservation Storage

This package coordinates sandbox creation reservations across API instances.

## Keys

- Storage index: `sandbox:storage:{teamID}:index`
- Pending zset: `sandbox:storage:{teamID}:reservations:pending`
- Result key: `sandbox:storage:{teamID}:reservations:{sandboxID}:result`
- PubSub routing key: `sandbox:storage:{teamID}:reservations:{sandboxID}:notify`

## Flow

`Reserve` runs a Lua script that atomically removes stale pending entries, checks whether the sandbox already exists or has already pending start, enforces the team limit using `SCARD(storage index) + ZCARD(pending zset)`, deletes any stale result key, and adds the sandbox ID to the pending zset.

When creation completes, `FinishStart` removes the sandbox from the pending zset, writes a TTL result key containing the opaque result the caller passes, and publishes the routing key.

A waiter (`WaitForStart`) subscribes to the routing key, probes the result key immediately, then waits for PubSub notifications or the 1 second fallback ticker. PubSub is best-effort; the fallback ticker is required for correctness.

`Release` is called when the sandbox is removed from storage (`Store.Remove`). It removes the sandbox from the pending zset, deletes the result key, and publishes the routing key.
