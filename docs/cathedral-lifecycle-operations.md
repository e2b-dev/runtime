# Cathedral lifecycle operations

This contract is local implementation evidence only. It does not qualify a
host, image, deployment, customer billing path, website, or CLI.

## Contract

`POST /v1/cathedral/sandboxes/{sandboxID}/lifecycle-operations` accepts an
authenticated `delete` or `pause` request with:

- `Idempotency-Key`: durable operation identity;
- `execution_id`: the exact sandbox incarnation the caller observed;
- optional `filesystem_only` for pause.

The server hashes the normalized request and binds team, sandbox, execution,
operation, pause mode, and key in Postgres before dispatch. Reusing a key with
a different binding returns `409`. Dispatch uses a fenced attempt number and a
bounded lease. A replay or recovery request may dispatch a never-started
`reserved` operation. It may retry an expired attempt only after the live
registry proves the same pinned execution is still `running`, which proves the
previous attempt did not commit a removal transition. A missing, superseded,
or still-transitioning execution becomes `unknown` instead of being blindly
acted on again. Recover with
`GET /v1/cathedral/lifecycle-operations/{idempotencyKey}`.

Node refusals that restore the same execution are returned to `reserved` and
remain retryable. A known execution mismatch is terminal `failed`. Other
unconfirmed provider outcomes remain `unknown`. Terminal writes run on a
detached bounded context, are generation-fenced, and are retried; if durability
still cannot be established, the request returns an error and recovery applies
the lease rules above.

Before the first dispatch, an authenticated consumer reads the current
incarnation from `GET /v1/cathedral/sandboxes/{sandboxID}/identity`. That
endpoint enforces team ownership and returns the execution ID that must be
pinned into the lifecycle request. Recovery by operation key happens first, so
a completed delete remains readable after the live sandbox identity is gone.

`completed` is written only after an execution-bound node RPC confirms that
the execution stopped. For pause, the snapshot build must also have reached a
durable successful state and its build ID is recorded. The Cathedral pause RPC
waits for both remote snapshot storage and Firecracker teardown before returning
that evidence; the ordinary runtime pause path remains asynchronous. `404` from ordinary
sandbox GET/list, a registry row disappearing, a legacy delete acknowledgement,
or joining an in-flight removal is never terminal evidence.

Delete reports snapshot/storage cleanup separately through `cleanup_state`.
`completed` with `cleanup_state=failed` means compute removal is proven but
storage cleanup debt remains. Replaying or recovering that exact operation key
retries only the idempotent snapshot cleanup and never redispatches compute.
A consumer must retain remaining debt and must not represent full cleanup or
final settlement as complete.

`unknown` is durable and non-retryable by POST. Recover it by key and reconcile
with operator/provider evidence; do not blindly replay the lifecycle action.

Pause persists `remaining_lifetime_ms` from the same transition-owned remaining
lifetime used to write the snapshot, with both values rounded up to seconds.
Presence is explicit: zero means exhausted, while an absent field identifies a
legacy snapshot. Resume without an explicit timeout uses the frozen value;
connect and traffic auto-resume are capped by it and refuse exhausted snapshots.
An explicit resume timeout remains the only override. The operation protocol
prevents stale pre-resume delete/pause work from acting on the new execution
identity.

## Consumer rules

1. Generate one operation key per user intent and persist it before calling.
2. Send the current provider `execution_id`; never identify an incarnation by
   sandbox ID alone.
3. Treat `reserved` as retryable, `dispatching` as leased in-flight work, and
   `unknown` as non-terminal for business settlement but not safe to replay.
4. Treat delete as compute-stopped only when `state=completed` and
   `execution_removed_at` is present. Close storage/billing only under the
   consumer's separately defined settlement rules and cleanup state.
5. Treat pause as complete only when `state=completed`,
   `execution_removed_at`, `snapshot_build_id`, and `snapshot_completed_at`
   are all present.
6. After disconnection, GET the operation by key. Never infer success from the
   sandbox listing and never submit a new key merely because the first response
   was lost.
