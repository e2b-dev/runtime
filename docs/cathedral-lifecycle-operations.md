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
operation, and key in Postgres before dispatch. Reusing a key with a different
binding returns `409`. Replaying the same key returns the stored operation and
never dispatches again. Recover it with
`GET /v1/cathedral/lifecycle-operations/{idempotencyKey}`.

Before the first dispatch, an authenticated consumer reads the current
incarnation from `GET /v1/cathedral/sandboxes/{sandboxID}/identity`. That
endpoint enforces team ownership and returns the execution ID that must be
pinned into the lifecycle request. Recovery by operation key happens first, so
a completed delete remains readable after the live sandbox identity is gone.

`completed` is written only after an execution-bound node RPC confirms that
the execution stopped. For pause, the snapshot build must also have reached a
durable successful state and its build ID is recorded. The Cathedral pause RPC
waits for remote snapshot storage to complete before returning that evidence;
the ordinary runtime pause path remains asynchronous. `404` from ordinary
sandbox GET/list, a registry row disappearing, a legacy delete acknowledgement,
or joining an in-flight removal is never terminal evidence.

Delete reports snapshot/storage cleanup separately through `cleanup_state`.
`completed` with `cleanup_state=failed` means compute removal is proven but
storage cleanup debt remains; a consumer must retain that debt and must not
represent full cleanup or final settlement as complete.

`unknown` is durable and non-retryable by POST. Recover it by key and reconcile
with operator/provider evidence; do not blindly replay the lifecycle action.

Pause persists `remaining_lifetime_ms`. The snapshot stores the same frozen
remaining lifetime, and a resume without an explicit timeout uses it rather
than granting a new default lifetime. Resume remains the existing authenticated
endpoint; the operation protocol prevents stale pre-resume delete/pause work
from acting on the new execution identity.

## Consumer rules

1. Generate one operation key per user intent and persist it before calling.
2. Send the current provider `execution_id`; never identify an incarnation by
   sandbox ID alone.
3. Treat `reserved`, `dispatching`, and `unknown` as non-terminal.
4. Treat delete as compute-stopped only when `state=completed` and
   `execution_removed_at` is present. Close storage/billing only under the
   consumer's separately defined settlement rules and cleanup state.
5. Treat pause as complete only when `state=completed`,
   `execution_removed_at`, `snapshot_build_id`, and `snapshot_completed_at`
   are all present.
6. After disconnection, GET the operation by key. Never infer success from the
   sandbox listing and never submit a new key merely because the first response
   was lost.
