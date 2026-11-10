# scheduling-api

gRPC service for scheduling. It serves `e2b.scheduling.v1.SchedulingService`,
which admits sandboxes against the live limit of their project and stores the
state of live sandboxes in PostgreSQL.

See [request flows](docs/request-flows.md) for the implemented start, pause and
kill paths, the proposed methods that are not implemented, and the later
scheduling-owned flow.

The contract is `e2b/scheduling/v1/scheduling.proto` in this directory. The
generated Go code is in `packages/shared/pkg/grpc/scheduling`. After a change
to the proto or to a SQL query, run `make generate`. Generation needs network
access: it fetches the protovalidate rules with `buf export`.

## Methods

`ListLive` provides bounded pages filtered by project, states, and expired deadlines.
An empty project filter is an internal all-project read.
`UpdateDeadline` compares the execution and previous deadline while the record is running.
`InspectReservation` exposes the current reservation identity and result for joiners.
`ClaimSnapshotDeletion` holds an absent or gone sandbox ID for snapshot deletion for 60 seconds.
It excludes `Reserve` for the same project under the same transaction lock.
Overlapping claim IDs coexist. Each claim expires independently; retrying the same claim ID renews only that claim.
`ReleaseSnapshotDeletionClaim` checks the project, sandbox and claim IDs. Admission stays blocked until every active claim for that project and sandbox is released or expires.
The tombstone cleanup round also removes up to one batch of expired claims per purge call.
These claims do not consume admission slots.

The caller authenticates the user and sends the project ID. Reaching this
service is the access-control boundary. A request never reads or changes a
sandbox of another project, except `ListStaleTransitions` and `ListLive` with
no project filter: internal reads of all projects in this database, for trusted
callers on the same internal network. They are not customer endpoints. `ClaimTransition` changes
only the sandbox of the project it names.

Requests use current-format sandbox IDs (`i` followed by 20 lowercase base36
characters) and UUID project, reservation and transition IDs. The service
stores a sandbox under an internal actor UUID (see
[Sandbox UUIDs](#sandbox-uuids)) and returns the sandbox ID.

A start is `Reserve`, then `Put` when the node has created the sandbox, or
`FinishStart` when it has not.

| Method | Effect | Outcomes |
|---|---|---|
| `Reserve` | Puts the sandbox in the starting state with the requested `end_time`, and adds one to the live count of the project. One transaction. | `RESERVED` and `ALREADY_PENDING` (with the start deadline), `ALREADY_RUNNING` (with the sandbox), `LIMIT_EXCEEDED`, `FINISHED` (with the failure, if `FinishStart` or the start timeout recorded one) |
| `Put` | Stores the sandbox record and makes the sandbox running. One transaction. An optional `end_time` sets the final API deadline on the first write. The supplied or reserved deadline is capped at `start_time + max_instance_length`. Retries keep the stored deadline. | `STORED`, `NOT_FOUND` |
| `FinishStart` | Ends a start that failed before `Put`: the sandbox is gone, the failure is kept, and the live count decreases by one. One transaction. | `FINISHED`, `NOT_FOUND`, `ALREADY_STORED` |
| `Get` | Reads a running sandbox, or one in a pause, kill or snapshot transition. A starting or gone sandbox is not returned. | the sandbox, or no sandbox |

A pause or a kill is `BeginTransition`, then `FinishTransition` when the
sandbox has ended on the node, or `RestoreRunning` when the node refused a
pause and the sandbox still runs. The service stores state only. The caller
runs the node operations. See [Pause and kill](#pause-and-kill).

| Method | Effect | Outcomes |
|---|---|---|
| `BeginTransition` | Moves a running sandbox to pausing or killing, and records the transition ID and action. The state change is atomic. | `STARTED` (with the sandbox), `JOINED` or `CONFLICT` (with the active transition ID and action), `NOT_FOUND`, `NOT_RUNNING`, `EXECUTION_MISMATCH`, `NOT_EXPIRED`, `ALREADY_FINISHED`, `ALREADY_RESTORED`, `ACTION_MISMATCH` |
| `FinishTransition` | Deletes the sandbox record, makes the sandbox gone with the failure, if any, and decreases the live count by one. One transaction. | `FINISHED`, `NOT_FOUND`, `RESTORED` |
| `RestoreRunning` | Restores a refused pause or kill. Sets the retry window and refusal episode. Keeps the live count. | `RESTORED` (with the retry window), `NOT_FOUND`, `NOT_RESTORABLE`, `ALREADY_FINISHED` |
| `WaitForTransition` | Waits at most `max_wait` for the transition to end. The calls for one transition share the reads on each replica. Writes nothing. | `FINISHED` (with the failure, if any), `RESTORED`, `WAITING`, `NOT_FOUND`, `UNKNOWN` |

A pause or kill whose owner does not end it becomes stale after 95 s. The
recovery methods let a trusted caller find and take it over. See
[Stale transitions](#stale-transitions).

| Method | Effect | Outcomes |
|---|---|---|
| `ListStaleTransitions` | Lists pauses and kills of all projects that began or were claimed at least 95 s ago, oldest first, with their node and execution. Pages of up to 1000 (default 100). Writes nothing. | the transitions, and `next_page_token` when the page is full |
| `ClaimTransition` | Gives a stale pause or kill a new transition ID, if the sandbox still has the expected execution. One statement. The state, action, record and live count do not change. | `CLAIMED` (with the transition and the sandbox), `NOT_FOUND`, `CONFLICT` (with the active transition), `NOT_STALE`, `EXECUTION_MISMATCH`, `ALREADY_FINISHED`, `ALREADY_RESTORED` |

Expected results are outcomes in the response:

- A repeated call with the same reservation ID changes nothing more. `Reserve`
  counts a sandbox once, `Put` keeps the record of the first call, and
  `FinishStart` decreases the count once.
- `Reserve` with the reservation ID of a start that has ended returns
  `FINISHED` and changes nothing. A new start of the same sandbox needs a new
  reservation ID.
- `Put` and `FinishStart` change a sandbox only when the project, sandbox ID
  and reservation ID all match a start in progress. A late call for an old
  reservation returns `NOT_FOUND`.
- `FinishStart` after `Put` returns `ALREADY_STORED`. This method does not stop
  a running sandbox. A pause or a kill does.
- `Put` with `NOT_FOUND` stored nothing. The caller must remove the sandbox it
  created on the node.

These results for an ended start or transition hold only while the service
still records it: a new start or transition of the sandbox replaces that
history, and the gone row can be deleted later. See [Tombstones](#tombstones).

### Start deadline

`Reserve` returns `start_deadline` with `RESERVED` and `ALREADY_PENDING`. It
is 90 s after `reserved_at` of the pending start, by the database clock. A
repeated `Reserve` with the same reservation ID returns the same deadline; it
does not extend it. With `ALREADY_PENDING` it is the deadline of the other,
pending reservation. The 90 s value is fixed in the service.

From the deadline on, the service can end the start in one transaction: the
sandbox is gone with the failure `start timed out`, code 4
(`DEADLINE_EXCEEDED`), and the counter slot of the start decreases by one.
The update applies only while the project, sandbox ID, reservation ID, the
starting state and the age all match, so it does not change a start that
`Put`, `FinishStart` or a new reservation changed first. After it:

- `Put` returns `NOT_FOUND`. The service changes only the database and does
  not stop a sandbox on a node. The caller must remove the sandbox it
  created.
- `FinishStart` returns `FINISHED` and keeps the timeout failure.
- `Reserve` with the same reservation ID returns `FINISHED` with the timeout
  failure.

A `Put` or a `FinishStart` after the deadline still succeeds while the start
has not been ended. Finish every start before its deadline.

The [background cleanup](#background-cleanup) ends expired starts, so a start
can end any time after its deadline, usually within about 20 s, later with
a backlog of expired starts. Ending a start does not stop a sandbox on a node. A sandbox that a
node created for an expired start runs there without a record until the
caller removes it; the service does not find or remove such sandboxes.

A gRPC error means one of these:

| Code | Cause |
|---|---|
| `INVALID_ARGUMENT` | Field validation reports all violations with `buf.validate.Violations` details. The record must also be a JSON object that PostgreSQL jsonb can store. `max_instance_length` and `retry_after` must also be whole numbers of microseconds, and `max_wait` at most 5 s. These checks return a fixed message without details. |
| `ALREADY_EXISTS` | `Reserve` for a sandbox ID that another project holds, including a gone sandbox whose row is not purged yet. |
| `DEADLINE_EXCEEDED`, `CANCELLED` | `WaitForTransition` whose deadline passed or whose caller cancelled before the transition ended. Call it again. |
| `INTERNAL` | A database or server failure. The message is always `internal error`; the service logs the cause. |

A transport error or `INTERNAL` does not tell the caller whether the
transaction committed. Retry with the same reservation or transition ID, or
reconcile.

### Pause and kill

The caller generates a random transition ID for each pause or kill that it
runs. The transition ID names the owner of the transition:

- `STARTED`: the caller owns the transition. It runs the node operation with
  the node and execution of the returned sandbox, then calls
  `FinishTransition` or `RestoreRunning` with the same transition ID. A repeat
  `BeginTransition` with that ID, for example after a lost response, returns
  `STARTED` again and changes nothing. Two callers must never share a
  transition ID.
- `JOINED`: another transition with the same action is active. The response
  carries its transition ID. The caller does not call the node. It waits with
  `WaitForTransition` and that transition ID.
- `CONFLICT`: a transition with the other action is active, for example a kill
  during a pause. The service does not replace or cancel it. The caller waits
  for the returned transition ID, then calls `BeginTransition` again.
- `ALREADY_FINISHED`, `ALREADY_RESTORED`, `ACTION_MISMATCH`: the sandbox
  already records this transition ID, as ended or with the other action.
  Nothing changed. A new attempt needs a new transition ID.
- `EXECUTION_MISMATCH`: `expected_execution_id` is set and the sandbox has
  another execution. A caller that acts on an earlier read, such as a scan for
  expired sandboxes, sets it, so that it does not stop a newer execution that
  reuses the sandbox ID.
- `NOT_EXPIRED`: `require_expired` is set, and `end_time` has not passed or the
  retry window of `RestoreRunning` is open. A pause or a kill without
  `require_expired` ignores the retry window.

The service detects a reused transition ID only while the sandbox records it
as its latest transition. A finished or restored transition keeps its ID on
the sandbox until the next `Reserve` or `BeginTransition` replaces it, or
until the gone row is purged.

`FinishTransition` always ends the sandbox: it deletes the record, makes the
sandbox gone and decreases the live count. Call it only when the sandbox has
stopped on the node, or the caller will remove it there. A node error or a
timeout does not prove that. `failure` reports an action that failed while
the sandbox ended all the same, for example a pause whose snapshot failed; the
sandbox is gone in both cases. A repeat call returns `FINISHED` and keeps the
first failure. A call with the ID of a restored pause returns `RESTORED` and
changes nothing. A call with an ID that the sandbox does not record returns
`NOT_FOUND`, also after a new start of the same sandbox.

`RestoreRunning` is for a pause that the node refused while the sandbox kept
running. It also restores a kill refused for a checkpoint. The sandbox is running
again with the same record, count and `end_time`. `refused_until` becomes now
plus `retry_after`. `refused_since` keeps the first refusal while each refusal
comes within 30 s after the previous window ended, and otherwise becomes now.
A repeat call returns `RESTORED` with the stored window and changes nothing.
`ALREADY_FINISHED` means that the sandbox has no record any more; if it still
runs on the node, the caller must remove it there.

There is no streaming or notification. On each replica, the
`WaitForTransition` calls for one transition (project, sandbox and transition
ID) share one watch. The watch reads the primary at once, and then again after
a delay that grows with the age of the transition. The database clock gives
the age.

| Transition age | Delay to the next read |
|---|---|
| Less than 100 ms | 25 ms |
| 100 ms to less than 500 ms | 50 ms |
| 500 ms to less than 1 s | 100 ms |
| 1 s to less than 2 s | 200 ms |
| 2 s or more | 500 ms |

A short transition is seen soon after it ends. A long one is seen up to
500 ms plus the read time after it ends. A call that joins a watch does not
cause a read. A call after `WAITING` does not restart the fast reads, because
the age comes from the database. The watch stops when its last call returns.
It keeps no result: a call after the watch has stopped starts a new watch,
which reads again. The replicas do not coordinate; each replica that has
callers reads on its own. For example, 100 callers that wait for one
transition older than 2 s, spread over 3 replicas, cause about 6 reads per
second. With one read every 200 ms for each caller, they would cause about
500. These numbers come from the schedule, not from a measurement.

`max_wait` is the budget of one call, at most 5 s. Each call has its own
budget and deadline. A call that returns or is cancelled does not change the
other calls. When `max_wait` passes, the call returns `WAITING` from the last
read of the watch, which can lag by a polling delay plus the database read
time; the call does not read again.
A call whose budget passes before the first read of its watch waits for that
read, so it gets the real outcome. One read has a 5 s limit. A failed read
fails all calls that wait for it, and the next call starts a new watch. The
watch holds no connection or transaction between reads, so a waiter can call
any replica and resume there after a reconnect. Set the caller deadline
longer than `max_wait` to normally receive `WAITING`; an earlier deadline
returns `DEADLINE_EXCEEDED`. The request also respects the 10 s server
default. Only `WaitForTransition` uses the watch; there is no wait method for
a start yet.

| Outcome | Meaning |
|---|---|
| `FINISHED` | `FinishTransition` ended this transition; the sandbox is gone. Carries the failure, if any. |
| `RESTORED` | `RestoreRunning` ended this pause; the sandbox is running. |
| `WAITING` | This transition was active at the last read before `max_wait` passed. Call again. |
| `NOT_FOUND` | The project has no sandbox with this ID. |
| `UNKNOWN` | The sandbox does not record this transition ID: a new start or transition replaced it, or it never began. The service did not keep its outcome. Read the sandbox with `Get`. |

### Stale transitions

A transition is stale when it began, or was last claimed, at least 95 s ago
by the database clock. The 95 s are fixed in the service and are not a
request parameter. A stale transition is only a candidate for recovery: its
owner may have failed, or its node call may still run. The service does not
call nodes and does not end stale transitions on its own; no caller uses
these methods yet.

`ListStaleTransitions` returns, for each stale pause or kill: project ID,
sandbox ID, transition ID, action, `transition_at`, execution ID, node ID and
cluster ID. It reads all projects. Pages are ordered by `transition_at`, then
by sandbox; `next_page_token` continues after the last transition of a full
page. A token is opaque and starts with a format version. The list is not a
snapshot: transitions that end, are claimed or begin between pages can be
missed or listed again, and a listed transition may no longer be stale when
the caller acts. A transition whose sandbox has no record is not listed. The
query scans and sorts the table, because an index on the state would make
state changes non-HOT updates.

`ClaimTransition` takes `old_transition_id` from the list, a new random
`new_transition_id`, and the listed `expected_execution_id`, which is
required. If the sandbox of the project is pausing or killing with
`old_transition_id`, that transition is at least 95 s old, and the sandbox
has `expected_execution_id`, the transition gets `new_transition_id` and
`transition_at` becomes now, so another claim must wait 95 s again. Nothing
else changes. The response carries the action and the sandbox, read in the
same transaction, so they belong to the claimed execution.

| Outcome | Meaning |
|---|---|
| `CLAIMED` | The sandbox is in the transition of `new_transition_id` with `expected_execution_id`. A repeat with the same IDs returns `CLAIMED` with the first `transition_at`. |
| `NOT_FOUND` | No such sandbox in the project, or it is starting, snapshotting, or running or gone after another transition. |
| `CONFLICT` | The sandbox is in a pause or kill with another transition ID, for example after another claim. Carries that transition. Of concurrent claims with different new IDs, one gets `CLAIMED` and the others `CONFLICT`. |
| `NOT_STALE` | The transition of `old_transition_id` is younger than 95 s. |
| `EXECUTION_MISMATCH` | The sandbox in the pause or kill has another execution. |
| `ALREADY_FINISHED` | The sandbox is gone and records one of the IDs. Only while the gone row remains. |
| `ALREADY_RESTORED` | The sandbox is running and records one of the IDs. |

A claim prevents `FinishTransition` and `RestoreRunning` with the old
transition ID from changing the claimed attempt: they return `NOT_FOUND`.
`WaitForTransition` with the old ID returns `UNKNOWN`. The new owner ends the
transition with `FinishTransition` or `RestoreRunning`, as after
`BeginTransition`.

The service keeps only the latest transition ID of a sandbox, so a claim
does not permanently reject `BeginTransition` with the old ID. While the
replacement is active, an old `BeginTransition` returns `JOINED` or
`CONFLICT`. After the replacement restores the sandbox to running, the old
`BeginTransition` can start another operation (`STARTED`). Owners must stop
retrying `BeginTransition` before 95 s have passed since their first
attempt, and must not retry it after learning that ownership changed. A
future API client needs a retry deadline below 95 s.

On `UNKNOWN`, reread the sandbox with `Get` and reassess the requested
operation against its execution and state. Do not use `BeginTransition` as a
read-only lookup: it can start a new operation. A fresh claim is younger
than 95 s, so the stale list does not show it.

The claim fences only `FinishTransition` and `RestoreRunning` with the old
ID, through the state and the new transition ID. It does not cancel a node
call that the previous owner still runs, and it does not lock the sandbox
for later operations. Before an automatic recovery uses these methods, the
orchestrator calls must be shown to be safe to repeat and limited to the
expected execution. Automatic recovery is not approved yet. Like
`new_transition_id`, a transition ID must never be reused.

### Admission

The live count of a project is split over counter slots 0 to 63. A sandbox is
counted on one slot, and `FinishStart` or `FinishTransition` decreases that
same slot.

| `limit` | Slots | Admission |
|---|---|---|
| up to 10,000 | 0 | Exact, if every concurrent `Reserve` of the project sends a limit in this range |
| 10,001 to 100,000 | 0 to 15 | Can exceed the limit while reservations run concurrently |
| above 100,000 | 0 to 63 | Can exceed the limit while reservations run concurrently |

A `Reserve` compares the limit with its own slot, locked, plus the other slots
as they were when it started. When the limit of a project changes from above
10,000 to 10,000 or less, reservations that still send the old limit count on
other slots, so the project can exceed the new limit while both run.

The slot is the last byte of the reservation ID modulo the slot count. Use a
random UUID (version 4 or 7) for each start.

Timestamps are stored with microsecond precision. `max_instance_length` must be
between 1 µs and 9,223,372,036 s, in whole microseconds.

## Configuration

| Variable | Required | Meaning |
|---|---|---|
| `SCHEDULING_GRPC_ADDRESS` | yes | Plaintext gRPC listen address, for example `:50051` |
| `SCHEDULING_DATABASE_URL` | yes | PostgreSQL URL shared by both pools: the same database, role and primary endpoint. The cleanup worker opens one more connection, outside the pools. |
| `SCHEDULING_WRITE_POOL_MAX_CONNS` | no | Maximum write-pool connections. Positive `int32`, default `24`. Overrides URL `pool_max_conns`. |
| `SCHEDULING_READ_POOL_MAX_CONNS` | no | Maximum read-pool connections. Positive `int32`, default `8`. Overrides URL `pool_max_conns`. |
| `NODE_ID` | yes | Node ID reported in telemetry |
| `ENVIRONMENT` | no | `local` skips the shutdown drain |

Other pgx pool settings go in the URL. `pool_min_conns` and
`pool_min_idle_conns` must each fit within both pool limits. The default limits
allow up to 32 connections per service replica, or 320 for 10 replicas.

Telemetry uses the shared OpenTelemetry client and its standard environment.
Each store operation is a span. The service does not log request or response
payloads.

## Startup, health and shutdown

On start, the service opens two pgx pools and pings each with a separate 10 s
timeout. A startup failure closes both pools and the listener, and the service
exits with an error. It does not apply migrations; run `make migrate` first.

Every new connection checks `pg_is_in_recovery()` and `transaction_read_only`
to require a writable primary. Both pools set `synchronous_commit=on` in the
connection startup parameters, including for replacement connections. The
service does not check or enforce `fsync`.

The standard gRPC health service then reports `SERVING` for `""` and
`e2b.scheduling.v1.SchedulingService`. On `SIGTERM` or `SIGINT` the cleanup
worker stops, releases its lock and closes its connection, and both health
statuses change to `NOT_SERVING`. The listener keeps serving for 10 s, then
stops gracefully within 10 s. Both database pools close after the worker has
stopped and the last request has finished.

Every request has a default 10 s server deadline and respects an earlier caller
deadline.

## Database

The service keeps actor state in its own PostgreSQL database. All tables are in
the `public` schema and have the `scheduling_` prefix, including the goose
ledger `scheduling_migrations`. The versioned migrations are in
`internal/db/migrations`.

Both pools connect to the same primary. `Get`, transition watches, standalone
retry-inspection reads and read-only snapshot transactions use the read pool.
Mutations and all reads within a mutation transaction use the write pool.

The store's queries are in `internal/db/queries/sql`. sqlc (configured in
`sqlc.yaml`, schema read from the migrations) generates the Go code in
`internal/db/queries/gen`.

`make migrate` applies the pending migrations to the database that
`SCHEDULING_MIGRATOR_DATABASE_URL` names and exits. Concurrent and repeated
runs are safe.

The release image includes the migrator. Set `SCHEDULING_API_IMAGE` to the
image used for the service. Export `SCHEDULING_MIGRATOR_DATABASE_URL` with its
database URL. Run this command before starting the service:

```bash
docker run --rm --entrypoint /usr/local/bin/scheduling-api-migrate \
  -e SCHEDULING_MIGRATOR_DATABASE_URL "$SCHEDULING_API_IMAGE"
```

| Table | Contents |
|---|---|
| `scheduling_actor` | Data that does not change while the actor lives. Written by `Put`, deleted by `FinishTransition`. |
| `scheduling_actor_run` | State, deadline (`end_time`), revision, transition, failure and retry window of an actor. Written by `Reserve`. A row in state `gone` is a tombstone, kept at least 5 minutes. |
| `scheduling_project_live_count` | Live actors per project, split over counter slots 0 to 63. `scheduling_actor_run.counter_slot` names the slot that counts the actor. |

`scheduling_actor_run.state` uses the PostgreSQL enum `public.scheduling_actor_state`:
`starting`, `running`, `pausing`, `killing`, `snapshotting`, `gone`.
State groups use explicit labels, not enum ordering.
Transition actions in `transition_action`:
1 pause, 2 kill, 3 snapshot.
No method begins a snapshot yet; `Get` and `BeginTransition` report a
snapshotting sandbox as such.

Nothing ends a transition whose caller never calls `FinishTransition` or
`RestoreRunning` on its own. Such a sandbox keeps counting against the limit
of its project, and waiters get `WAITING`, until a caller recovers it (see
[Stale transitions](#stale-transitions)). The cleanup worker does not touch
transitions.

Start expiry and the tombstone purge read the table with a scan, at most a
given number of rows per call. The table has no index on `state`,
`reserved_at` or `gone_at`, so that state changes stay HOT updates.

### Tombstones

A gone row (state 6) keeps the outcome of the latest start or transition of
a sandbox. A new start of the same sandbox reuses the row and replaces that
outcome at once, so this is not a guarantee for every earlier result. A gone
row that no new start reuses is kept for at least 5 minutes after `gone_at`,
by the database clock. The 5 minutes are fixed in the service. While the row
remains:

- `Reserve` with the same reservation ID returns `FINISHED`, with the failure
  if one was recorded.
- `FinishStart`, `FinishTransition` and `WaitForTransition` for the ended
  start or transition return their terminal outcome, and `BeginTransition`
  with the ended transition ID returns `ALREADY_FINISHED`.
- `Reserve` from another project returns `ALREADY_EXISTS`.

`Store.PurgeTombstones` deletes gone rows that are older than that, up to a
given number per call, in one statement. It does not change the live counts:
a gone sandbox is not counted. A row that a concurrent `Reserve` is reusing
is skipped, and purges on several replicas delete different rows. After the
purge the service keeps nothing of the sandbox:

- `FinishStart`, `FinishTransition`, `WaitForTransition` and
  `BeginTransition` return `NOT_FOUND`.
- `Reserve` with the old reservation ID starts a new attempt and returns
  `RESERVED`. Retry a lost response with the same identifiers only for a
  bounded time, well within the 5 minutes. Never reuse a reservation ID
  indefinitely.
- Any project can reserve the sandbox ID.

The [background cleanup](#background-cleanup) runs the purge.

### Background cleanup

Each replica runs a cleanup worker. Only the replica that holds the
PostgreSQL session advisory lock `0x73636865645f636c` (a 64-bit key) in the
scheduling database does the work. The others try to take the lock once per
round, 10 s apart, and do not keep a connection between tries. The lock is
held on a dedicated connection, one per replica, outside the pool that
`pool_max_conns` sizes, so a pool of one connection also works. That
connection always has `application_name` `scheduling-api-cleanup` and
`idle_session_timeout` 60 s, whatever the database URL sets.
`idle_session_timeout` needs PostgreSQL 14 or later; the tests use
PostgreSQL 18.

After each round, the worker waits 10 s before the next attempt. Each round
has a 10 s work budget split between start expiry and tombstone deletion,
5 s each. Connecting and trying the lock share a 5 s timeout, which comes
before the budget. The budgets end the work by cancelling it. The lock check before
each batch is not cancelled by a budget and has its own 1 s timeout, so a
round can take a little longer when the database is slow to answer.

1. Start expiry ends expired starts (see [Start deadline](#start-deadline))
   in batches of 1000, in actor ID order. The next batch or round continues
   after the last start it tried. Starts that fail every time therefore do
   not keep the others from ending. A start that the budget interrupts is
   tried again from there; it is not a failure.
2. Tombstone deletion deletes tombstones past the retention (see
   [Tombstones](#tombstones)), 1000 per statement, while a batch is full. It
   runs even when start expiry used its whole budget.

Full batches are 10 ms apart. The work uses the service pool, in short
transactions with a 1 s lock timeout and a 5 s statement timeout; request
queries keep their own limits. A start whose counter row stays locked longer
than 1 s fails for this round and is tried again later.

Before each batch, the leader checks that its lock connection still runs on
the backend that took the lock. If the check fails, it closes the connection
and tries to take the lock again in the next round. If the leader's host
fails or its process hangs, PostgreSQL ends the idle lock session after the
60 s idle session timeout, if the database is healthy, and another replica
takes the lock at its next try, up to 10 s later; it is not immediate. Two
replicas can briefly both work, for example when a lost connection has not
been noticed yet. That is safe: every statement checks the state of the row
it changes, and the purge skips locked rows. A lost lock does not mean that
any sandbox stopped.

The worker logs a round that changed rows, with counts and duration. A round
with failures logs one error with the failure counts and the first error of
each step; running out of a budget is not a failure. A start that fails on
every round, for example because its counter row is missing, logs an error
every round: it means that the live counts are wrong, and it needs an
operator; the service does not repair counters. The worker also logs when it
acquires, loses or releases the lock. Each store call is a span.

The worker does not end transitions (pausing, killing) and does not repair
counters. On a database without the migrations, each round logs an error.

## Sandbox UUIDs

The helper in `internal/id` converts current-format sandbox IDs to internal
actor UUIDs, and back. Only this service uses the actor UUIDs; its API keeps
sandbox IDs. The helper rejects other sandbox ID shapes.

The 20 characters are a base36 number P (`0`-`9` = 0-9, `a`-`z` = 10-35).
P is less than 36^20, so it fits in 104 bits. It is packed into an RFC 9562
version 8 UUID, scheme revision 1:

| Bits    | Value                   |
|---------|-------------------------|
| 127-112 | magic `0xe2b5`          |
| 111-110 | scheme revision `0b01`  |
| 109-80  | P bits 103-74           |
| 79-76   | UUID version `0b1000`   |
| 75-64   | P bits 73-62            |
| 63-62   | RFC 9562 variant `0b10` |
| 61-0    | P bits 61-0             |

```text
i00000000000000000000  e2b54000-0000-8000-8000-000000000000
i5f3k9q2m8x1c7v4b6n0p  e2b54659-8cda-8bc6-abe3-090364ffe509
izzzzzzzzzzzzzzzzzzzz  e2b56a2e-2d14-88a4-9fe8-20ffffffffff
```

A UUID converts back only if its version, variant, magic and revision match
exactly and P is less than 36^20. Any other UUID, for example UUIDv4 or
UUIDv7, is not decoded by this helper. The magic and revision identify the
format only; they do not prove that this service made the UUID.

Do not change this layout. A different layout needs a new scheme revision.

## Tests

The database tests need a PostgreSQL server. Each test gets its own database,
copied from a migrated template on that server. Do not point
`PG_TEST_DATABASE_URL` at a database you care about.

```bash
docker run -d --rm --name pg-test -p 55432:5432 --tmpfs /var/lib/postgresql \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_INITDB_ARGS="--no-sync -c max_connections=500 -c fsync=off -c synchronous_commit=off -c full_page_writes=off -c timezone=UTC" \
  postgres:18-alpine
export PG_TEST_DATABASE_URL="postgres://postgres:postgres@localhost:55432/postgres?sslmode=disable"
make test
```

`go test -short` skips the database tests.

| Package | Covers |
|---|---|
| `internal/store` | The SQL: admission under concurrency, retries, project isolation, counter slots, failed starts, start deadlines and expiry under concurrency, tombstone purge under concurrency, transitions under concurrency, stale transition lists and claims under concurrency, retry windows, waits across store instances, rollback on failure |
| `internal/cleanup` | The cleanup worker: one leader across workers, a new leader after the leader's backend ends, release on stop, expiry and purge rounds, a locked counter row, database failures |
| `internal/server` | The gRPC methods through the production server and interceptors: validation, outcomes, waits across two servers, deadlines, page tokens, sanitized errors |
| `.` | Startup with the database, health, shutdown, cleanup with a one-connection pool |

## Commands

```bash
make generate   # regenerate the Go code from the proto and the SQL queries
make build      # bin/scheduling-api
make migrate    # apply the database migrations
make test       # go test -race ./...; database tests need PG_TEST_DATABASE_URL
make run-local  # build and listen on :50051; needs SCHEDULING_DATABASE_URL
make image      # local image, built from the packages directory
```
