# scheduling-api

gRPC service for scheduling. It serves `e2b.scheduling.v1.SchedulingService`,
which admits sandboxes against the live limit of their project and stores the
state of live sandboxes in PostgreSQL. No other service calls it yet.

See [request flows](docs/request-flows.md) for the implemented start, pause and
kill paths, the proposed methods that are not implemented, and the later
scheduling-owned flow.

The contract is `e2b/scheduling/v1/scheduling.proto` in this directory. The
generated Go code is in `packages/shared/pkg/grpc/scheduling`. After a change
to the proto or to a SQL query, run `make generate`. Generation needs network
access: it fetches the protovalidate rules with `buf export`.

## Methods

The caller authenticates the user and sends the project ID. Reaching this
service is the access-control boundary. A request never reads or changes a
sandbox of another project.

Requests use current-format sandbox IDs (`i` followed by 20 lowercase base36
characters) and UUID project, reservation and transition IDs. The service
stores a sandbox under an internal actor UUID (see
[Sandbox UUIDs](#sandbox-uuids)) and returns the sandbox ID.

A start is `Reserve`, then `Put` when the node has created the sandbox, or
`FinishStart` when it has not.

| Method | Effect | Outcomes |
|---|---|---|
| `Reserve` | Puts the sandbox in the starting state with the requested `end_time`, and adds one to the live count of the project. One transaction. | `RESERVED`, `ALREADY_PENDING`, `ALREADY_RUNNING` (with the sandbox), `LIMIT_EXCEEDED`, `FINISHED` (with the failure, if `FinishStart` recorded one) |
| `Put` | Stores the sandbox record and makes the sandbox running. One transaction. Does not change `end_time`. | `STORED`, `NOT_FOUND` |
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
| `RestoreRunning` | Makes a pausing sandbox running again and sets a retry window. The record and the live count do not change. One statement. | `RESTORED` (with the retry window), `NOT_FOUND`, `NOT_RESTORABLE`, `ALREADY_FINISHED` |
| `WaitForTransition` | Waits at most `max_wait` for the transition to end. The calls for one transition share the reads on each replica. Writes nothing. | `FINISHED` (with the failure, if any), `RESTORED`, `WAITING`, `NOT_FOUND`, `UNKNOWN` |

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

A gRPC error means one of these:

| Code | Cause |
|---|---|
| `INVALID_ARGUMENT` | Field validation reports all violations with `buf.validate.Violations` details. The record must also be a JSON object that PostgreSQL jsonb can store. `max_instance_length` and `retry_after` must also be whole numbers of microseconds, and `max_wait` at most 5 s. These checks return a fixed message without details. |
| `ALREADY_EXISTS` | `Reserve` for a sandbox ID that another project holds, including a gone sandbox. |
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
the sandbox until the next `Reserve` or `BeginTransition` replaces it.

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
running. It does not apply to a kill (`NOT_RESTORABLE`). The sandbox is running
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
| `SCHEDULING_DATABASE_URL` | yes | PostgreSQL URL shared by both pools: the same database, role and primary endpoint. |
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
`e2b.scheduling.v1.SchedulingService`. On `SIGTERM` or `SIGINT` both change to
`NOT_SERVING`. The listener keeps serving for 10 s, then stops gracefully
within 10 s. Both database pools close after the last request has finished.

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
| `scheduling_actor_run` | State, deadline (`end_time`), revision, transition, failure and retry window of an actor. Written by `Reserve`. A row in state `gone` is a tombstone. |
| `scheduling_project_live_count` | Live actors per project, split over counter slots 0 to 63. `scheduling_actor_run.counter_slot` names the slot that counts the actor. |

`scheduling_actor_run.state` uses the PostgreSQL enum `public.scheduling_actor_state`:
`starting`, `running`, `pausing`, `killing`, `snapshotting`, `gone`.
State groups use explicit labels, not enum ordering.
Transition actions in `transition_action`:
1 pause, 2 kill, 3 snapshot.
No method begins a snapshot yet; `Get` and `BeginTransition` report a
snapshotting sandbox as such.

Nothing removes tombstones yet. Nothing ends a start whose caller never calls
`Put` or `FinishStart`, or a transition whose caller never calls
`FinishTransition` or `RestoreRunning`. Such a sandbox keeps counting against
the limit of its project, and waiters get `WAITING`.

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
| `internal/store` | The SQL: admission under concurrency, retries, project isolation, counter slots, failed starts, transitions under concurrency, retry windows, waits across store instances, rollback on failure |
| `internal/server` | The gRPC methods through the production server and interceptors: validation, outcomes, waits across two servers, deadlines, sanitized errors |
| `.` | Startup with the database, health, shutdown |

## Commands

```bash
make generate   # regenerate the Go code from the proto and the SQL queries
make build      # bin/scheduling-api
make migrate    # apply the database migrations
make test       # go test -race ./...; database tests need PG_TEST_DATABASE_URL
make run-local  # build and listen on :50051; needs SCHEDULING_DATABASE_URL
make image      # local image, built from the packages directory
```
