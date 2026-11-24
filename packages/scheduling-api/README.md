# scheduling-api

gRPC service for scheduling. It serves the start reservation service
`e2b.scheduling.reservation.v1.ReservationService` and the sandbox state service
`e2b.scheduling.state.v1.StateService` on the Redis data of the API: it connects to
the same Redis and reads and writes the same keys. The API's sandbox store
calls it when the `sandbox-state-scheduling-api` flag is on, which it is not by
default.

The Go client is `packages/shared/pkg/sandboxstore/remote`. It implements the
sandbox store interfaces `Storage` and `ReservationStorage` over a gRPC
connection to this service, with the same results and errors as the Redis
backends.

The generated Go code of the contracts is in
`packages/shared/pkg/grpc/contracts/scheduling/reservation` and
`packages/shared/pkg/grpc/contracts/scheduling/state`.

## Transport

Set `E2B_TLS_CERT_FILE`, `E2B_TLS_KEY_FILE`, and `E2B_TLS_CA_FILE` to the
service certificate chain, private key, and trusted CA bundle. Certificates carry
a SPIFFE URI SAN and the appropriate client/server authentication usage.
Use a separate service identity, not the orchestrator identity.
Set `E2B_TLS_GRPC_ALLOW` to the comma-separated API SPIFFE identities permitted
to call the service. Wildcards are rejected.
`E2B_TLS_GRPC_MODE` defaults to `required`; only `required` and `off` are accepted.
Missing or unreadable certificate files and an empty allow-list fail startup in
required mode. The shared loader reloads certificates every minute.

For an isolated plaintext local run, explicitly set `E2B_TLS_GRPC_MODE=off`.
The shared mTLS listener permits plaintext gRPC health probes in required mode,
but rejects other plaintext calls.

## Methods

The protos describe each method, its fields and its outcomes.

### ReservationService

| Method | Outcomes |
|---|---|
| `Reserve` | `RESERVED`, `ALREADY_PENDING`, `ALREADY_EXISTS`, `LIMIT_EXCEEDED`, `KILLED` |
| `FinishStart` | none |
| `WaitForStart` | `FINISHED` (with the result), `WAITING`, `NO_RESULT` |
| `Release` | none |
| `ClaimKill` | `claimed` with `claim_id`, or not claimed |
| `ReleaseKillClaim` | none |

### StateService

The records of running sandboxes, as the JSON of the store's record.

| Group | Methods |
|---|---|
| Records | `Add`, `Get`, `Remove`, `TeamItems` (stream), `ExpiredItems` (stream), `TeamsWithSandboxCount`, `Reconcile` |
| Updates | `BeginUpdate`, `CommitUpdate`, `AbortUpdate` |
| Holds | `HoldExecution`, `MoveHeldExecution`, `ReleaseHeldExecution`, `RemoveHeldExecution` |
| Transitions | `BeginRemoving`, `FinishTransition`, `RestoreRunning`, `RestoreCheckpointed`, `WaitForTransition` |

The service validates the requests, unary and stream, with the protovalidate
rules of the protos and returns `INVALID_ARGUMENT` with
`buf.validate.Violations` details. A `WaitForStart` or `WaitForTransition`
call waits at most 5 s and then returns `WAITING`.

## Configuration

The `INFRA_REDIS_*` variables name the Redis that holds the sandbox state
today, shared with the API: a temporary dependency until the service has its
own storage.

| Variable | Required | Meaning |
|---|---|---|
| `SCHEDULING_GRPC_ADDRESS` | yes | gRPC listen address, for example `:50051`; mTLS is required by default |
| `INFRA_REDIS_URL` | one of the two | Redis URL (`redis://`, `rediss://` or `host:port`) of a standalone server |
| `INFRA_REDIS_CLUSTER_URL` | one of the two | `host:port` of a cluster; used instead of `INFRA_REDIS_URL` when both are set |
| `INFRA_REDIS_TLS_ENABLED` | no | `true` connects with TLS |
| `INFRA_REDIS_TLS_CA_BASE64` | no | Base64 PEM CA bundle for TLS; needs `INFRA_REDIS_TLS_ENABLED` |
| `INFRA_REDIS_PASSWORD` | no | Redis password; needs TLS |
| `INFRA_REDIS_POOL_SIZE` | no | Connection pool size, default `160` |
| `NODE_ID` | yes | Node ID reported in telemetry |
| `ENVIRONMENT` | no | `local` skips the shutdown drain |

Set the Redis variables to the values of the API, so that both use the same
Redis data.

Telemetry uses the shared OpenTelemetry client and its standard environment.
The service does not log request or response payloads.

The shared gRPC server metrics count calls by status code.

## Startup, health and shutdown

On start, the service connects to Redis and pings it. The shared Redis client
retries the first ping for up to 5 minutes. If the Redis connection, the TLS
configuration or the listener fails on start, the service closes what it
opened and exits with an error.

The standard gRPC health service then reports `SERVING` for `""`,
`e2b.scheduling.reservation.v1.ReservationService` and
`e2b.scheduling.state.v1.StateService`.
On `SIGTERM` or `SIGINT` all health statuses change to `NOT_SERVING`. The listener keeps serving for 10 s, then
stops gracefully within 10 s. Then the service stops the sandbox store and
closes the Redis client.

As in the API, the sandbox store runs in the background: it sends and receives
the Redis notifications that wake `WaitForStart` and `WaitForTransition` calls
and the waits for a record's lock, and it repairs the
sandbox store indexes every few minutes.

Every unary request has a default 70 s server deadline and respects an
earlier caller deadline: a state call can wait up to one minute for a record's
lock. Streams have no server deadline. A message, request or response, can be
at most 32 MiB.

## Tests

The tests of `internal/reservation`, `internal/state`, `internal/server` and
`.` need a Redis server, named by `REDIS_TEST_DATABASE_URL`. Each test's
client namespaces its keys and removes them when the test ends.

```bash
docker run -d --rm --name e2b-test-redis -p 56379:6379 redis:8-alpine --save "" --appendonly no
export REDIS_TEST_DATABASE_URL="redis://localhost:56379/0"
make test
```

`go test -short` skips the Redis tests.

| Package | Covers |
|---|---|
| `internal/config` | Configuration parsing and TLS settings validation |
| `internal/reservation` | Reservation gRPC handlers: outcomes, validation and errors, against Redis and a fake store |
| `internal/state` | Sandbox state gRPC handlers: record fidelity, outcomes, validation and errors, against Redis and a fake store |
| `internal/server` | The production gRPC server with fake stores: unary and stream request validation, panic recovery, the request deadline, the `WaitForStart` and `WaitForTransition` bounds, the message limit; mTLS: trusted and wrong caller identities, untrusted certificates, wrong server identity, plaintext; the client's deadlines and message limit against the server's, and a record above 4 MiB through Redis |
| `.` | Startup with Redis, health, a start, a record and a kill through the listener, shutdown, listener in use |

## Commands

```bash
make build      # bin/scheduling-api
make test       # go test -race ./...; needs REDIS_TEST_DATABASE_URL
make run-local  # build and listen on :50051; needs INFRA_REDIS_URL, and the E2B_TLS_* files or E2B_TLS_GRPC_MODE=off
make image      # local image, built from the packages directory
```
