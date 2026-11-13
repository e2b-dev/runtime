# scheduling-api

gRPC service for scheduling. It serves the start reservation service
`e2b.scheduling.v1.ReservationService` on the Redis data of the API: it
connects to the same Redis and reads and writes the same keys. No caller uses
it yet.

The contract is
[`e2b/scheduling/v1/reservation.proto`](e2b/scheduling/v1/reservation.proto)
in this directory. The contract of the sandbox state service,
[`e2b/scheduling/v1/state.proto`](e2b/scheduling/v1/state.proto), is there
too; the service does not serve it yet. The generated Go code is in
`packages/shared/pkg/grpc/scheduling`. After a change to the proto, run
`make generate`. Generation needs network access: it fetches the protovalidate
rules with `buf export`.

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

The proto describes each method, its fields and its outcomes.

| Method | Outcomes |
|---|---|
| `Reserve` | `RESERVED`, `ALREADY_PENDING`, `ALREADY_EXISTS`, `LIMIT_EXCEEDED`, `KILLED` |
| `FinishStart` | none |
| `WaitForStart` | `FINISHED` (with the result), `WAITING`, `NO_RESULT` |
| `Release` | none |
| `ClaimKill` | `claimed` with `claim_id`, or not claimed |
| `ReleaseKillClaim` | none |

The service validates the requests with the protovalidate rules of the proto
and returns `INVALID_ARGUMENT` with `buf.validate.Violations` details. A
`WaitForStart` call waits at most 5 s and then returns `WAITING`.

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

The standard gRPC health service then reports `SERVING` for `""` and
`e2b.scheduling.v1.ReservationService`. On `SIGTERM` or `SIGINT` both health
statuses change to `NOT_SERVING`. The listener keeps serving for 10 s, then
stops gracefully within 10 s. Then the service stops the sandbox store and
closes the Redis client.

As in the API, the sandbox store runs in the background: it sends and receives
the Redis notifications that wake `WaitForStart` calls, and it repairs the
sandbox store indexes every few minutes.

Every request has a default 10 s server deadline and respects an earlier caller
deadline.

## Tests

The tests of `internal/reservation`, `internal/state` and `.` need a Redis
server, named by `REDIS_TEST_DATABASE_URL`. Each test's client namespaces its
keys and removes them when the test ends.

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
| `internal/server` | The production gRPC server with a fake store: request validation, panic recovery, the request deadline, the `WaitForStart` bound; mTLS: trusted and wrong caller identities, untrusted certificates, wrong server identity, plaintext |
| `.` | Startup with Redis, health, a start through the listener, shutdown, listener in use |

## Commands

```bash
make generate   # regenerate the Go code from the proto
make build      # bin/scheduling-api
make test       # go test -race ./...; needs REDIS_TEST_DATABASE_URL
make run-local  # build and listen on :50051; needs INFRA_REDIS_URL, and the E2B_TLS_* files or E2B_TLS_GRPC_MODE=off
make image      # local image, built from the packages directory
```
