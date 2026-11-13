# scheduling-api

gRPC service for scheduling. It has no RPC contract: it serves only the
standard gRPC health service.

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

## Configuration

| Variable | Required | Meaning |
|---|---|---|
| `SCHEDULING_GRPC_ADDRESS` | yes | gRPC listen address, for example `:50051`; mTLS is required by default |
| `NODE_ID` | yes | Node ID reported in telemetry |
| `ENVIRONMENT` | no | `local` skips the shutdown drain |

Telemetry uses the shared OpenTelemetry client and its standard environment.
The service does not log request or response payloads.

The shared gRPC server metrics count calls by status code.

## Startup, health and shutdown

If the TLS configuration or the listener fails on start, the service exits with
an error.

The standard gRPC health service then reports `SERVING` for `""`. On `SIGTERM`
or `SIGINT` the health status changes to `NOT_SERVING`. The listener keeps
serving for 10 s, then stops gracefully within 10 s.

Every request has a default 10 s server deadline and respects an earlier caller
deadline.

## Tests

The tests need no external services.

| Package | Covers |
|---|---|
| `internal/config` | Configuration parsing and TLS settings validation |
| `internal/server` | mTLS of the gRPC server: trusted and wrong caller identities, untrusted certificates, wrong server identity, plaintext |
| `.` | Startup, health, shutdown, listener in use |

## Commands

```bash
make build      # bin/scheduling-api
make test       # go test -race ./...
make run-local  # build and listen on :50051; needs the E2B_TLS_* files or E2B_TLS_GRPC_MODE=off
make image      # local image, built from the packages directory
```
