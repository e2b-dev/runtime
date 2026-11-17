# Scheduling client TLS

The API TLS configuration has a dedicated scheduling hop. Its caller selects
whether to construct the hop with the `schedulingHop` argument to `newServiceTLS`.
The API startup leaves it unselected until the backend integration is added.

When selected, the hop defaults to mTLS. It uses the shared certificate loader
and verifies the server's SPIFFE identity. Configure these settings:

```dotenv
E2B_TLS_CERT_FILE=/path/to/api.crt
E2B_TLS_KEY_FILE=/path/to/api.key
E2B_TLS_CA_FILE=/path/to/ca.crt
E2B_TLS_SCHEDULING_CLIENT_MODE=on
E2B_TLS_SCHEDULING_EXPECT=spiffe://local.example/scheduling-api
E2B_TLS_SCHEDULING_SERVER_NAME=scheduling-api.local
```

`E2B_TLS_SCHEDULING_CLIENT_MODE` defaults to `on` for a selected hop.
Only exact `on` and `off` values are accepted. Invalid values fail construction,
including when valid certificate files are present.
`E2B_TLS_SCHEDULING_EXPECT` is a required comma-separated list of exact server
SPIFFE identities. `E2B_TLS_SCHEDULING_SERVER_NAME` adds an optional DNS certificate
check. The dial address is not the identity check.

Use distinct identities for the API, scheduling service, and orchestrator.
Give each process only its own certificate and key, plus the CA bundle.
The shared loader reloads files every minute. Missing or invalid certificate
files fail construction in `on` mode. There is no plaintext fallback.
Explicit `off` retains the shared TLS library's local-test mode.
The settings are static, not feature-flag controlled. An unselected hop does
not load or validate its settings and has no credentials.

Run the credential and configuration tests from `packages/api`:

```sh
go test -race . ./internal/cfg
```

The tests use real TLS over TCP. They cover trusted connections, wrong server
and caller identities, untrusted certificates, plaintext rejection, and invalid
modes with valid certificates.
