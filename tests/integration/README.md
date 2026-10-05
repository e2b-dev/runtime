# Integration Tests

Package for defining integration tests. Currently, there is a setup for API and Orchestrator testing.

## Run locally

1. Configure test variables in a root `.env.<environment>` file and select it with
   `make switch-env ENV=<environment>` from the repository root. Use
   [`.env.local`](.env.local) as a reference for local API values;
   `TESTS_ORCHESTRATOR_HOST` must point to the orchestrator for tests that call it directly.
2. If you made changes to the `api` or `envd` protobuf spec, run `make generate` from this folder (and don't forget to generate it in `envd` if changes apply there too).
3. If the orchestrator is not reachable directly, tunnel its gRPC port (`TESTS_ORCHESTRATOR_HOST`, e.g. `localhost:5008`) to a sandbox node before running the tests
4. Run `make test` in this folder or `make test-integration` from the repository root.

Narrow the run with `make test/<path under internal/tests>`, e.g. `make test/api/templates`
or `make test/api/templates/build_template_test.go:TestTemplateBuildCOPY`.

## What CI runs

The `uncompressed` config runs the whole suite. The compressed configs (`zstd1`,
`lz4`) re-run only `scripts/compression-tests.tsv`, the explicit allow-list of
tests whose subject is writing or reading back a snapshot; that file documents
the inclusion criteria and the code paths the compression knobs reach. Select it
locally with `TESTS_ONLY=scripts/compression-tests.tsv make test`. An entry that
no longer matches a real test is an error, so the list cannot rot into silently
reduced coverage.

## Usage of clients (api, orchestrator, envd)

All tests are in the folder internal/tests. You can see the usage of different clients in the tests. Here are just basics.

### API

HTTP client. In order to pass the API key, use the `setup.WithAPIKey()` option.

```go
client := setup.GetAPIClient()

sbxTimeout := int32(60)
resp, err := client.PostSandboxesWithResponse(ctx, api.NewSandbox{
    Timeout:    &sbxTimeout,
}, setup.WithAPIKey())
```

### Orchestrator

GRPC client. There is no authentication needed as it runs behind API in production.

```go
client := setup.GetOrchestratorClient(t, ctx)
resp, err := client.List(ctx, &emptypb.Empty{})
```

### Envd

Envd client is used for interacting with the sandbox.
There are two API types—HTTP and GRPC.
Each of them provides different methods for interacting with the sandbox; you need to check which ones you need.

#### HTTP

In order to access correct sandbox URL, you need to pass `setup.WithSandbox(...)` with the required arguments.

```go
client := setup.GetEnvdClient(t, ctx)
resp, err := client.HTTPClient.PostFilesWithBodyWithResponse(
    ctx,
    ...,
    setup.WithSandbox(sbx.JSON201.SandboxID),
)
```

#### GRPC

In order to access correct sandbox URL, you need to call `setup.SetSandboxHeader(...)` with the required arguments.

All methods also expect a user (`user`/`root`) to be set in the header.
You can achieve it using `setup.SetUserHeader(...)`.

```go
client := setup.GetEnvdClient(t, ctx)
req := connect.NewRequest(&filesystem.ListDirRequest{
    Path: "/",
})
setup.SetSandboxHeader(req.Header(), sbx.JSON201.SandboxID)
setup.SetUserHeader(req.Header(), "user")
resp, err := client.FilesystemClient.ListDir(ctx, req)
```
