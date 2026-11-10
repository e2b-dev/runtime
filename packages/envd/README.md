# envd

Daemon that runs inside a sandbox that allows interacting with the sandbox via calls from the SDK.

## Development

### Versioning

The envd version in `pkg/version.go` is maintained by release-please from the Conventional Commits merged since the last envd release; do not edit it by hand. A change that affects behavior (code, dependencies) ships under a `feat` or `fix` commit type, so that a version the orchestrator can gate on is cut. Pure comment or documentation changes use `docs`, a type the changelog hides, and cut no release; `chore` and `refactor` are visible in it and do cut one.

### Running locally

Run the following command to (re)build the envd daemon and start a Docker container with envd running inside:

```bash
make start-docker
```

You can use E2B SDKs with env var `E2B_DEBUG=true` or with a debug parameter set to `true` when creating or connecting to a sandbox, to connect to the envd started with this command.

### Generating API server stubs

Run the following command to install the necessary dependencies for generating server stubs:

```bash
make init-generate
```

#### Generating

After changing the API specs in `./spec/` run the following command to generate the server stubs:

```bash
make generate
```

### Debugging

- <https://golangforall.com/en/post/go-docker-delve-remote-debug.html>
- <https://github.com/golang/vscode-go/blob/master/docs/debugging.md>

Run `make run-debug` and then connect to the port `2345` with a debugger or use the VSCode run/debug and run the "Debug envd" to build the envd, Docker, and start the debugging.
