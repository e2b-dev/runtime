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

`-isnotfc` skips MMDS polling both at startup and after `/init`. Standalone
instances keep their explicitly initialized identity rather than polling
Firecracker metadata. Firecracker instances retain the background refresh.

To exercise the `/init` mode check against reachable metadata, run from the
repository root:

```bash
go -C packages/envd build -o bin/envd .
python3 scripts/test-envd-mmds-mode.py packages/envd/bin/envd
```

This optional Linux regression requires Python 3, Bubblewrap (`bwrap`) with
unprivileged user namespaces, and iproute2 (`ip`). It checks the running daemon
in a private network with a metadata fixture: Firecracker-mode startup and
post-init refresh must fetch metadata, while standalone initialization must not
contact it during the observation window or overwrite explicit identity files.
The fixture uses private `/run` and `/tmp` mounts and does not change host network
configuration. It does not boot a Firecracker VM.

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
