# Develop the application locally

> Note: Linux is required for developing on bare metal. This is a work in progress. Not everything will function as expected.

## Prerequisites

| Requirement | Minimum | Notes |
|---|---|---|
| OS | Linux (bare metal or VM with nested virtualization) | KVM support required for Firecracker |
| CPU | 4+ cores recommended | Firecracker spawns microVMs |
| RAM | 8 GB minimum, 16 GB recommended | Huge pages alone reserve ~4 GB |
| Go | - | See `go.work` for the exact version |
| Docker / Docker Compose | Docker Engine 24+ with Compose v2 | `docker compose version` to verify |
| Node.js / npm | Required for building base template | `node --version` |
| gsutil | Google Cloud SDK CLI | Used to download prebuilt kernels and firecrackers |

**KVM check** — the orchestrator runs Firecracker microVMs, which require `/dev/kvm`:
```bash
ls -l /dev/kvm
# If missing: sudo modprobe kvm_intel  (or kvm_amd)
# For cloud VMs, ensure nested virtualization is enabled.
```

## System prep

1. Load the NBD (Network Block Device) kernel module:
   ```bash
   sudo modprobe nbd nbds_max=64
   ```
   Verify: `lsmod | grep nbd` should show the module loaded.

2. Enable huge pages:
   ```bash
   sudo sysctl -w vm.nr_hugepages=2048
   ```
   Verify: `grep HugePages_Total /proc/meminfo` should show `2048`.

> To persist these across reboots, add `nbd nbds_max=64` to `/etc/modules-load.d/nbd.conf` and `vm.nr_hugepages=2048` to `/etc/sysctl.d/99-hugepages.conf`.

## Download prebuilt artifacts (customized firecrackers and linux kernels)

Requires `gsutil` from the Google Cloud SDK.

1. `make download-public-kernels` download linux kernels
2. `make download-public-firecrackers` download firecracker versions

Verify:
```bash
ls packages/fc-kernels/        # should contain kernel binaries
ls packages/fc-versions/builds/ # should contain firecracker binaries (executable)
```

## Run the local infrastructure

```bash
make local-infra
```

This runs the following Docker Compose services (defined in `packages/local-dev/docker-compose.yaml`):

### Required services (core functionality)

| Service | Purpose | Port(s) |
|---|---|---|
| **postgres** | Primary database (users, teams, templates) | 5432 |
| **clickhouse** | Analytics and sandbox metrics | 8123 (HTTP), 9000 (native) |
| **redis** | Caching, pub/sub, orchestrator coordination | 6379 |

### Observability stack (optional for basic operation)

These services provide logging, tracing, and metrics. The application runs without them, but you will see connection errors in logs. For a minimal setup you can comment them out in `packages/local-dev/docker-compose.yaml`.

| Service | Purpose | Port(s) |
|---|---|---|
| **grafana** | Dashboards and log/trace viewer | 53000 |
| **loki** | Log aggregation | 3100 |
| **tempo** | Distributed tracing | 3200 |
| **mimir** | Metrics storage (Prometheus-compatible) | — |
| **memcached** | Cache for Tempo | 11211 |
| **otel-collector** | OpenTelemetry collector | 4317 (gRPC), 4318 (HTTP) |
| **vector** | Log forwarder (ships to Loki) | 30006 |

Verify: `docker compose -f packages/local-dev/docker-compose.yaml ps` — all services should show `running` (except `tempo-init` which exits after setup).

## Prepare local environment

1. Initialize the Postgres database:
   ```bash
   make -C packages/db migrate-local
   ```

2. Initialize the ClickHouse database:
   ```bash
   make -C packages/clickhouse migrate-local
   ```

3. Build envd (embedded in sandbox templates):
   ```bash
   make -C packages/envd build
   ```
   Verify: `ls packages/envd/bin/envd` exists.

4. Seed the database with a local dev user, team, and API tokens:
   ```bash
   make -C packages/local-dev seed-database
   ```
   This creates a local dev user, a team (`local-dev team`), and the API tokens shown in [Client configuration](#client-configuration) below.

## Run the application locally

These commands launch each service in the foreground. You need multiple terminal windows (or use `tmux`/`screen`).

1. **API server** (terminal 1):
   ```bash
   make -C packages/api run-local
   ```
   Verify: `curl -s http://localhost:3000/health` returns a response.

2. **Orchestrator + template manager** (terminal 2):
   ```bash
   make -C packages/orchestrator build-debug && sudo make -C packages/orchestrator run-local
   ```
   > `sudo` is required because the orchestrator manages Firecracker microVMs, which need root access for KVM, networking (TAP devices), and cgroup management.

   Verify: `curl -s http://localhost:5008/health` returns a response.

3. **Client proxy** (terminal 3):
   ```bash
   make -C packages/client-proxy run-local
   ```
   Verify: `curl -s http://localhost:3003/health` returns a response.

## Build the base template

Once all services are running:

```bash
make -C packages/shared/scripts local-build-base-template
```

This instructs the orchestrator to create the `base` sandbox template, which is required before you can create sandboxes.

## Verify it works (hello world)

After starting all services and building the base template, verify end-to-end operation:

```bash
# Create a sandbox using the local API
curl -s -X POST http://localhost:3000/sandboxes \
  -H "X-API-Key: e2b_53ae1fed82754c17ad8077fbc8bcdd90" \
  -H "Content-Type: application/json" \
  -d '{"templateID": "base"}'
```

A successful response returns a JSON object with a `sandboxID`. You can then interact with the sandbox through the client proxy at `localhost:3002`.

Using the Python or JS SDK:
```python
import e2b

# Configure for local development
sandbox = e2b.Sandbox(
    api_key="e2b_53ae1fed82754c17ad8077fbc8bcdd90",
    api_url="http://localhost:3000",
    sandbox_url="http://localhost:3002",
    template="base",
)
print(sandbox.id)  # sandbox is running locally
```

## Client configuration

```dotenv
E2B_API_KEY=e2b_53ae1fed82754c17ad8077fbc8bcdd90
E2B_API_URL=http://localhost:3000
E2B_SANDBOX_URL=http://localhost:3002
```

These tokens are generated by the `seed-database` step and are hardcoded for local development.
On a host other people can reach, do not use the fixed key: set `SEED_TEAM_API_KEY` to a key of your own
(`e2b_` plus at least 32 hex characters), or to `random` together with `SEED_TEAM_API_KEY_FILE=<path>`
to have the seed generate one and keep it in that file, where a re-run picks it up again.

# Services

| Service | URL / Connection string |
|---|---|
| Grafana | http://localhost:53000 |
| Postgres | `postgres:postgres@127.0.0.1:5432` |
| ClickHouse (HTTP) | http://localhost:8123 |
| ClickHouse (native) | `clickhouse:clickhouse@localhost:9000` |
| Redis | `localhost:6379` |
| OTel Collector (gRPC) | `localhost:4317` |
| OTel Collector (HTTP) | `localhost:4318` |
| Vector | `localhost:30006` |
| e2b API | http://localhost:3000 |
| e2b Client Proxy | http://localhost:3002 |
| e2b Orchestrator | http://localhost:5008 |

# Environment files

Each service reads its configuration from a `.env.local` file in its package directory. These are checked into the repo with working defaults for local development — you should not need to modify them.

| File | Purpose |
|---|---|
| `packages/api/.env.local` | API server config: database connections, Redis, OTEL, volume tokens |
| `packages/orchestrator/.env.local` | Orchestrator config: storage paths, firecracker/kernel dirs, Redis |
| `packages/client-proxy/.env.local` | Client proxy config: edge URL, service discovery, Redis |
| `packages/shared/scripts/.env.local` | Build scripts: API keys and URL for template builds |
| `tests/integration/.env.local` | Integration test config: API URL, test credentials |

# Unit tests that need PostgreSQL

The database tests in `packages/db`, `packages/api` and `packages/local-dev` share one PostgreSQL server, named by `PG_TEST_DATABASE_URL`. Each test gets its own database, copied from a template, and dropped when the test ends. A disposable server tuned for tests keeps them fast:

```bash
docker run -d --rm --name e2b-test-postgres -p 55432:5432 --tmpfs /var/lib/postgresql \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_INITDB_ARGS="--no-sync -c max_connections=500 -c fsync=off -c synchronous_commit=off -c full_page_writes=off" \
  postgres:18-alpine
export PG_TEST_DATABASE_URL="postgres://postgres:postgres@localhost:55432/postgres?sslmode=disable"
(cd packages/db && go test ./...)
```

Without the variable those tests fail; `go test -short` skips them.

# Unit tests that need Redis

Most tests that call `redis_utils.SetupInstance` share one Redis server, named by `REDIS_TEST_DATABASE_URL`. Each test's client namespaces its keys and removes them when the test ends, so a disposable server is enough:

```bash
docker run -d --rm --name e2b-test-redis -p 56379:6379 redis:8-alpine --save "" --appendonly no
export REDIS_TEST_DATABASE_URL="redis://localhost:56379/0"
```

Without the variable those tests fail; `go test -short` skips them. The few tests that rely on commands a namespaced client cannot isolate (SCAN patterns, raw key names in command hooks, the shared pub/sub channel) call `SetupContainerInstance` and still start a Redis container of their own.

CI also runs the sandbox store's suites on a one-node cluster-mode Valkey, where a command or script whose keys span hash slots fails with `CROSSSLOT`, as it does on a sharded deployment. When `REDIS_TEST_CLUSTER_URL` is set, `SetupInstance` uses that server instead. A cluster client dials the port the node announces, so publish the port the server listens on, at 55535 or below (the cluster bus takes the port plus 10000):

```bash
docker run -d --rm --name e2b-test-valkey-cluster -p 46380:46380 valkey/valkey:8.1.10 \
  valkey-server --port 46380 --cluster-enabled yes --save "" --appendonly no
sleep 1
docker exec e2b-test-valkey-cluster valkey-cli -p 46380 cluster addslotsrange 0 16383
until docker exec e2b-test-valkey-cluster valkey-cli -p 46380 cluster info | grep -q cluster_state:ok; do sleep 1; done
export REDIS_TEST_CLUSTER_URL="redis://localhost:46380/0"
```

# Troubleshooting

### `/dev/kvm` not found

The orchestrator requires KVM to run Firecracker microVMs.

- **Bare metal**: Load the module: `sudo modprobe kvm_intel` (Intel) or `sudo modprobe kvm_amd` (AMD).
- **Cloud VM**: Enable nested virtualization. On GCP: stop the VM, set `--enable-nested-virtualization`, restart. On AWS: use a `.metal` instance type.
- **WSL2**: KVM is not supported. Use a Linux VM with nested virtualization instead.

### Port conflicts

If `make local-infra` fails with "address already in use", check which service is conflicting:

```bash
sudo ss -tlnp | grep <port>
```

Common conflicts: PostgreSQL on 5432, Redis on 6379, or another Grafana on 3000 (the local Grafana uses 53000 to avoid this).

### Orchestrator fails without `sudo`

The orchestrator manages Firecracker microVMs, which require root privileges for:
- `/dev/kvm` access
- Creating TAP network interfaces
- Managing cgroups for microVM resource limits
- Mounting NBD devices

This is expected. Always run the orchestrator with `sudo`.

### `gsutil` not found when downloading artifacts

Install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install

You do not need to authenticate — the public builds bucket allows anonymous access.

### ClickHouse migration fails

Ensure ClickHouse is fully started before running migrations. After `make local-infra`, wait a few seconds for ClickHouse to initialize, then retry:

```bash
make -C packages/clickhouse migrate-local
```

### Huge pages warning

If you see memory allocation errors from Firecracker, verify huge pages are allocated:

```bash
grep HugePages /proc/meminfo
```

If `HugePages_Free` is 0, the system may not have enough free memory. Stop other applications or increase RAM.
