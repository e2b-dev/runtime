# CLAUDE.md

This development guide applies to Codex, Claude Code, and OpenCode.
Repository and review rules are shared through:

@AGENTS.md

## Project Overview

E2B Infrastructure is the backend infrastructure powering E2B (e2b.dev), an open-source cloud platform for AI code interpreting. It provides sandboxed execution environments using Firecracker microVMs.

**Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — it explains what each service does, how services interact (with diagrams of the core flows: sandbox creation, traffic routing, pause/resume, template builds), and the deployment topology. Reading it first is the fastest way to understand this repository.

**Keep docs/ARCHITECTURE.md updated**: if your change alters anything it describes (service responsibilities, ports, protocols, data stores, flows, deployment topology), update the document in the same change.

## Common Development Commands

### Setup & Environment
```bash
# Switch between environments (prod, staging, dev)
make switch-env ENV=staging

# Setup local development stack (PostgreSQL, Redis, ClickHouse, monitoring)
make local-infra
```

### Building & Testing
```bash
# Run all unit tests across packages
make test

# Run integration tests
make test-integration

# Build specific package
make build/api
make build/orchestrator

# Generate code (proto, SQL, OpenAPI)
make generate

# Format and lint code
make fmt
make lint

# Regenerate mocks
make generate-mocks

# Tidy go dependencies
make tidy
```

### Running Services Locally
```bash
# From packages/api/
make run-local          # Run API server on :3000
make dev                # Run with air (hot reload)

# From packages/orchestrator/
make run-local          # Run orchestrator
make run-debug          # Run with race detector
```

### Package-Specific Commands
```bash
# API: Generate OpenAPI code
cd packages/api && make generate

# Orchestrator: Generate proto + OpenAPI
cd packages/orchestrator && make generate

# DB: Run migrations
make migrate

# Run single test
cd packages/<package> && go test -v -run TestName ./path/to/package
```

### Publishing Artifacts
```bash
# Build and upload all service images to your GCP project
make build-and-upload

# Build specific service
make build-and-upload/api
make build-and-upload/orchestrator
```

## Architecture Overview

### Service Communication Flow
```
Client → Client-Proxy → API (REST) ⟷ PostgreSQL
                      ↓              ⟷ Redis
                   Orchestrator     ⟷ ClickHouse
                      ↓ (gRPC)
                   Firecracker VMs
                      ↓
                   Envd (in-VM daemon)
```

### Core Services

**API (`packages/api/`)** - REST API using Gin framework
- Entry point: `main.go`
- Core logic: `internal/handlers/store.go` (APIStore)
- Authentication: API keys and OIDC auth provider JWTs
- OpenAPI code generation: `internal/api/*.gen.go`
- Port: 80

**Dashboard API (`packages/dashboard-api/`)** - REST backend for the web dashboard, not the SDK
- Entry point: `main.go`
- Spec: `spec/openapi-dashboard.yml`; legacy team management, template tags, builds, admin bootstrap, `/v1/management`
- Talks to Postgres and ClickHouse; never to orchestrators (see `docs/ARCHITECTURE.md`)
- Port: 3010

**Orchestrator (`packages/orchestrator/`)** - Firecracker microVM orchestration
- Entry point: `main.go`
- VM management: `pkg/sandbox/`
- Firecracker integration: `pkg/sandbox/fc/`
- Networking: `pkg/sandbox/network/`
- Storage: `pkg/sandbox/nbd/` (Network Block Device)
- Template caching: `pkg/sandbox/template/`
- Template building: `pkg/template/`
- gRPC server: `pkg/server/`
- Utilities: `cmd/clean-nfs-cache/`, `cmd/inspect-build/`, `cmd/dummy-orchestrator/`

**Envd (`packages/envd/`)** - In-VM daemon using Connect RPC
- Runs inside each Firecracker VM
- Process management API: `packages/envd/spec/process/process.proto`
- Filesystem API: `packages/envd/spec/filesystem/filesystem.proto`
- Port: 49983
- **Version in `pkg/version.go` must be bumped on every behavioral change** (not comments/docs-only changes)

**Client Proxy (`packages/client-proxy/`)** - Edge routing layer
- Service discovery via `packages/shared/pkg/servicediscovery`
- Request routing to orchestrators
- Redis-backed state management

**Shared (`packages/shared/`)** - Common utilities
- Proto definitions: `pkg/grpc/orchestrator/`, `pkg/grpc/envd/`
- Telemetry: `pkg/telemetry/` (OpenTelemetry)
- Logging: `pkg/logger/` (Zap + OTEL)
- Database: `pkg/db/` (ent ORM)
- Models: `pkg/models/`
- Storage: `pkg/storage/` (GCS/S3 clients)
- Feature flags: `pkg/featureflags/` (LaunchDarkly)

**Database (`packages/db/`)** - PostgreSQL layer
- Migrations: `migrations/*.sql` (goose)
- Queries: `queries/*.sql` (sqlc)
- Generated code: `queries/` (plus `pkg/auth/queries/`, `pkg/dashboard/queries/`)

### Key Technologies

- **go 1.26.8** with workspaces (`go.work`)
- **Firecracker** for microVM virtualization
- **PostgreSQL** for primary data (sqlc for queries)
- **ClickHouse** for analytics
- **Redis** for caching and state
- **OpenTelemetry** for observability (Grafana stack: Loki, Tempo, Mimir)
- **gRPC/Connect RPC** for service communication
- **Gin** (API), **chi** (Envd) for HTTP

### Code Generation

The codebase uses several code generators:

1. **Protocol Buffers** (`packages/orchestrator/generate.Dockerfile`)
   - Generates: `packages/shared/pkg/grpc/*/`
   - Run: `make generate/orchestrator`

2. **OpenAPI** (`oapi-codegen`)
   - Spec: `spec/openapi.yml`
   - Generates: API handlers, types, specs
   - Run: `make generate/api`

3. **SQL** (`sqlc`)
   - Queries: `packages/db/queries/*.sql`
   - Generates: Type-safe DB code
   - Run: `make generate/db`

4. **Mocks** (`mockery`)
   - Config: `.mockery.yaml`
   - Run: `make generate-mocks`

### Testing Patterns

- **Unit tests**: Use `testify/assert` and `testify/require`
- **Database tests**: Use `testcontainers-go` for real PostgreSQL
- **Integration tests**: `tests/integration/` with shared test utilities
- **Mocking**: Generated mocks in `mocks/` directories
- **Race detection**: Tests run with `-race` flag

Example test invocation:
```bash
# Single package
go test -race -v ./internal/handlers

# Specific test
go test -race -v -run TestCreateSandbox ./internal/handlers
```

## Important Development Notes

### Working with Proto/gRPC
- Proto files: `packages/envd/spec/process/`, `packages/envd/spec/filesystem/`, orchestrator protos at `packages/orchestrator/*.proto`
- Shared protos: `packages/shared/pkg/grpc/`
- After editing proto files, run `make generate/orchestrator` and `make generate/shared`

### Database Migrations
- Migrations: `packages/db/migrations/`
- Create: `cd packages/db && make create-migration NAME=your-migration-name` — this generates the file with a correct `YYYYMMDDHHMMSS` timestamp. Do NOT hand-create migration files; placeholder timestamps like `120000`/`000000` cause same-day version collisions and are rejected by the out-of-order-migrations CI check.
- Apply: `make migrate` (requires POSTGRES_CONNECTION_STRING)
- Code generation: `make generate/db` (regenerates sqlc code)

### Environment Variables
- Local development defaults: `packages/<service>/.env.local`; see [DEV-LOCAL.md](DEV-LOCAL.md).
- Self-hosting configuration: [embed/compose/.env](embed/compose/.env); see [embed/README.md](embed/README.md).
- Custom environment configs: `.env.<environment>`, selected with `make switch-env ENV=<environment>`.

### Firecracker & VM Management
- Orchestrator requires **sudo** to run (Firecracker needs root)
- VM networking uses `iptables` and Linux `netlink`
- Storage uses NBD (Network Block Device)
- Templates cached in GCS bucket (configurable via TEMPLATE_BUCKET_NAME)
- Kernel/Firecracker versions: `packages/fc-versions/`

### Observability
- All services export OpenTelemetry traces/metrics/logs
- Local stack includes Grafana + Loki + Tempo + Mimir
- Telemetry setup: `packages/shared/pkg/telemetry/`
- Logger: `packages/shared/pkg/logger/` (Zap with OTEL)
- Profiling: API exposes pprof on `/debug/pprof/` (see `packages/api/Makefile` profiler target)

### CI/CD Workflows
- `.github/workflows/pull-request.yml` - PR orchestrator (lint, OpenAPI, unit, arm64, integration)
- `.github/workflows/pr-tests.yml` - Unit test shards
- `.github/workflows/integration_tests.yml` - Integration test suite

## Architecture Patterns

1. **Service Isolation**: Each service runs in containers with defined gRPC/HTTP interfaces
2. **Shared Libraries**: Cross-cutting concerns (logging, telemetry, DB) in `packages/shared`
3. **Event-Driven**: ClickHouse + Redis pub/sub for async operations
4. **Caching Strategy**: Redis for templates, auth tokens, performance optimization
5. **Feature Flags**: LaunchDarkly for gradual rollouts
6. **Graceful Shutdown**: Services handle SIGTERM with context cancellation
7. **Health Checks**: gRPC health protocol + HTTP health endpoints

## Debugging

### Logs
- Local: Docker logs in `make local-infra`
- Production: Grafana Loki
