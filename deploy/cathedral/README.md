# Cathedral runtime release slice

This directory packages the existing Apache-2.0 runtime source for a private
Cathedral deployment. The upstream copyright and license remain authoritative.
The customer-facing service and catalogue use Cathedral names; upstream names
remain only where required by source compatibility or attribution.

This is a **single dedicated proof host** using the existing Embed Compose
topology and databases. It is not a production topology or an HA design.
The Compose overlay replaces the stock API and PostgreSQL migrator images and
installs a locally built orchestrator binary before its launcher starts. It
retains the Compose release's client-proxy, envd, kernel, Firecracker, BusyBox,
seed, ClickHouse migrator, and database pins. The API, migrator, and
orchestrator are built from the same exact source revision.

## Build the pinned artifacts

On a build machine with Docker Buildx, create a detached, clean source worktree
at the recorded revision. This keeps later deployment-file commits out of the
runtime artifact:

```sh
export CATHEDRAL_RUNTIME_GIT_REVISION=f11618e72af6996d4df3290078a6d54fd3099649
export CATHEDRAL_RUNTIME_SOURCE_DIR=/absolute/path/to/cathedral-runtime-source
git worktree add --detach "$CATHEDRAL_RUNTIME_SOURCE_DIR" \
  "$CATHEDRAL_RUNTIME_GIT_REVISION"
./deploy/cathedral/build-artifacts.sh \
  "$CATHEDRAL_RUNTIME_SOURCE_DIR" /absolute/private/path/cathedral-artifacts
```

The generated directory contains the orchestrator binary, its SHA-256, and a
mode-600 image-ID lock. Keep it private. If building away from the proof host,
transfer the artifact directory and the two Docker images with `docker save`
and `docker load`, then update only the lock's absolute artifact/deploy paths;
verify the image IDs again before starting. No registry push is part of this
slice. The Compose-pinned envd release remains the template's envd; record its
version at build time and confirm it in the read-only preflight.

## Start the isolated proof host

Use a dedicated x86-64 Linux host meeting `embed/compose/README.md`'s KVM,
Docker, cgroup, NBD, hugepage, disk and network requirements. The upstream
`host-setup` changes kernel modules, sysctls and iptables rules; never run it
on a host carrying other workloads. Install `nftables` and `jq` as host prerequisites.
The guard blocks every non-loopback ingress to the host-networked runtime
ports for both IPv4 and IPv6. Apply it **before** starting Compose:

```sh
sudo ./deploy/cathedral/ingress-guard.sh install
./deploy/cathedral/proof-host.sh /absolute/private/path/cathedral-artifacts config --quiet
sudo ./deploy/cathedral/proof-host.sh /absolute/private/path/cathedral-artifacts up
./deploy/cathedral/proof-host.sh /absolute/private/path/cathedral-artifacts ps
```

The API is reachable on host loopback port 3000 and sandbox traffic on 3002
after the guard; the binaries still bind all interfaces, so the guard is a
required isolation control. They are plain HTTP inside the host. A private TLS reverse proxy or
an authenticated tunnel is still required for any remote caller; expose only
those two front doors. The sandbox front door can use the existing
`E2b-Sandbox-Id` and `E2b-Sandbox-Port` routing headers, preserving
`X-Access-Token` through client-proxy, so a wildcard DNS name is unnecessary
for the Cathedral API's header-routed requests.
Do not remove the guard while the stack is running. API, orchestrator and
client-proxy have restart disabled in the overlay so a reboot cannot restart
them before the ephemeral guard is reinstalled. Reapply the guard and run `up`
after each host reboot. No public dashboard is started.

`up` migrates the local PostgreSQL database with the custom migrator, runs the
stock ClickHouse migrator and seed, installs the pinned host binary, starts the
API/proxy/orchestrator, and builds the Compose `base` template. This build is
local proof material. The `cathedral-ready` dependent makes `up --wait` wait
for the template build and healthy client-proxy without printing the team key.
It is not automatically a Cathedral catalog entry.
Use `proof-host.sh ... logs SERVICE` for failures; `down` retains volumes.

## Required deployment inputs

This proof uses Compose's local PostgreSQL, Redis, ClickHouse and filesystem
artifact store. The host must be isolated and temporary; database credentials
in upstream Compose are evaluation defaults. Before any live or customer use,
replace this topology with managed secrets, backup, private durable stores,
TLS/API routing, and operational controls. Build the intended Cathedral
template through the authenticated template API after TLS connectivity is
available. Do not catalog it until `ready`, exact shape/build ID/envd version,
and artifact digest are recorded.

## Read-only release gate

With no sandbox admission or creation involved:

```sh
"$CATHEDRAL_OPERATOR_CHECKOUT/deploy/cathedral/preflight.sh" \
  --url "$CATHEDRAL_RUNTIME_API_URL" \
  --api-key-file "$CATHEDRAL_RUNTIME_API_KEY_FILE" \
  --template-id "$CATHEDRAL_TEMPLATE_ID" \
  --build-id "$CATHEDRAL_TEMPLATE_BUILD_ID" \
  --cpu "$CATHEDRAL_TEMPLATE_CPU" \
  --memory-gib "$CATHEDRAL_TEMPLATE_MEMORY_GIB"
```

The check proves the observed template ID, build ID, shape, status, and envd
version at that moment. It does not prevent a later rebuild, so deployment must
retain and recheck the expected build ID. The remaining integration step is a
private TLS API URL and sandbox front door reachable from the Cathedral API,
with `SANDBOX_URL` pointed at that front door, header routing and guest token
preservation, then the template preflight and catalog entry. Keep
`CATHEDRAL_E2B_ENABLED=false` until a separately authorized canary.

## Runtime-only durability proof

After the private API and template are qualified, install the Cathedral API's
pinned `e2b==2.49.1` Python SDK (and its `packaging` dependency) into an
isolated Python environment. This script creates at most one secure,
120-second sandbox with internet egress denied, recovers its durable create
key, checks a replay returns the same sandbox ID, and uses the original
sandbox detail plus direct SDK constructor to run one bounded command and
write/read a tiny file. It does not call SDK connect or extend the lifetime.
It then pins the current execution and requests a durable delete, even if the
guest checks fail. It polls the delete receipt until node stop and snapshot
cleanup are both confirmed. The key file is read without printing its
contents; output contains only operation/sandbox IDs and proof states. Use a
private HTTPS origin or host loopback HTTP:

```sh
./deploy/cathedral/runtime-proof.py \
  --api-url "$CATHEDRAL_RUNTIME_API_URL" \
  --api-key-file "$CATHEDRAL_RUNTIME_API_KEY_FILE" \
  --template-id "$CATHEDRAL_TEMPLATE_ID" \
  --sandbox-url http://127.0.0.1:3002
```

Save the two printed operation keys. If the connection fails, recover by those
keys before deciding whether any further action is safe. The script never
reissues an ambiguous delete. Guest command and file roundtrips are explicitly
reported as `PASS`, `FAIL` or `NOT_PROVEN`; this tool does not qualify the
Cathedral API, website or customer path. A failed or unknown cleanup must be investigated by
the delete operation key. The 120-second guest TTL is the final safety bound,
not proof of successful cleanup.

## Temporary HTTPS ingress for the isolated proof

For a short remote adapter proof, build the standard-library proxy for the
x86-64 host and copy the binary there. It binds only `127.0.0.1:13000`
(control API) and `127.0.0.1:13002` (envd guest traffic):

```sh
GO111MODULE=off GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -o /private/path/cathedral-ingress ./deploy/cathedral/ingress-proxy.go
/private/path/cathedral-ingress --api-key-file /private/path/seeded-team-api-key
```

The key file must contain the proof install's existing seeded team API key;
keep it private and never paste the key into a command or tunnel URL. The
control listener compares `X-API-Key` and permits only the Cathedral create,
lookup, identity, delete, sandbox detail/timeout, capabilities, and bounded
template-list routes. Admin, dashboard, debug and database routes are absent.
The guest listener accepts only envd process/filesystem/file routes on port
49983 with sandbox routing headers and `X-Access-Token`; client-proxy and the
orchestrator still perform the authoritative per-guest token check. The proxy
does not log bodies, headers, credentials or guest output.

With a separately verified `cloudflared` binary, run two short-lived quick
tunnels in separate host terminals. Each returns its own temporary HTTPS URL:

```sh
cloudflared tunnel --url http://127.0.0.1:13000
cloudflared tunnel --url http://127.0.0.1:13002
```

Use the first URL as `CATHEDRAL_E2B_API_URL` and the second as
`CATHEDRAL_E2B_SANDBOX_URL`. The proxy rewrites the upstream Host to loopback
so client-proxy uses the preserved `E2b-Sandbox-Id` and `E2b-Sandbox-Port`
headers. Stop both tunnels and the proxy when the bounded proof ends; quick
tunnel URLs are ephemeral. This ingress is not an admission gate. Keep
`CATHEDRAL_E2B_ENABLED=false` until that separate gate is approved.
