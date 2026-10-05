# E2B Embed with Docker Compose

Runs the whole of Embed, the control plane and a real Firecracker
orchestrator alike, directly on one Linux host you own: no Terraform, no cloud
account, no wrapper scripts, and `docker compose up` as the whole interface.
It is a single-machine package rather than a deployment pattern;
the hub is [`../README.md`](../README.md).

## Requirements

- Linux x86-64 or arm64 with KVM and a 4 KiB-page kernel (Ubuntu's default
  on both), on a host you own and may mutate: bare metal, or a VM created with
  nested virtualization enabled (on GCE, `--enable-nested-virtualization`; on
  EC2, `--cpu-options NestedVirtualization=enabled` on an Intel type that
  offers it, such as `m8i`; on Apple silicon, a Lima or other
  Virtualization.framework VM with nested virtualization on, which needs an
  M3 or newer and macOS 15 or newer). x86-64
  is what the guides were written and tested on. arm64 is verified end to
  end on bare metal: on 2026-09-14 a bare-metal arm64 host (a GCE `c4a` metal
  instance, Ubuntu 26.04, kernel 7.0) reached `ready` in under two minutes
  and passed the smoke test, sandbox create, pause and resume, template
  builds from arm64 images and 64 concurrent sandboxes. arm64 needs kernel
  6.10 or newer: restoring a sandbox needs userfaultfd write-protect, which
  arm64 gained in Linux 6.10, so on Ubuntu 24.04's stock 6.8 kernel the stack
  is healthy but every sandbox start fails with `Failed to UFFD object` until
  `linux-generic-hwe-24.04` is installed. The nine pinned images are
  published for both architectures, and `fetch-artifacts` verifies the arm64
  orchestrator and envd against the `.sha256` their release writes beside
  the object. A pin with no such object still stops with a `FIX:` line
  naming it.
- Ubuntu 24.04 is the recommended host: kernel 6.8 or newer (6.10 or newer on
  arm64, above), glibc 2.34 or newer (the released orchestrator's floor),
  cgroup v2 (systemd's default),
  and `iptables`, `rsync`, `e2fsprogs` and `iproute2` installed. Ubuntu
  server has all four but not `python3-venv`, which [Try it](#try-it)
  needs. Refresh the index first, or a package that is genuinely missing
  fails to fetch:
  `sudo apt-get update && sudo apt-get install -y iptables rsync e2fsprogs iproute2 python3-venv`.
- `/dev/kvm` and `/dev/net/tun` present. On a VM, `/dev/kvm` means nested
  virtualization is enabled for it; on GCE, stop the VM, run
  `gcloud compute instances update <vm> --enable-nested-virtualization`, and
  start it again; on EC2, stop the instance, run
  `aws ec2 modify-instance-cpu-options --instance-id <id> --nested-virtualization enabled`,
  and start it again. Without either device, `up` stops at `preflight` with a
  `FIX:` line and nothing that needs KVM is started.
- Docker Engine 27 or newer with Compose 2.24 or newer, and your user in the
  `docker` group. Docker's own apt repository is the simplest way to get
  both: `curl -fsSL https://get.docker.com | sh && sudo usermod -aG docker
  $USER`, then log in again. Ubuntu 24.04's `docker-compose-v2` package also
  clears the Compose floor. `docker version` and `docker compose version`
  show what you have; an
  older Compose rejects the compose file with an error on `required:` before
  anything starts.
- 12 GiB RAM recommended and 20 GiB free disk. `HUGEPAGES=2048` reserves
  4 GiB of that RAM for sandboxes, and `preflight` does not check RAM, so on
  a smaller host the first signal is `host-setup` failing with `FIX: give the
  host more memory (12 GiB recommended) or lower HUGEPAGES`. Lower
  `HUGEPAGES` rather than assuming 8 GiB is always enough. Size it by the
  sandboxes you will run at once: every sandbox takes its guest memory from
  this reservation as it touches pages, up to its full size (256 of the 2 MiB
  pages for a 512 MiB `base` sandbox, about 90 while idle), so the default
  carries eight sandboxes at full memory or about twenty idle ones. Past
  that the stack stays healthy and each create fails with `500: Failed to
  place sandbox` (the same 500 an exhausted NBD pool gives; the orchestrator
  log tells them apart) while `docker compose logs orchestrator` shows `uffd
  process exited: failed to wrap memfd: mmap memfd: cannot allocate memory`.
  Set a
  larger `HUGEPAGES` in `.env` and run `up` again: `host-setup` re-applies
  the sysctl on every start and the orchestrator needs no restart.
- Outbound HTTPS to Docker Hub, `us-docker.pkg.dev`, `storage.googleapis.com`,
  `github.com` and `raw.githubusercontent.com`. No account and no token:
  every image and binary the stack pulls is public.

## Install

Two files, nothing else:

```bash
mkdir e2b && cd e2b
curl -fsSL --remote-name-all "https://raw.githubusercontent.com/e2b-dev/runtime/main/embed/compose/{compose.yaml,.env}"
docker compose up -d --wait
```

The first run checks the host, prepares it, downloads the Firecracker
artifacts (about 60 MB), pulls the pinned images, migrates and seeds the
databases and builds the `base` template. It exits 0 only when the base
template is ready, about 2 minutes on an 8-vCPU / 32 GiB VM; a second `up`
takes about a minute, the healthcheck start periods. Smaller hosts take
longer. Nothing is rolled back if it fails, and nothing needs to be: see
[If `up` fails](#if-up-fails).

## Try it

Put this install's three SDK variables and the dashboard URL, its own team
API key included, in your shell:

```bash
eval "$(docker compose exec ready cat /run/e2b/sdk.env)"
```

Open the dashboard at `$E2B_DASHBOARD_URL` (http://localhost:3001) and paste
`$E2B_API_KEY` into its key form. On the machine itself, or through the
tunnel in [Ports](#ports), that is all. From another machine without a
tunnel, the browser also has to reach sandbox traffic on port 3002 at an
address it can open: set `E2B_DASHBOARD_HOST=<this host's address>` in
`.env` and run `docker compose up -d --wait` again, which recreates only the
`dashboard` service.

Install the SDK in a virtual environment, which is what keeps it off the
system Python that Ubuntu's `pip` refuses to write to. On Ubuntu `venv`
comes from `python3-venv`: install it first, which the apt line in
[Requirements](#requirements) does, or this fails with `ensurepip is not
available`.

```bash
python3 -m venv .venv && .venv/bin/pip install e2b==2.46.0
```

Then create a sandbox. Save this and run it with `.venv/bin/python`:

```python
from e2b import Sandbox
sbx = Sandbox.create("base")
print(sbx.commands.run("echo hello from the sandbox").stdout)
sbx.kill()
```

Or run the packaged smoke test, which does the same with the JavaScript SDK
in a container and then reaches a port inside the sandbox through
client-proxy:

```bash
docker compose --profile test run --rm smoke
```

## Remove

```bash
docker compose down    # stop; running sandboxes end, the data and host state stay, so the next up is fast
docker compose down -v # also the databases and the team key; the next up rebuilds base
docker compose --profile purge run --rm host-teardown  # undo the host setup
```

The purge is valid only after `down -v`. If you purged after a plain `down`,
run `down -v` before the next `up`, or the database still lists templates
whose files the purge removed.

## Reference

### Everyday commands

| Command | Effect |
|---------|--------|
| `docker compose up -d --wait` | start (or reconcile) everything and wait until the `base` template exists |
| `docker compose ps` | service status; `ready` running means the stack is usable |
| `docker compose logs -f api orchestrator` | follow logs |
| `docker compose logs ready` | the four `export` lines, this install's team API key and dashboard URL included |
| `docker compose --profile test run --rm smoke` | run the SDK smoke test in a container |

One stack per host; there are no VM-name or port knobs. `E2B_DASHBOARD_HOST`
(default `localhost`) is the address a browser uses for sandbox traffic on
port 3002 from the dashboard; set it in `.env` to this host's address when
you open the dashboard from another machine without a tunnel. `HUGEPAGES`
(default 2048) and `PF_MIN_FREE_GIB` (default 20) can be lowered in `.env` or
the environment for small hosts and CI. `NBDS_MAX` (default 64) is the number
of NBD devices `host-setup` asks the kernel for. Every running sandbox holds
one and a template build in flight holds one or two more, so the loaded
module's `nbds_max` is the ceiling on concurrent sandboxes: with every device
taken, a create waits for one until the api gives up (`500: Failed to place
sandbox: sandbox creation failed` or `504: placement timed out`), and the
creates queued behind it in a burst get `503: not enough capacity`, the node's
refusal to have more than three sandboxes starting at once. A module already
loaded with more devices than `NBDS_MAX` is accepted and raises the ceiling.
The module takes the number only when it loads, so set `NBDS_MAX` in `.env`
before the first `up`, or with the stack down: once `ls /sys/block/nbd*/pid`
prints nothing, `modprobe -r nbd` on the host and `up` again; `host-setup`
stops with a `FIX:` line when the loaded module has fewer devices than asked.
The orchestrator also refuses the 201st running sandbox (`max number of
running sandboxes on node reached (200)`), a limit this stack cannot raise.
`NBD_POOL_SIZE` (32) is the orchestrator's warm buffer of claimed devices,
not a ceiling, and stays below `NBDS_MAX` on purpose: at parity the pool's
refill loop spins and logs `no free slots` every few seconds on an idle host.
`FORCE_REBUILD=1`, settable in the
same two places, forces `base-template` to rebuild even when a `base` row
already shows `ready`, for a stale or broken template. `TEAM_API_KEY`,
`ADMIN_TOKEN` and `SANDBOX_ACCESS_TOKEN_HASH_SEED` are under Secrets below,
and `E2B_OTEL_COLLECTOR_GRPC_ENDPOINT` under Telemetry.

An optional, git-ignored `env/api.local.env` can add api variables. Create
the `env/` directory next to `compose.yaml` yourself, since the two-file
install does not ship one. It cannot override a key already set in the api
`environment:` block, because Compose gives `environment:` precedence over
`env_file`, so change those in `compose.yaml`.

### What runs where

The reference's [What runs where](../docs/REFERENCE.md#what-runs-where) has the services.
This is what they do to the host, which is why it should be a dedicated host
or VM: on every `up`, `host-setup`

- loads the `nbd` (with `nbds_max=NBDS_MAX`), `tun` and `kvm` kernel modules;
- writes `/etc/modules-load.d/e2b.conf`, `/etc/modprobe.d/e2b-nbd.conf`,
  `/etc/udev/rules.d/97-nbd-device.rules` and `/etc/sysctl.d/90-e2b.conf`,
  which also raises `net.ipv4.tcp_max_syn_backlog` and `vm.max_map_count`;
- adds one iptables mangle rule, the TCP MSS clamp;
- reserves the hugepages;
- creates `/var/lib/e2b`, `/fc-*`, `/orchestrator` and `/var/run/netns`.

`host-teardown` removes the files, the directories, the hugepage reservation
and the iptables rule, and unloads `nbd` when no device is still connected
(it says `nbd left loaded` when one is). It removes the iptables rule only if
`host-setup` was the one that added it: a host that already had an identical
TCP MSS clamp keeps its own firewall configuration, and the purge says
`MSS clamp: not added by host-setup, kept`. `kvm` and `tun` are left loaded
on purpose, because other software on the host may need them, and the two
extra sysctls stay at their raised values; all of that reverts on the next
reboot.

After a host reboot, run `docker compose up -d --wait` once. The long-running
services return on their own (`restart: unless-stopped`), but the TCP MSS
clamp and `/var/run/netns` are the two host mutations that do not persist
across a reboot (the clamp is an iptables rule, `/var/run/netns` lives on a
tmpfs), and `host-setup` recreates both idempotently on that run.

### If `up` fails

Nothing is rolled back and nothing needs to be: every step is idempotent. Read
the `FIX:` line in the failing service's log (`docker compose logs preflight`,
`host-setup`, `fetch-artifacts` or `base-template`), fix what it names, and
run `docker compose up -d --wait` again; the steps that already succeeded are
skipped.

A failed `preflight` has changed nothing on the host. A failed `host-setup`
leaves behind whatever it had already done, all of it idempotent: the config
files, the loaded modules and, depending on where it stopped, the reserved
hugepages (`HUGEPAGES` times 2 MiB, 4 GiB by default) and the TCP MSS rule,
which `host-teardown` or a reboot releases. A failed download leaves only fully verified files behind.
A failed template build leaves a template row marked `error` that the next
build replaces; the `base-template` step already runs one Firecracker build VM
through the orchestrator, which the same SIGTERM and `host-teardown` sweep
cleans up like any other sandbox. `ready` is the marker that the stack is
usable for the sandboxes you create.

To undo everything instead:

```bash
docker compose down -v && docker compose --profile purge run --rm host-teardown
```

### Ports

The thirteen ports and what each is for are in the hub. Compose binds them on
every host interface: let trusted clients reach 3000, 3001 and 3002, and make
sure the other ten are reachable on no address the host holds, its own public
one included, not only from the network edge.

From another machine, tunnel rather than open, and use the same four
variables with `127.0.0.1` in place of `localhost`. Template builds with
`copy()` steps upload through the orchestrator's port 5008, which stays closed
to the network, so tunnel that too; the upload URL the stack hands out
(`http://127.0.0.1:5008/...`) is then valid unchanged.

```bash
ssh -N -L 3000:127.0.0.1:3000 -L 3001:127.0.0.1:3001 -L 3002:127.0.0.1:3002 -L 5008:127.0.0.1:5008 <user>@<host>
```

### Secrets

The team API key is this install's own. The seed generates it on the first
`up` and keeps it in the `seed-state` volume (`/run/e2b/team-api-key`), where
a later seed run finds it again and `base-template`, `smoke` and `ready` read
it; `docker compose logs ready` prints it and
`docker compose exec ready cat /run/e2b/sdk.env` returns it as shell exports.
`down -v` drops that volume together with the databases, so the next `up`
generates a new key and there is never a key without its database or a
database without its key.

To choose or rotate the key, set `TEAM_API_KEY` (`e2b_` plus at least 32 hex
characters) in `.env` and run `up`. To rotate to a fresh generated key, remove
the file and run `up`:

```bash
docker compose run --rm --no-deps --entrypoint rm seed /run/e2b/team-api-key
docker compose up -d --wait
```

Either way the seed inserts the new key and revokes its earlier one, which
stops working within five minutes (the api caches team lookups in Redis for
that long). Within a few seconds `ready` re-renders `/run/e2b/sdk.env` and
`docker compose logs ready` prints the new key: `up` re-runs the seed but does
not recreate `ready`, so `ready` watches the key file instead. Keys created
any other way are never touched, and nothing in the two shipped files carries
a key ([`../tests/team-api-key.bats`](../tests/team-api-key.bats)).

The dashboard keeps the key you paste in an httpOnly browser cookie for a
year, not marked Secure because the dashboard is served over plain http;
sign-out clears it, and rotating the key signs every browser out.

The api's own two secrets, `ADMIN_TOKEN` and
`SANDBOX_ACCESS_TOKEN_HASH_SEED`, are this install's own in the same way. The
`api-secrets` one-shot generates both on the first `up` and keeps them in the
`seed-state` volume as `/run/e2b/api.env`, a file only root can read, as
`GEN_ADMIN_TOKEN` and `GEN_SANDBOX_ACCESS_TOKEN_HASH_SEED`; a later run finds
them again and the entrypoints of the api and dashboard-api read them;
nothing prints them, and `down -v` drops them with the databases. The
reference's [Secrets](../docs/REFERENCE.md#secrets) says what the two are
for.

To choose your own instead, put them in `.env` before the first `up`. A value
there wins over the generated one, one variable at a time, and the install is
still two files:

```bash
printf 'ADMIN_TOKEN=%s\nSANDBOX_ACCESS_TOKEN_HASH_SEED=%s\n' "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" >> .env
```

To rotate the generated pair, remove the file, write a new one and recreate
the api on it:

```bash
docker compose run --rm --no-deps --entrypoint rm api-secrets /run/e2b/api.env
docker compose run --rm --no-deps api-secrets
docker compose up -d --no-deps --force-recreate api dashboard-api
```

`--no-deps` throughout: the two one-shot runs need no other service, and
recreating the api's dependencies would restart the orchestrator and end every
running sandbox. The new hash seed invalidates the traffic and envd tokens of
the `secure` sandboxes that survive the api's restart, and the new admin token
invalidates every admin client. A value pinned in `.env` is not rotated by
this: change the line and recreate the api and dashboard-api.
`docker compose exec api env` shows both empty: the entrypoint fills them in
for the api process only. Read them from the volume instead:
`docker compose run --rm --no-deps --entrypoint cat
api-secrets /run/e2b/api.env`.

### Telemetry

Nothing is exported by default, so the dashboard's monitoring charts and the
SDK's `getMetrics` (`get_metrics` in Python) stay empty. To run the built-in
OpenTelemetry collector, which keeps those metrics in this stack's
ClickHouse, uncomment the two lines that ship commented at the end of the
optional block in `.env`:

```bash
E2B_OTEL_COLLECTOR_GRPC_ENDPOINT=127.0.0.1:4317
COMPOSE_PROFILES=otel
```

Then run `docker compose up -d --wait`, which starts `otel-collector` and
recreates the four services that export to it; recreating the orchestrator
ends the sandboxes running on it. While the collector runs,
`curl -s 127.0.0.1:13133` answers `Server available`, and once a sandbox has
been running for a few seconds its rows show up:
`docker compose exec clickhouse clickhouse-client --user clickhouse --password clickhouse --query "SELECT count() FROM sandbox_metrics_gauge"`.

For a collector of your own, uncomment only the first line and put that
collector's address in it. To turn the built-in one off again, comment both
lines out, run `docker compose rm -sf otel-collector` and then
`docker compose up -d --wait`. The reference's
[Observability](../docs/REFERENCE.md#observability) says what each choice
gives you and how to forward traces and logs as well.

### Upgrading

Download `compose.yaml` and `.env` again, with the same command as
[Install](#install), and run `docker compose up -d --wait --pull always`.
Before that `up`, carry the optional lines you had set, such as
`E2B_DASHBOARD_HOST` or the two under [Telemetry](#telemetry), over into the
new `.env`: it ships them commented again, and without the Telemetry pair the
services stop exporting. The `.env` you get pins the stack images that go with
those files, so the two always move together. To take a particular commit
rather than the newest, put the commit in place of `main` in the two URLs, as
in `raw.githubusercontent.com/e2b-dev/runtime/<commit>/embed/compose/…`; the raw
host ignores `?ref=`. Binary checksums live inside the tools
image, so bumping a Firecracker artifact means taking newer files rather than
editing anything on the host. Running sandboxes end when the orchestrator
restarts; a single machine has nowhere to drain them to.

### Known limitations

- Nothing on this stack authenticates a network peer except api's own API key
  and admin token check. Thirteen ports bind every host interface, and the
  orchestrator's 5008 in particular is an unauthenticated control API that
  creates and kills sandboxes. Expose 3000, 3001 and 3002 to trusted clients if
  you must, and keep the other ten unreachable on every address the host holds,
  its own public one included.
- The team API key rotates only through the seed (set `TEAM_API_KEY`, or
  remove the key file, then `up`), not through an API call, and the old key
  keeps working for up to five minutes afterwards.
- With an api release that reads `PAUSE_REFUSAL_RESTORE` (the pin here
  predates it), a pause the node refuses — it is still persisting the
  sandbox's parent — leaves the sandbox running and answers 503; retry it.
  Until then a refused pause ends the old way, with the sandbox killed and a
  500. A full disk is not a refusal in the pinned orchestrator: that pause
  fails after it has started and ends the same old way.
- Container logs are capped: every service keeps at most five 50 MiB log
  files (`x-logging` in `compose.yaml`), so `docker compose logs` shows about
  the last 250 MiB per service and older lines are gone. Without the cap a
  service that fails on a full disk writes error output faster than anything
  frees space and holds the disk at 100%. Raise the cap in your copy for a
  longer history.
- Expected log noise, all harmless: an OIDC warning from api at startup,
  because no identity provider is configured, and, every ten seconds from both
  api and the orchestrator, `failed to upload metrics: exporter export
  timeout`, because no OpenTelemetry collector is configured and the endpoint
  is empty. That line stops once `E2B_OTEL_COLLECTOR_GRPC_ENDPOINT` names a
  collector that answers ([Telemetry](#telemetry)).
  dashboard-api also logs `ADMIN_AUTH_PROVIDER_CONFIG is not configured` once
  at startup; the management endpoints it guards are not used here.
- To run without the dashboard, remove the `dashboard` and `dashboard-api`
  services from your copy of `compose.yaml`, and `dashboard` from `ready`'s
  `depends_on`; the rest of the stack runs.

### Troubleshooting

- **Logs missing for a sandbox or a build.** `docker compose logs vector`
  shows insert errors (ClickHouse down, or a row it refused), and
  `docker compose exec clickhouse clickhouse-client --user clickhouse --password clickhouse --query "SELECT count() FROM sandbox_logs"`
  shows whether rows arrive at all. Vector buffers and retries while
  ClickHouse is down; the api's log endpoints fail during that time.
- **`preflight` failed.** Read the `FIX:` line in
  `docker compose logs preflight`. The most common cause is a missing
  `/dev/kvm`: on a VM, nested virtualization was off when it was created.
- **`up --wait` failed at `base-template`.** Its log ends with `FIX: read the
  api and orchestrator logs for the template-manager error, then start the
  stack again`, and `docker compose logs orchestrator` has the
  template-manager error itself. A line saying `orchestrator not
  registered with the api yet, retrying` is normal for the first seconds after
  a start.
- **The smoke test failed.** Its output ends with `FIX: read the api,
  orchestrator and client-proxy logs`; read those three logs. If the last line
  is instead `FIX: the exposed port could not be reached; read the
  client-proxy and orchestrator logs`, the sandbox was created and ran the
  command, but the listener the test started on port 8080 did not answer
  through client-proxy within 15 seconds. If it is `FIX: the sandbox could not
  be killed; read the api and orchestrator logs`, the sandbox was created and
  ran the command but the stack could not end it. That still fails the smoke
  test, and the sandbox may still be running.
- **`Stopping orchestrator, success: false`** and `orchestrator-launch:
  orchestrator exited with status 1` at the end of the orchestrator's log
  after `docker compose stop` are the binary's normal SIGTERM path, not a
  crash; only a line starting `FIX:` means an actual failure. Once `down` has
  removed the container there is no log left to read.
- **Template builds fail inside the sandbox with `Hash Sum mismatch`, `Remote
  end closed connection` or `Unable to fetch some archives`** while small
  downloads work: the MSS clamp is missing. This is expected on every GCE VM,
  whose NIC MTU is below 1500, and can also happen after a reboot.
  `sudo iptables -t mangle -S FORWARD | grep TCPMSS` must show one rule; rerun
  `docker compose up -d --wait` to have `host-setup` add it.
- **Sandboxes fail to start although the orchestrator is healthy.** Confirm
  that the `orchestrator` service has `cgroup: host` in `compose.yaml` and
  that its launcher's `nsenter` call includes `-C`, in
  [`scripts/orchestrator-launch.sh`](scripts/orchestrator-launch.sh) rather
  than in `compose.yaml`; the symptom is `fork/exec /usr/bin/unshare: no such
  file or directory` in `docker compose logs orchestrator`.
- **A missing or incomplete `.env` beside `compose.yaml`** is the likeliest
  two-file mistake. Compose stops before starting anything and names every
  variable it could not resolve:

  ```
  error while interpolating services.api.image: required variable
  E2B_API_IMAGE is missing a value: fetch the .env that ships beside
  compose.yaml (README, Install)
  ```

  Fetch `.env`, or the lines it is missing, with the Install command above and
  retry. A partial `.env` fails the same way, naming only what is missing.
- **`/sys/module/nbd/parameters/max_part` shows 31** for the configured 16.
  That is the kernel's rounding, and it is expected.
- **`host-teardown` refuses to run while the stack is up**, with `FIX: run
  docker compose down -v first, then docker compose --profile purge run --rm
  host-teardown`.
- **`host-teardown` said `nbd left loaded`.** A device was still connected. It
  unloads on reboot, or run `sudo modprobe -r nbd` once `pgrep firecracker`
  prints nothing.
- **Stuck stack.** `docker compose down -v && docker compose --profile purge
  run --rm host-teardown && docker compose up -d --wait` recreates everything
  in a few minutes once the images are cached.

### Developing

This directory holds the two files the Install downloads together with the
scripts, configs and Dockerfiles behind the images they pin. From a checkout,
with the package root as the working directory, the Python smoke test runs
directly:

```bash
python3 -m venv .venv && .venv/bin/pip install e2b==2.46.0
.venv/bin/python compose/smoke/smoke_test.py
```

It needs the `python3-venv` package on Ubuntu, and
[`smoke/smoke_test.py`](smoke/smoke_test.py) is not shipped in either stack
image, so it is not reachable from a two-file install. The stack is tested
with the 2.46 SDK line (`e2b@2.46.1`, `e2b==2.46.0`), the versions the
container smoke test and the images pin.

Changing an E2B component pin means editing `.env` and, for a binary, the
checksums in [`scripts/fetch-artifacts.sh`](scripts/fetch-artifacts.sh); then
`make images` builds the three stack images locally under their pinned tags,
and `docker compose up -d --wait` **without** `--pull always` uses the images
just built rather than the published ones. The reference's
[Developing](../docs/REFERENCE.md#developing) covers `make lint`, `make test`,
`make sync-configs` and `make stores-check`, all of which run from the package
root.
