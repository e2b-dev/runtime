# Orchestrator

## Commands

**Prerequisite:** Enable NBD module first:

```bash
modprobe nbd nbds_max=4096

cat <<EOH >/etc/udev/rules.d/97-nbd-device.rules
# Disable inotify watching of change events for NBD devices
ACTION=="add|change", KERNEL=="nbd*", OPTIONS:="nowatch"
EOH
udevadm control --reload-rules
udevadm trigger
```

**Prerequisite:** Allocate enough 2MB HugeTLB pages (hugepages) for testing:

```bash
echo 1024 | sudo tee /proc/sys/vm/nr_hugepages   # for 2GB hugepages, adjust as needed
```

### Create Build

Create a new build.

```bash
sudo go run ./cmd/create-build -to-build <uuid> -storage .local-build
```

Flags:

- `-to-build <uuid>` - Output build ID (UUID, required)
- `-from-build <uuid>` - Base build ID for incremental builds
- `-template <id>` - Template ID (default: `local-template`)
- `-storage <path>` - Local path or `gs://bucket` (enables local mode with auto-download of kernel/FC)
- `-sandbox-dir <path>` - Override `SANDBOX_DIR` (the rootfs path baked into the snapshot)
- `-kernel <version>` - Kernel version (default: `vmlinux-6.1.158`)
- `-firecracker <version>` - Firecracker version (default: `v1.14.1_458ca91`)
- `-vcpu <n>` - vCPUs (default: `1`)
- `-memory <mb>` - Memory in MB (default: `512`)
- `-disk <mb>` - Disk in MB (default: `1000`)
- `-hugepages` - Use 2MB huge pages (default: `true`, set `false` for 4KB pages)
- `-start-cmd <cmd>` - Start command
- `-ready-cmd <cmd>` - Ready check command

> You can use the `$(uuidgen)` command to generate a random UUID.
>
> If you are using Mise, you can run the command as `sudo $(which go) run ./cmd/create-build -to-build <uuid> -storage .local-build`.

---

## Service status

`ServiceInfo` reports one of six statuses. `Healthy` and `NotReady` (the startup hold until the edition's egress answers ready) are the process's own; `ShuttingDown` is entered only by the shutdown and is terminal. `Draining`, `Unhealthy` and `Standby` are set by an operator or a sizer through `ServiceStatusOverride`, which the API exposes as its admin `POST /nodes/{nodeID}` route.

- **Owner.** A `Standby` override may carry an owner: at most 63 characters of `[a-z0-9-]`, starting and ending alphanumeric. While an owner is recorded, a `Standby` under another owner is refused with `FailedPrecondition`; a `Healthy`, `Draining` or `Unhealthy` from anyone applies and clears the owner, so a person's actions work as before and the owner says whose park it is and nothing more; `force` applies the carried status and owner regardless. The response reports the owner as `status_owner`, and the build advertises `capabilities: ["park-owner"]` beside its labels, never among them.
- **Persistence.** A `Standby` is written to `$ORCHESTRATOR_BASE_PATH/service-status.json` (status, owner, change time) and restored at start, so a parked node stays parked across a restart until a `Healthy` carrying its owner arrives. The file is removed when the status leaves `Standby`; a shutdown keeps it, and an unreadable file is logged and ignored. A park also outlives the startup hold: a release before the node's egress is ready lands on `NotReady` and becomes `Healthy` once it is.
- **Create refusal.** `Create` answers `ResourceExhausted` while the status is not `Healthy`, with a `NODE_NOT_ACCEPTING_SANDBOXES` detail that tells it from a full node; the API places the sandbox on another node and leaves this one out of its next sample, while a capacity refusal is retried on the same node as before.
- **Shutdown bound.** Once the sandboxes are drained, every closer runs under one 30 s deadline and the final wait for the service goroutines is bounded by another 30 s. A closer or wait cut at its deadline is logged with `forced=true` and makes the process exit 1, as a failing closer does.

The dummy orchestrator (`cmd/dummy-orchestrator`) runs the same status machine; `STATE_DIR` gives it the record file (the directory is created when missing) and `MAX_SANDBOXES` its sandbox limit.

---

## Architecture (ARM64) Support

The orchestrator supports both `amd64` (x86_64) and `arm64` (aarch64) architectures. Architecture is detected automatically via `runtime.GOARCH` at compile time.

### Architecture naming convention

This project uses **Go/Docker/Debian naming** (`amd64`/`arm64`) for architecture directories in binary paths and GCS buckets:

| Convention | x86_64 name | ARM64 name | Used by |
|------------|-------------|------------|---------|
| **Go/Docker/Debian** | `amd64` | `arm64` | This repo, Docker, `dpkg --print-architecture` |
| Linux/GNU | `x86_64` | `aarch64` | `uname -m`, kernel Makefiles |

Binary paths follow the `{version}/{arch}/` layout:

```
# Firecracker (GCS bucket or FIRECRACKER_VERSIONS_DIR)
fc-versions/v1.12.1_210cbac/amd64/firecracker
fc-versions/v1.12.1_210cbac/arm64/firecracker

# Kernels (GCS bucket or HOST_KERNELS_DIR)
kernels/vmlinux-6.1.158/amd64/vmlinux.bin
kernels/vmlinux-6.1.158/arm64/vmlinux.bin
```

> **Note:** The [fc-kernels](https://github.com/e2b-dev/fc-kernels) repo currently uses `x86_64` instead of `amd64` for its directory names. This will be aligned in a follow-up change.

### ARM64-specific behavior

- **SMT** is disabled (ARM processors don't support simultaneous multi-threading)
- **CPU detection** uses fallback values since `gopsutil` doesn't populate Family/Model on ARM64
- **OCI platform** is set to the target architecture instead of hardcoded `amd64`
- **Busybox binaries** are fetched per architecture by `scripts/fetch-busybox.sh` and selected at runtime from `<busybox dir>/<version>/<GOARCH>/busybox`
- **Guest kernel command line** drops the x86-only `i8042.*` and `clocksource=kvm-clock` parameters on arm64; the console stays `ttyS0` (Firecracker exposes a 16550-compatible UART on both architectures)

### Cross-architecture deployment

`TARGET_ARCH` is a **runtime** environment variable that overrides the architecture used for path resolution and OCI image pulls. When unset, defaults to the host architecture (`runtime.GOARCH`).

```bash
# Run orchestrator targeting amd64 paths from an arm64 host
TARGET_ARCH=amd64 ./bin/orchestrator

# Or in .env file (read at runtime)
echo "TARGET_ARCH=amd64" >> .env.local
```

`TARGET_ARCH` affects:
- Firecracker and kernel binary path resolution (`{version}/{arch}/...`)
- OCI image platform for container pulls

It does **not** affect:
- Makefile compilation (see `BUILD_ARCH` below)
- Hardware-dependent runtime behavior (SMT detection, CPU info) which always uses the actual host architecture

### Building for ARM64

`BUILD_ARCH` defaults to `amd64`. To build ARM64 binaries:

```bash
# Single command
BUILD_ARCH=arm64 make build-local

# Or set in .env.local for persistent override
echo "BUILD_ARCH=arm64" >> .env.local
```

This applies to all services: orchestrator, envd, client-proxy.

---

## Environment Variables

Automatically set in local mode. Set before running to override:

- `TARGET_ARCH` - Target architecture override (`amd64` or `arm64`; default: host architecture)
- `HOST_ENVD_PATH` - Envd binary path (default: `../envd/bin/envd`)
- `HOST_KERNELS_DIR` - Kernel versions dir (local: `{storage}/kernels`, prod: `/fc-kernels`)
- `FIRECRACKER_VERSIONS_DIR` - Firecracker versions dir (local: `{storage}/fc-versions`, prod: `/fc-versions`)
- `ORCHESTRATOR_BASE_PATH` - Base orchestrator data (local: `{storage}/orchestrator`, prod: `/orchestrator`)
- `SANDBOX_DIR` - Sandbox working dir (default: `/fc-vm`)

## Limitations

- Custom template builds support base images from the declared distro families: **Debian/Ubuntu** (apt), the **RHEL family** — Fedora, CentOS Stream, Rocky, Alma — (dnf/microdnf/yum), **TencentOS Server** 3+ and **OpenCloudOS** 8+ (dnf/microdnf/yum; TencentOS Server 2.x repos lack socat, fuse3, nftables and jq, so its builds fail at package install), **Kylin** Linux Advanced Server V10 (dnf/microdnf/yum; its repos ship `shadow` rather than `shadow-utils`), **Arch** (pacman), and **Alpine** (apk, OpenRC). The distro is resolved from the image's `/etc/os-release` `ID` — never by probing for package managers. Images without an os-release identity (distroless, scratch) or from an undeclared family are rejected fast, with a build-log error naming the reason and the supported families (see `pkg/template/build/phases/base/distro`). Minimal/restricted-repo images may fail provisioning if their repos don't carry the required packages; the failure is surfaced in the build log.
- Sandboxes always boot the kernel E2B supplies, never one from the base image: `/lib/modules` is empty, no kernel modules can be loaded, and SELinux is disabled on the kernel command line (`selinux=0`), so a base image that ships an SELinux policy boots without it. `kernel-devel` from a distro's repos resolves against a kernel that isn't running. **RHEL** (`rhel`, incl. UBI), **Oracle Linux** (`ol`) and **Amazon Linux** (`amzn`) are therefore not accepted even though they are RPM/dnf images — they are chosen for kABI, signed kernel modules and UEK, which this cannot honour. The community rebuilds above are supported because they are chosen for the userland.
