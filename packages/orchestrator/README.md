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

### Resume Build

Resume a sandbox from a build.

```bash
# Local storage
sudo go run ./cmd/resume-build -from-build <uuid> -storage .local-build -iterations 10

# Remote GCS storage (pass credentials to sudo)
sudo GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_credentials.json \
  go run ./cmd/resume-build -from-build <uuid> -storage gs://bucket -iterations 10

# Pause mode: resume, run command, then snapshot
sudo go run ./cmd/resume-build -from-build <uuid> -to-build <new-uuid> \
  -storage .local-build -cmd-pause "pip install numpy"

# Pause mode: start command through envd, then wait for SIGUSR1 before snapshot
sudo go run ./cmd/resume-build -from-build <uuid> -to-build <new-uuid> \
  -storage .local-build -cmd-signal-pause "python3 /home/user/workspace/job.py"
```

Flags:

- `-from-build <uuid>` - Build ID (UUID) to resume from (required)
- `-to-build <uuid>` - Output build ID (UUID) for pause snapshot (auto-generated if not specified)
- `-storage <path>` - Local path or `gs://bucket` (default: `.local-build`)
- `-sandbox-dir <path>` - Override `SANDBOX_DIR` (the rootfs path baked into the snapshot)
- `-iterations <n>` - Number of iterations, 0 = interactive (default: `0`)
- `-cold` - Clear cache between iterations (cold start each time)
- `-no-prefetch` - Disable memory prefetching
- `-v` - Verbose logging
- `-pause` - Start and immediately pause (create snapshot)
- `-signal-pause <signal>` - Wait for signal before pause (e.g., `SIGTERM`, `SIGUSR1`)
- `-cmd-pause <cmd>` - Execute command in sandbox, then pause on success
- `-cmd-signal-pause <cmd>` - Execute command in sandbox, then wait for `SIGUSR1` before pause

**Pause mode example:**

```bash
# Chain builds with explicit build IDs
sudo go run ./cmd/resume-build -from-build $BUILD1 -to-build $BUILD2 \
  -storage .local-build -cmd-pause "apt install curl"

sudo go run ./cmd/resume-build -from-build $BUILD2 -to-build $BUILD3 \
  -storage .local-build -cmd-pause "pip install requests"

# Start a long-running command through envd, then trigger pause from the host
sudo go run ./cmd/resume-build -from-build $BUILD3 -to-build $BUILD4 \
  -storage .local-build -cmd-signal-pause "python3 /home/user/workspace/job.py"
# In another shell:
ps -ef | grep 'cmd/resume-build' | grep -v grep
sudo kill -SIGUSR1 <resume-build-pid>
```

### Copy Build

Copy a build between storage locations (local or GCS).

```bash
# Local to local
go run ./cmd/copy-build -build <uuid> -from .local-build -to /other/path

# Local to GCS
go run ./cmd/copy-build -build <uuid> -from .local-build -to gs://bucket

# GCS to GCS
go run ./cmd/copy-build -build <uuid> -from gs://bucket1 -to gs://bucket2
```

Flags:

- `-build <uuid>` - Build ID (UUID, required)
- `-from <path>` - Source: local path or `gs://bucket`
- `-to <path>` - Destination: local path or `gs://bucket`
- `-team <uuid>` - Team UUID (if set, prints SQL to populate DB on stdout)
- `-envd-version <version>` - Envd version (required if team provided) — must match the version present in the template
- `-vcpu <n>` - vCPUs (default: `2`)
- `-memory <mb>` - Memory in MB (default: `1024`)
- `-disk <mb>` - Disk in MB (default: `1024`)
- `-tag <name>` - Build assignment tag (default: `default`)

**Print DB seed SQL:**

When `-team` is set, the command reads `metadata.json` from the destination to get `kernel_version` and `firecracker_version`, generates a new env ID, and prints SQL statements (`BEGIN`/`COMMIT` wrapped) to stdout. Pipe directly to `psql` to populate the database:

```bash
go run ./cmd/copy-build -build <uuid> -from .local-build -to gs://bucket \
  -team <team-uuid> -envd-version 0.2.11 -vcpu 2 -memory 1024 -disk 1024 \
  | psql $POSTGRES_CONNECTION_STRING
```

### Mount Build Rootfs

```bash
# Local storage
sudo go run ./cmd/mount-build-rootfs -build <uuid> -storage .local-build -mount /mnt/rootfs

# Remote GCS storage (pass credentials to sudo)
sudo GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_credentials.json \
  go run ./cmd/mount-build-rootfs -build <uuid> -storage gs://bucket -mount /mnt/rootfs
```

Flags:

- `-build <uuid>` - Build ID (UUID, required unless `-empty`)
- `-storage <path>` - Local path or `gs://bucket` (default: `.local-build`)
- `-mount <path>` - Mount path
- `-verify` - Verify rootfs integrity (requires `-mount`)
- `-log` - Enable logging
- `-empty` - Create an empty rootfs instead of loading a build
- `-size <bytes>` - Size of empty rootfs (default: 1GB)
- `-block-size <bytes>` - Block size (default: 4096)

### Inspect Build

Inspect build artifacts (headers and data blocks).

```bash
# Inspect memfile header (default)
go run ./cmd/inspect-build -build <uuid> -storage .local-build

# Inspect rootfs header
go run ./cmd/inspect-build -build <uuid> -storage .local-build -rootfs

# Inspect header + data blocks
go run ./cmd/inspect-build -build <uuid> -storage .local-build -data

# Inspect first 100 data blocks
go run ./cmd/inspect-build -build <uuid> -storage .local-build -data -end 100

# Remote GCS storage
go run ./cmd/inspect-build -build <uuid> -storage gs://bucket -rootfs -data
```

Flags:

- `-build <uuid>` - Build ID (UUID, required)
- `-storage <path>` - Local path or `gs://bucket` (default: `.local-build`)
- `-memfile` - Inspect memfile artifact (default)
- `-rootfs` - Inspect rootfs artifact
- `-data` - Also inspect data blocks (not just header)
- `-start <n>` - Start block index (default: `0`, only with `-data`)
- `-end <n>` - End block index (default: all, only with `-data`)

### Show Build Diff

Show the diff between two builds and preview merge result.

```bash
# Memfile (default)
go run ./cmd/show-build-diff -from-build <uuid> -to-build <uuid> -storage .local-build

# Rootfs
go run ./cmd/show-build-diff -from-build <uuid> -to-build <uuid> -storage .local-build -rootfs

# With visualization
go run ./cmd/show-build-diff -from-build <uuid> -to-build <uuid> -storage gs://bucket -visualize
```

Flags:

- `-from-build <uuid>` - Base build ID (required)
- `-to-build <uuid>` - Diff build ID (required)
- `-storage <path>` - Local path or `gs://bucket` (default: `.local-build`)
- `-memfile` - Inspect memfile artifact (default)
- `-rootfs` - Inspect rootfs artifact
- `-visualize` - Visualize the headers

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

- Custom template builds support base images from the declared distro families: **Debian/Ubuntu** (apt), the **RHEL family** — Fedora, CentOS Stream, Rocky, Alma — (dnf/microdnf/yum), **Arch** (pacman), and **Alpine** (apk, OpenRC). The distro is resolved from the image's `/etc/os-release` `ID` — never by probing for package managers. Images without an os-release identity (distroless, scratch) or from an undeclared family are rejected fast, with a build-log error naming the reason and the supported families (see `pkg/template/build/phases/base/distro`). Minimal/restricted-repo images may fail provisioning if their repos don't carry the required packages; the failure is surfaced in the build log.
- Sandboxes always boot the kernel E2B supplies, never one from the base image: `/lib/modules` is empty, no kernel modules can be loaded, and SELinux is disabled on the kernel command line (`selinux=0`), so a base image that ships an SELinux policy boots without it. `kernel-devel` from a distro's repos resolves against a kernel that isn't running. **RHEL** (`rhel`, incl. UBI), **Oracle Linux** (`ol`) and **Amazon Linux** (`amzn`) are therefore not accepted even though they are RPM/dnf images — they are chosen for kABI, signed kernel modules and UEK, which this cannot honour. The community rebuilds above are supported because they are chosen for the userland.