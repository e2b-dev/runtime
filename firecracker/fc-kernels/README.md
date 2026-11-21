# fc-kernels

## Overview

This project builds custom Linux kernels for Firecracker microVMs from the same kernel sources as the official Firecracker repo, using the configuration files (and optional patches) that live in this repo.

## Prerequisites

- Linux environment (for building kernels)

## Building locally

1. **Configure kernel versions:**
   - Edit `kernel_versions.txt` to specify which kernel versions to build (one per line, e.g. `6.1.158`).
   - Place the corresponding config(s) in `configs/x86_64/<version>.config` and `configs/arm64/<version>.config`.
   - (Optional) Drop `*.patch` files into `patches/<version>/` to apply on top of the upstream tree before build.

2. **Build:**
   ```sh
   make build              # builds all versions in kernel_versions.txt for x86_64
   make build-arm64        # same, for arm64
   ./build.sh 6.1.158      # build a single version (x86_64)
   ./build.sh 6.1.158 arm64
   ```

   Output: `builds/vmlinux-<version>/<arch>/vmlinux.bin` where `<arch>` is `amd64` or `arm64` (Go/OCI convention). For x86_64 a legacy copy is also placed at `builds/vmlinux-<version>/vmlinux.bin`.

## Adding a patch release

A version is built from the newest `microvm-kernel*<version>-*.amzn*` tag of [amazonlinux/linux](https://github.com/amazonlinux/linux), or from the newest general `kernel*<version>-*.amzn*` tag when the microvm line has none, so those tags are where the available patch releases come from. Upstream tags the general build first and the microvm one days later, when at all; the weekly bump proposes from either. A tag upstream renamed with a `-dontuse` suffix is never built. Match the prefix loosely: upstream puts the series there on some tags (`microvm-kernel-6.1.158-15.288.amzn2023`, but `microvm-kernel6.18-6.18.36-71.136.amzn2023`), and a pattern that assumes the hyphen quietly hides the newest releases of a series.

```sh
git ls-remote --tags https://github.com/amazonlinux/linux | grep -E 'refs/tags/(microvm-)?kernel[^ ]*-6\.1\.[0-9]+-[^ ]*\.amzn2[0-9]*$'
```

A new version needs its own config, seeded from the version it supersedes and then settled against the tree it will be built from:

```sh
for arch in x86_64 arm64; do cp configs/$arch/6.1.158.config configs/$arch/6.1.177.config; done
cp -r patches/6.1.158 patches/6.1.177          # only if that version carries patches
sudo env CONFIG_ONLY=1 ./build.sh 6.1.177      # root: build.sh installs its build dependencies
sudo env CONFIG_ONLY=1 ./build.sh 6.1.177 arm64
sudo chown -R "$(id -u):$(id -g)" configs      # the resolved configs come back root-owned
```

`CONFIG_ONLY=1` stops after `olddefconfig` and writes the resolved config back to `configs/<arch>/<version>.config`, so the symbols the newer tree added are recorded and reviewable rather than silently taking upstream defaults at build time. It applies `patches/<version>/` on the way, which is where a carried patch that no longer applies shows up, and it sets `CONFIG_BUILD_SALT` from the tag being built, which `olddefconfig` leaves alone: a seeded config would otherwise ship the version it was seeded from. Run it per architecture on that architecture's machine: Kconfig probes the compiler, so a config resolved cross is not the one a native build resolves.

Then add the version to `kernel_versions.txt` and build.

`make build` builds every version in the file, so keep the list to the versions you still want built. Dropping a pin is safe: published artifacts are never overwritten or deleted, and a template records the kernel version it was built with.

## Flavours

A flavour is the same upstream tree and patches built under a different config, named `<stem>-<flavour>`: `6.1.190-android` is `6.1.190` with binder, DMA-BUF heaps, Android's networking options, a small patch that answers Android's ashmem ioctls on memfd files instead of an ashmem driver, and PSI (pressure stall information) on by default, so a sandbox can run Android in a container. The other configs build PSI in but leave it off unless the guest cmdline says `psi=1`; Android's low-memory killer daemon needs `/proc/pressure`, and without it the UI never comes up. `build.sh` takes the upstream tag and `patches/<stem>/` from the stem and everything else (the config, the output directory, and so the published name a template selects) from the full name.

A flavour is defined once, for every stem: `flavours/<arch>/<flavour>.config` holds only the lines it changes in a stem's config, and `patches/<flavour>/` the patches it carries on top of the stem's. When `patches/<stem>-<flavour>/` holds patches, they replace `patches/<flavour>/` for that one build, for a stem the shared patches no longer apply to. `scripts/flavour_fragment.py` derives a fragment from a flavoured config and the config it was seeded from; each fragment's header says which pair it came from.

What builds is still one full, resolved config per pin, `configs/<arch>/<stem>-<flavour>.config`, reviewed and committed like any other. When that file does not exist yet, `CONFIG_ONLY=1` seeds it from `configs/<arch>/<stem>.config` with the fragment appended (Kconfig takes a symbol's last assignment, so the fragment wins), resolves it with `olddefconfig` as for any version, and fails without writing it if any fragment line did not survive unchanged: `olddefconfig` quietly turns off a symbol whose dependencies the tree no longer meets, which would ship the flavour without what it exists for. A config that exists is re-settled like any version, and the fragment is checked again. A build never seeds.

So when the bump adds a stem, it adds `<stem>-android` next to it and seeds it the same way, per architecture on that architecture's machine:

```sh
sudo env CONFIG_ONLY=1 ./build.sh 6.1.190-android
sudo env CONFIG_ONLY=1 ./build.sh 6.1.190-android arm64
sudo chown -R "$(id -u):$(id -g)" configs
```

A fragment line that does not survive is a flavour change to review, not a line to drop. A stem is never seeded from a flavour, and older flavoured pins (`6.1.158-android`) keep building from their own committed configs. To change the flavour itself, edit the fragment and re-seed the newest flavoured config (remove it and run `CONFIG_ONLY=1` again).

## New kernel in E2B's infra
_Note: these steps should give you a new kernel on your self-hosted E2B using https://github.com/e2b-dev/infra_

- Build the kernel from the new config or patch.
- Update `DefaultKernelVersion` in [packages/shared/pkg/featureflags/flags.go](https://github.com/e2b-dev/infra/blob/main/packages/shared/pkg/featureflags/flags.go) if you changed the kernel version. It is the fallback: `DEFAULT_KERNEL_VERSION` in the environment overrides it, and a `build-kernel-version` feature flag overrides both.
- Build and deploy `api`.

## Architecture naming

Output directories use Go's `runtime.GOARCH` convention (`amd64`, `arm64`) so they match the infra orchestrator's `TargetArch()` path resolution. The build-time variable `TARGET_ARCH` (`x86_64`, `arm64`) is only used internally for config paths and cross-compilation flags.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../../LICENSE) for details.
