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

A version is built from the newest `microvm-kernel*<version>-*.amzn*` tag of [amazonlinux/linux](https://github.com/amazonlinux/linux), so that is where the available patch releases come from. Match the prefix loosely: upstream puts the series there on some tags (`microvm-kernel-6.1.158-15.288.amzn2023`, but `microvm-kernel6.18-6.18.36-71.136.amzn2023`), and a pattern that assumes the hyphen quietly hides the newest releases of a series.

```sh
git ls-remote --tags https://github.com/amazonlinux/linux | grep -E 'microvm-kernel[^ ]*-6\.1\.[0-9]+-'
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

A flavour is the same upstream tree and patches built under a different config, named `<version>-<flavour>`: `6.1.158-android` is `6.1.158` with binder, DMA-BUF heaps, Android's networking options, a small patch that answers Android's ashmem ioctls on memfd files instead of an ashmem driver, and PSI (pressure stall information) on by default, so a sandbox can run Android in a container. The other configs build PSI in but leave it off unless the guest cmdline says `psi=1`; Android's low-memory killer daemon needs `/proc/pressure`, and without it the UI never comes up. `build.sh` takes the upstream tag and the `patches/` directory from the stem (`6.1.158`) and everything else — the config, the output directory, and so the published name a template selects — from the full name. Seed a flavour's config from its stem's and settle it the same way as a new version (`sudo env CONFIG_ONLY=1 ./build.sh 6.1.158-android`, per architecture). A flavour is a sibling of its stem, not a patch release: the weekly bump neither seeds from it nor bumps it. A flavour may also carry patches of its own under `patches/<version>-<flavour>/`; they apply after the stem's, and a change there rebuilds only that flavour.

## New kernel in E2B's infra
_Note: these steps should give you a new kernel on your self-hosted E2B using https://github.com/e2b-dev/infra_

- Build the kernel from the new config or patch.
- Update `DefaultKernelVersion` in [packages/shared/pkg/featureflags/flags.go](https://github.com/e2b-dev/infra/blob/main/packages/shared/pkg/featureflags/flags.go) if you changed the kernel version. It is the fallback: `DEFAULT_KERNEL_VERSION` in the environment overrides it, and a `build-kernel-version` feature flag overrides both.
- Build and deploy `api`.

## Architecture naming

Output directories use Go's `runtime.GOARCH` convention (`amd64`, `arm64`) so they match the infra orchestrator's `TargetArch()` path resolution. The build-time variable `TARGET_ARCH` (`x86_64`, `arm64`) is only used internally for config paths and cross-compilation flags.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../../LICENSE) for details.
