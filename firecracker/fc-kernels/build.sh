#!/bin/bash
# inspired by https://github.com/firecracker-microvm/firecracker/blob/main/resources/rebuild.sh

set -euo pipefail

# Usage:
#   ./build.sh                            # build all versions in kernel_versions.txt for $TARGET_ARCH
#   ./build.sh <kernel_version> [arch]    # build a single version
#
# arch is one of: x86_64 (default), arm64 (kernel-style names).
# A version may carry a flavour, 6.1.158-android: the upstream tag and the patches
# come from its stem (6.1.158), the config and the output from the full name.
# Output: builds/vmlinux-<version>/<output_arch>/vmlinux.bin where
# <output_arch> is the Go/OCI name (amd64/arm64) used by the orchestrator.
#
# CONFIG_ONLY=1 stops after olddefconfig and writes the resolved config back to
# configs/<arch>/<version>.config instead of building. Use it to settle a config
# copied from another version against the tree it will actually be built from:
# the symbols the new tree added are then in the file, and reviewable, rather
# than silently taking upstream defaults at build time. It needs an explicit
# version — it overwrites the config it reads, and doing that to every pinned
# version at once is never what anyone means.

HOST_ARCH="$(uname -m)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

normalize_arch() {
  case "$1" in
    x86_64)  echo "amd64" ;;
    aarch64) echo "arm64" ;;
    *)       echo "$1" ;;
  esac
}

install_dependencies() {
  local target_arch="$1"
  local packages=(
    bc binutils bison busybox-static cpio curl dwarves flex gcc libelf-dev libssl-dev make patch squashfs-tools tree
  )

  [[ "$target_arch" == "arm64" && "$HOST_ARCH" != "aarch64" ]] && packages+=( gcc-aarch64-linux-gnu )

  apt update
  apt install -y "${packages[@]}"
}

# The tag to build a kernel version from: the microvm flavour when it has one,
# else the general kernel tag, and among those the highest rpm release.
#
# The release is compared numerically (sort -V), because a rebuild bumps only
# that field and a string compare puts 9.100 above 46.372. Selecting by release
# rather than by tag creation date also makes the choice a property of the tag
# names alone, so anything that only has the tag list — an automated bump
# deciding what to propose — resolves the same tag this build will check out.
get_tag() {
  local kernel_version="$1" prefix tag
  for prefix in microvm-kernel kernel; do
    # Loose between the prefix and the version, because upstream puts arbitrary
    # text there (microvm-kernel6.18-6.18.33-65.128.amzn2023). The glob is
    # anchored, so the 'kernel' pass still does not match microvm tags, and the
    # '-' after the version keeps 6.1.177 from matching 6.1.1770.
    tag="$(git --no-pager tag -l "${prefix}*${kernel_version}-*.amzn2*" | sort -V | tail -n1)"
    if [ -n "$tag" ]; then
      echo "$tag"
      return 0
    fi
  done
  return 1
}

# The upstream version a pin is built from: 6.1.158 for 6.1.158-android.
version_stem() {
  echo "${1%%-*}"
}

# The salt Kconfig carries into the vmlinux as an ELF note, naming what the
# binary was built from. It is a plain string symbol, so olddefconfig keeps
# whatever the config it reads says: a config seeded from another version names
# that version until this recomputes it.
#
# Version, the distro the tag belongs to, and rpm's arch name. Not the tag's rpm
# release: that field moves when upstream rebuilds a version, and the config is
# resolved once and built from weeks later, so carrying it would go stale on its
# own. It would also separate nothing — one pinned version publishes one
# artifact per architecture, whichever release is current when the build runs.
build_salt() {
  local tag="$1" version="$2" target_arch="$3" rpm_arch
  case "$target_arch" in
    arm64) rpm_arch="aarch64" ;;
    *)     rpm_arch="$target_arch" ;;
  esac
  # The distro field get_tag matched on, found by name rather than by position:
  # its glob ends open, so this field is not always the tag's last. A tag
  # without one is not one get_tag returns, so fail rather than salt with
  # whatever happens to sit there.
  [[ "$tag" =~ amzn2[0-9]* ]] || return 1
  echo "${version}-${BASH_REMATCH[0]}.${rpm_arch}"
}

apply_patches() {
  local version="$1"
  local patches_dir="$SCRIPT_DIR/patches/$version"
  [ -d "$patches_dir" ] || return 0
  shopt -s nullglob
  local patches=("$patches_dir"/*.patch)
  shopt -u nullglob
  [ "${#patches[@]}" -gt 0 ] || return 0
  echo "Applying ${#patches[@]} patch(es) for $version"
  for p in "${patches[@]}"; do
    git apply --check "$p"
    git apply "$p"
  done
}

build_version() {
  local version="$1"
  local target_arch="$2"
  local output_arch
  output_arch="$(normalize_arch "$target_arch")"

  echo "Starting build for kernel version: $version (${target_arch})"

  cp "$SCRIPT_DIR/configs/${target_arch}/${version}.config" .config

  local stem tag=""
  stem="$(version_stem "$version")"
  # get_tag greps, so no match is a non-zero exit, not just empty output.
  tag="$(get_tag "$stem")" || true
  if [ -z "$tag" ]; then
    echo "No amzn tag for kernel version $version" >&2
    return 1
  fi
  echo "Checking out $tag for kernel version: $version"
  git checkout -f "$tag"
  # A patch that adds files leaves them untracked; the next build's apply would refuse them.
  git clean -fdq

  apply_patches "$stem"
  # A flavour may carry patches of its own, applied after its stem's.
  [ "$version" = "$stem" ] || apply_patches "$version"

  local make_opts="" cross=""
  if [[ "$target_arch" == "arm64" ]]; then
    if [[ "$HOST_ARCH" != "aarch64" ]]; then
      make_opts="ARCH=arm64 CROSS_COMPILE=aarch64-linux-gnu-"
      cross="aarch64-linux-gnu-" # use the cross objcopy on the aarch64 vmlinux ELF
    else
      make_opts="ARCH=arm64"
    fi
  fi

  if [[ "${CONFIG_ONLY:-}" == "1" ]]; then
    # Only while resolving: a build takes the salt as committed, like the rest
    # of the config, so what ships is what was reviewed.
    local salt
    salt="$(build_salt "$tag" "$version" "$target_arch")"
    echo "Setting the build salt to $salt"
    ./scripts/config --set-str BUILD_SALT "$salt"
  fi

  echo "Resolving config against the $version tree"
  make $make_opts olddefconfig

  if [[ "${CONFIG_ONLY:-}" == "1" ]]; then
    echo "Writing the resolved config back to configs/${target_arch}/${version}.config"
    cp .config "$SCRIPT_DIR/configs/${target_arch}/${version}.config"
    return 0
  fi

  echo "Building kernel version: $version"
  if [[ "$target_arch" == "arm64" ]]; then
    make $make_opts Image -j "$(nproc)"
  else
    make $make_opts vmlinux -j "$(nproc)"
  fi

  echo "Copying finished build to builds directory"
  local out_dir="$SCRIPT_DIR/builds/vmlinux-${version}/${output_arch}"
  local legacy_dir="$SCRIPT_DIR/builds/vmlinux-${version}"
  mkdir -p "$out_dir"
  if [[ "$target_arch" == "arm64" ]]; then
    # arm64 boots arch/arm64/boot/Image, not the raw vmlinux ELF — ship Image as the
    # boot artifact. When the config builds DWARF, also ship a split vmlinux.debug
    # companion from the vmlinux ELF that Image is built from (boot image unchanged).
    cp arch/arm64/boot/Image "$out_dir/vmlinux.bin"
    if readelf -S vmlinux | grep -q '\.debug_info'; then
      "${cross}objcopy" --only-keep-debug vmlinux "$out_dir/vmlinux.debug"
    fi
  elif readelf -S vmlinux | grep -q '\.debug_info'; then
    # The config builds with DWARF. Ship a lean boot image (loadable segments +
    # symtab, DWARF stripped) plus a split vmlinux.debug companion. --strip-debug
    # only removes non-loadable .debug_* sections, so the boot image's loadable
    # segments are unchanged vs a no-DWARF build.
    objcopy --only-keep-debug vmlinux "$out_dir/vmlinux.debug"
    objcopy --strip-debug vmlinux "$out_dir/vmlinux.bin"
    # legacy path (x86_64, no arch subdir) for backwards compat
    cp "$out_dir/vmlinux.bin" "$legacy_dir/vmlinux.bin"
    cp "$out_dir/vmlinux.debug" "$legacy_dir/vmlinux.debug"
  else
    cp vmlinux "$out_dir/vmlinux.bin"
    cp vmlinux "$legacy_dir/vmlinux.bin"
  fi
}

ensure_linux_repo() {
  cd "$SCRIPT_DIR"
  [ -d linux ] || git clone --no-checkout --filter=tree:0 https://github.com/amazonlinux/linux
  cd linux
  make distclean || true
}

main() {
  local single_version="${1:-}"
  local target_arch="${2:-${TARGET_ARCH:-x86_64}}"

  if [[ "${CONFIG_ONLY:-}" == "1" && -z "$single_version" ]]; then
    echo "CONFIG_ONLY=1 needs a version: it rewrites configs/<arch>/<version>.config" >&2
    exit 1
  fi

  install_dependencies "$target_arch"

  ensure_linux_repo

  if [[ -n "$single_version" ]]; then
    build_version "$single_version" "$target_arch"
  else
    while IFS= read -r raw; do
      local version="${raw%%#*}"
      version="${version#"${version%%[![:space:]]*}"}"
      version="${version%"${version##*[![:space:]]}"}"
      [ -z "$version" ] && continue
      build_version "$version" "$target_arch"
    done <"$SCRIPT_DIR/kernel_versions.txt"
  fi
}

main "$@"
