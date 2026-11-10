#!/bin/bash

set -euo pipefail

FIRECRACKER_REPO_HOST="github.com/${FIRECRACKER_SOURCE_REPO:-e2b-dev/e2b-firecracker}.git"

: "${FIRECRACKER_REPO_TOKEN:?must be set to clone the firecracker repo}"

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <commit_hash> <version_name> [arch]" >&2
  echo "  commit_hash:  Full git commit hash to build" >&2
  echo "  version_name: Output directory name (e.g., v1.14-0.1.0)" >&2
  echo "  arch:         amd64 (default) or arm64" >&2
  exit 1
fi

commit_hash="$1"
version_name="$2"
arch="${3:-amd64}"

# Map Go/Docker arch names to Rust target triples
case "$arch" in
  amd64)  rust_target="x86_64-unknown-linux-musl" ;;
  arm64)  rust_target="aarch64-unknown-linux-musl" ;;
  *)
    echo "Error: unsupported architecture: $arch (expected amd64 or arm64)" >&2
    exit 1
    ;;
esac

git clone "https://x-access-token:${FIRECRACKER_REPO_TOKEN}@${FIRECRACKER_REPO_HOST}" firecracker
cd firecracker
git checkout "$commit_hash"

out_dir="../builds/${version_name}/${arch}"
mkdir -p "$out_dir"
release_bin="build/cargo_target/${rust_target}/release/firecracker"

echo "Building Firecracker $version_name for $arch ($rust_target)..."
tools/devtool -y build --release -- --bin firecracker

# Output goes into {version_name}/{arch}/firecracker
cp "$release_bin" "$out_dir/firecracker"

# The debug variant is only useful where resume-build can drive it, and that
# tool attaches gdb solely through the FIRECRACKER_GDB_SOCKET environment
# variable. A source line without that fallback (v1.10) ships the prod binary
# alone instead of failing the sanity check below.
if ! grep -rqF FIRECRACKER_GDB_SOCKET src; then
  echo "Warning: $version_name has no FIRECRACKER_GDB_SOCKET support; skipping the debug build" >&2
  cd ..
  rm -rf firecracker
  exit 0
fi

# Also build a debug variant: same release (optimized) build with the gdb
# feature enabled and debug symbols kept. Used ONLY for debugging guest kernels
# on dev nodes; it is never deployed to prod client nodes, which resolve the FC
# binary at exactly "<version>/<arch>/firecracker" (a different name).
#
# devtool's release build strips and splits debug info into <bin>.debug, so we
# publish the binary plus its companion. The debuglink is repointed to the
# renamed companion so gdb auto-loads it when the two are colocated.
echo "Building debug Firecracker $version_name for $arch ($rust_target)..."
# `tools/devtool build` (via tools/release.sh) compiles with the crate's default
# features only and silently drops any cargo args after `--`, so `--features gdb`
# never takes effect. Enable gdb by adding it to the firecracker crate's default
# features instead. This edits only the throwaway clone (removed below), and the
# prod binary was already built and copied above, so no backup/restore is needed.
sed -i '/^\[features\]/a default = ["gdb"]' src/firecracker/Cargo.toml
tools/devtool -y build --release -- --bin firecracker
cp "$release_bin" "$out_dir/firecracker-debug"

# Sanity: the debug binary MUST actually contain the gdb feature, otherwise it is
# useless for guest-kernel debugging and indistinguishable from the prod binary.
# The `FIRECRACKER_GDB_SOCKET` env-var literal (builder.rs, #[cfg(feature = "gdb")])
# is present iff the feature was compiled in, and survives stripping.
# Use grep -c (counts, consumes all input) rather than grep -q: under
# `set -o pipefail`, grep -q closing the pipe early can SIGPIPE `strings` and make
# the pipeline report failure even on a match.
gdb_feature_strings=$(strings "$out_dir/firecracker-debug" | grep -cF FIRECRACKER_GDB_SOCKET || true)
if [[ "${gdb_feature_strings:-0}" -eq 0 ]]; then
  echo "Error: firecracker-debug built WITHOUT the gdb feature (no FIRECRACKER_GDB_SOCKET)" >&2
  exit 1
fi
if [[ -f "${release_bin}.debug" ]]; then
  cp "${release_bin}.debug" "$out_dir/firecracker-debug.debug"
  objcopy --remove-section .gnu_debuglink "$out_dir/firecracker-debug" 2>/dev/null || true
  ( cd "$out_dir" && objcopy --add-gnu-debuglink=firecracker-debug.debug firecracker-debug )
else
  echo "Warning: ${release_bin}.debug not found; firecracker-debug ships without split DWARF" >&2
  # Drop any stale debuglink (the build points it at "firecracker.debug", which we
  # do not ship next to firecracker-debug) so gdb doesn't chase a missing file.
  objcopy --remove-section .gnu_debuglink "$out_dir/firecracker-debug" 2>/dev/null || true
fi

cd ..
rm -rf firecracker
