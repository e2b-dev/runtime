#!/usr/bin/env bash
# fetch-artifacts: download the released binaries into the host filesystem
# (mounted at HOST_ROOT) and verify them. Runs unprivileged in the tools image.
if [ -z "${FA_NO_MAIN:-}" ]; then set -euo pipefail; fi

HOST_ROOT="${HOST_ROOT:-/host}"
BUCKET="${BUCKET:-https://storage.googleapis.com/e2b-artifact-binaries}"
FA_ARCH="${FA_ARCH:-$(uname -m)}"

die() { echo "fetch-artifacts: $1" >&2; echo "FIX: $2" >&2; exit 1; }

# Checksums pinned inside the tools image. envd, the kernel, Firecracker and
# BusyBox are pinned by hand in .env, so a bump adds its row here first; a
# version .env names is installable only once a tools image carrying that row
# is published, or once its release has written the .sha256 the fallback
# below reads. The orchestrator moves with the platform release instead
# (.env carries a release marker on it), and a release cannot know the
# checksum of a binary it has not built yet — its rows below cover the
# versions pinned before that; newer ones are verified by the .sha256 the
# release writes beside the binary (see sidecar_sha256). Superseded rows stay
# for the rollback path.
declare -gA SHA256=(
  ["orchestrator/v0.11.0/orchestrator"]="d43e9c6d0b64433e71d5442367dab5142bc868028c144d0439bc84634c116629"
  ["envd/v0.7.0/envd"]="9cda948e73383708c2a3fe2b323ee02483bc0f85d3c2dd8ffea4b1c60cfe35d5"
  ["firecrackers/v1.14-0.2.0/amd64/firecracker"]="ef22aec7cbffcf6cc44a8436a4db79f9e6fe5c52218c81af321dd20f10ad6e5d"
  ["kernels/vmlinux-6.1.177_5008931/amd64/vmlinux.bin"]="9191ced12d24e6e381753a7ab12ec850877524d453eae75c20aeb176f3b5ad05"
  ["busybox/1.36.1/amd64/busybox"]="d7cce939adb09a41a22a5f846d22ba8d576b38dbb2b46a5c77a3a3e27ec52520"
  ["orchestrator/v0.15.0/orchestrator"]="b46e64241f830ceaedf91fccdf4598fcf818179ea156130def16946ce342ecc4"
  ["envd/v0.9.0/envd"]="c42a31d738718b5cf7654e258e5b111308646a905331b266294cdcbeb0a02355"
  ["envd/v0.9.202609130627-59497eb9134/envd"]="9b788bb48aef37afc317a09ff7a5b247ec365049333cf3144e2d3e4b29890612"
  # arm64. The three Firecracker artifacts are published for both
  # architectures already. The orchestrator and envd publish arm64 objects
  # (orchestrator/<version>/arm64/orchestrator, envd/<version>/arm64/envd)
  # with a .sha256 beside each; fetch() verifies those from the sidecar
  # when this table has no row. Rows here are only for versions that
  # predate the sidecar, or a rollback that still needs an in-image pin.
  ["firecrackers/v1.14-0.2.0/arm64/firecracker"]="66a8347a08741e47f850da1720cc6153a6998a2c9e87666958261afbcc9ba05e"
  ["kernels/vmlinux-6.1.177_5008931/arm64/vmlinux.bin"]="3e134b55a6e4feec481f6f3a2813d42860e28c273eeb91c02df0661322e61dab"
  ["busybox/1.36.1/arm64/busybox"]="eddab48ed02fe55034a3f9321c6d4f9db10d699bc92880bd334d94a5c55363e3"
)

# The checksum the release workflow published beside the binary, as
# <key>.sha256 in sha256sum format. The bucket is create-only, so the binary
# and its sidecar are written once, by the same run, and neither can change
# afterwards. Returns 1 when the sidecar is missing or is not a sha256.
sidecar_sha256() {
  local key="$1" line
  line="$(curl -fsSL --retry 3 "$BUCKET/$key.sha256" 2>/dev/null)" || return 1
  line="${line%%[[:space:]]*}"
  [[ "$line" =~ ^[0-9a-f]{64}$ ]] || return 1
  printf '%s\n' "$line"
}

goarch() {
  case "$1" in
    x86_64|amd64) echo amd64 ;;
    aarch64|arm64) echo arm64 ;;
    *) return 1 ;;
  esac
}

sha256_of() { sha256sum "$1" | awk '{print $1}'; }

# fetch <bucket key> <destination path> <mode>
fetch() {
  local key="$1" dest="$2" mode="$3" want got
  want="${SHA256[$key]:-}"
  if [ -z "$want" ]; then
    want="$(sidecar_sha256 "$key")" || {
      echo "fetch-artifacts: no sha256 pinned for $key and no $key.sha256 beside it" >&2
      echo "FIX: add the object's checksum to scripts/fetch-artifacts.sh, or publish the version through the release workflow, which writes the .sha256" >&2
      return 1
    }
  fi
  if [ -f "$dest" ] && [ "$(sha256_of "$dest")" = "$want" ]; then
    # The mode is re-applied even though the bytes are already right: an
    # artifact can be in place with the wrong bits (an earlier run interrupted
    # between mv and chmod, a file copied in by hand, a restored backup), and a
    # non-executable orchestrator, envd, firecracker or busybox fails much
    # later, as an exec error from a service that looks correctly installed.
    chmod "$mode" "$dest" ||
      die "cannot chmod $mode $dest" "check that the host filesystem mounted at $HOST_ROOT is writable, then rerun"
    echo "fetch-artifacts: $key already present"
    return 0
  fi
  # The three host-filesystem writes below each end in a FIX line of their own:
  # HOST_ROOT is a bind mount of the host's /, so a full or read-only host
  # would otherwise fail the one-shot with a bare mkdir/mv/chmod error.
  mkdir -p "$(dirname "$dest")" ||
    die "cannot create $(dirname "$dest")" "check that the host filesystem mounted at $HOST_ROOT is writable and has free space, then rerun"
  echo "fetch-artifacts: downloading $key"
  curl -fsSL --retry 3 -o "$dest.tmp" "$BUCKET/$key" || {
    rm -f "$dest.tmp"
    die "download of $key failed" "check network access to $BUCKET and the pinned versions in .env, then rerun"
  }
  got="$(sha256_of "$dest.tmp")"
  if [ "$got" != "$want" ]; then
    rm -f "$dest.tmp"
    echo "fetch-artifacts: sha256 mismatch for $key: got $got want $want" >&2
    echo "FIX: the version in .env and its checksum (scripts/fetch-artifacts.sh, or the published $key.sha256) disagree" >&2
    return 1
  fi
  mv "$dest.tmp" "$dest" || {
    rm -f "$dest.tmp"
    die "cannot move the verified $key into $dest" "check that the host filesystem mounted at $HOST_ROOT is writable and has free space, then rerun"
  }
  chmod "$mode" "$dest" ||
    die "cannot chmod $mode $dest" "check that the host filesystem mounted at $HOST_ROOT is writable, then rerun"
}

main() {
  local v
  for v in E2B_ORCHESTRATOR_VERSION E2B_ENVD_VERSION E2B_KERNEL_VERSION E2B_FIRECRACKER_VERSION E2B_BUSYBOX_VERSION; do
    [ -n "${!v:-}" ] || die "$v is not set" "set $v in .env (the pinned release versions) and rerun"
  done
  local ga
  ga="$(goarch "$FA_ARCH")" || {
    echo "fetch-artifacts: unsupported architecture $FA_ARCH" >&2
    echo "FIX: only x86_64 and aarch64 hosts are supported; the released orchestrator and envd have no $FA_ARCH build" >&2
    exit 1
  }
  # The orchestrator and envd objects carry no arch segment for amd64 (the key
  # every earlier release published under) and an arm64/ segment otherwise, the
  # layout the three Firecracker artifacts have always used. On the host both
  # land at arch-less paths: the orchestrator reads envd from /fc-envd/envd.
  local seg=""
  [ "$ga" = amd64 ] || seg="$ga/"
  fetch "orchestrator/${E2B_ORCHESTRATOR_VERSION}/${seg}orchestrator" "$HOST_ROOT/var/lib/e2b/bin/orchestrator" 0755 || return 1
  fetch "envd/${E2B_ENVD_VERSION}/${seg}envd" "$HOST_ROOT/fc-envd/envd" 0755 || return 1
  fetch "firecrackers/${E2B_FIRECRACKER_VERSION}/${ga}/firecracker" "$HOST_ROOT/fc-versions/${E2B_FIRECRACKER_VERSION}/${ga}/firecracker" 0755 || return 1
  fetch "kernels/${E2B_KERNEL_VERSION}/${ga}/vmlinux.bin" "$HOST_ROOT/fc-kernels/${E2B_KERNEL_VERSION}/${ga}/vmlinux.bin" 0644 || return 1
  fetch "busybox/${E2B_BUSYBOX_VERSION}/${ga}/busybox" "$HOST_ROOT/fc-busybox/${E2B_BUSYBOX_VERSION}/${ga}/busybox" 0755 || return 1
  echo "fetch-artifacts: ok"
}

if [ -z "${FA_NO_MAIN:-}" ]; then main; fi
