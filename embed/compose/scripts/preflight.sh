#!/usr/bin/env bash
# preflight: verify the host can run the E2B orchestrator. Runs in PID 1's
# namespaces (nsenter) from the compose service of the same name. Every
# variable below is overridable so the checks are unit-testable.
if [ -z "${PF_NO_MAIN:-}" ]; then set -euo pipefail; fi

PF_SYS="${PF_SYS:-/sys}"
PF_DEV="${PF_DEV:-/dev}"
PF_UNAME_R="${PF_UNAME_R:-$(uname -r)}"
PF_ARCH="${PF_ARCH:-$(uname -m)}"
PF_PAGE_SIZE="${PF_PAGE_SIZE:-$(getconf PAGESIZE 2>/dev/null || echo 0)}"
if [ -z "${PF_GLIBC:-}" ]; then
  pf_ldd="$(ldd --version 2>/dev/null || true)"
  PF_GLIBC="$(awk 'NR==1{print $NF}' <<<"$pf_ldd")"
fi
: "${PF_GLIBC:=0}"
PF_FREE_GIB="${PF_FREE_GIB:-$(df -BG --output=avail / 2>/dev/null | tail -n1 | tr -dc '0-9' || echo 0)}"
PF_MIN_FREE_GIB="${PF_MIN_FREE_GIB:-20}"
PF_MODPROBE="${PF_MODPROBE:-modprobe}"
PF_SKIP_DEVICES="${PF_SKIP_DEVICES:-}"
# Binaries the orchestrator execs on the host (rsync and e2fsprogs for template
# and sandbox rootfs work, iptables and ip for sandbox networking). Standard on
# Ubuntu 24.04 server; a host without one fails a template build minutes later
# with an opaque error, so it is checked here.
PF_TOOLS="${PF_TOOLS:-iptables rsync mkfs.ext4 tune2fs e2fsck ip}"

FAILURES=()
fail() { FAILURES+=("$1"); }

# version_ge A B: true when A >= B using sort -V
version_ge() { [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]; }

check_arch() {
  case "$PF_ARCH" in
    x86_64 | aarch64) ;;
    *) fail "host architecture is $PF_ARCH. FIX: only x86_64 and aarch64 hosts are supported; the released orchestrator and envd have no $PF_ARCH build" ;;
  esac
}
# The orchestrator's memory snapshots and its 2 MiB hugepages assume 4 KiB
# pages; an arm64 kernel built with 64 KiB pages (RHEL's choice) would reserve
# 512 MiB hugepages and mis-map every snapshot.
check_page_size() {
  [ "$PF_PAGE_SIZE" = 4096 ] ||
    fail "the kernel page size is $PF_PAGE_SIZE bytes, need 4096. FIX: boot a 4 KiB-page kernel (Ubuntu's default on x86_64 and aarch64); the orchestrator's snapshots and 2 MiB hugepages assume 4 KiB pages"
}
check_kernel() {
  version_ge "${PF_UNAME_R%%-*}" 6.8 ||
    fail "kernel $PF_UNAME_R is older than 6.8. FIX: apt-get install linux-generic-hwe-24.04 on the host and reboot"
}
check_kvm() {
  [ -n "$PF_SKIP_DEVICES" ] || [ -c "$PF_DEV/kvm" ] ||
    fail "$PF_DEV/kvm is missing. FIX: $(kvm_fix)"
}
# The remedy differs per architecture: x86 has firmware switches and two
# modules to load; arm64 KVM is built into the kernel and only exists when the
# firmware booted it at EL2 or the hypervisor exposes nested virtualization.
kvm_fix() {
  case "$PF_ARCH" in
    aarch64) echo "on bare metal check that the firmware boots the kernel at EL2 (dmesg | grep -i kvm); on a VM recreate it with nested virtualization enabled (Apple silicon: a Lima or other Virtualization.framework VM with nestedVirtualization on, M3 or newer, macOS 15 or newer)" ;;
    *) echo "on bare metal enable VT-x or AMD-V in firmware and modprobe kvm_intel or kvm_amd; on a VM recreate it with nested virtualization enabled (GCE: --enable-nested-virtualization)" ;;
  esac
}
check_tun() {
  [ -n "$PF_SKIP_DEVICES" ] || [ -c "$PF_DEV/net/tun" ] ||
    fail "$PF_DEV/net/tun is missing. FIX: modprobe tun on the host"
}
check_cgroup2() {
  [ -f "$PF_SYS/fs/cgroup/cgroup.controllers" ] ||
    fail "cgroup v2 is not mounted at /sys/fs/cgroup. FIX: boot the host with systemd.unified_cgroup_hierarchy=1"
}
check_glibc() {
  version_ge "$PF_GLIBC" 2.34 ||
    fail "glibc $PF_GLIBC is older than 2.34, the released orchestrator needs 2.34+. FIX: use an Ubuntu 24.04 host"
}
check_nbd() {
  "$PF_MODPROBE" -n nbd >/dev/null 2>&1 ||
    fail "the running kernel has no nbd module. FIX: apt-get install linux-modules-$PF_UNAME_R on the host and reboot"
}
check_tools() {
  local t missing=()
  # shellcheck disable=SC2086
  for t in $PF_TOOLS; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  [ "${#missing[@]}" -eq 0 ] ||
    fail "missing on the host: ${missing[*]}. FIX: apt-get install iptables rsync e2fsprogs iproute2 on the host (the orchestrator execs them there), then start the stack again"
}
check_disk() {
  [ "${PF_FREE_GIB:-0}" -ge "$PF_MIN_FREE_GIB" ] ||
    fail "only ${PF_FREE_GIB} GiB free on /, need ${PF_MIN_FREE_GIB}. FIX: free space on / or give the host a larger disk"
}

main() {
  check_arch; check_page_size; check_kernel; check_kvm; check_tun; check_cgroup2; check_glibc; check_nbd; check_tools; check_disk
  if [ "${#FAILURES[@]}" -gt 0 ]; then
    for f in "${FAILURES[@]}"; do echo "preflight: $f" >&2; done
    echo "FIX: resolve the ${#FAILURES[@]} item(s) above, then start the stack again" >&2
    exit 1
  fi
  echo "preflight: ok (arch=$PF_ARCH page=$PF_PAGE_SIZE kernel=$PF_UNAME_R glibc=$PF_GLIBC free=${PF_FREE_GIB}GiB)"
}

if [ -z "${PF_NO_MAIN:-}" ]; then main; fi
