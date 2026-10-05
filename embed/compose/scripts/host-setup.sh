#!/usr/bin/env bash
# host-setup: kernel modules, sysctls, udev rule and directories the
# orchestrator needs. Runs in PID 1's namespaces (nsenter) as root.
# The `set` is guarded because tests source this file for its functions
# (HS_NO_MAIN=1) and must not have the harness shell switched to -euo pipefail,
# the same shape preflight.sh and fetch-artifacts.sh use.
if [ -z "${HS_NO_MAIN:-}" ]; then set -euo pipefail; fi

NBDS_MAX="${NBDS_MAX:-64}"
NBD_MAX_PART="${NBD_MAX_PART:-16}"
HUGEPAGES="${HUGEPAGES:-2048}"

log() { echo "host-setup: $*"; }
die() { echo "host-setup: $1" >&2; echo "FIX: $2" >&2; exit 1; }
positive_int() {
  case "$2" in
    ''|*[!0-9]*|0) die "$1=$2 is not a positive integer" "set $1 to a whole number in .env or the environment, or leave it unset for the default" ;;
  esac
}

HS_IPTABLES="${HS_IPTABLES:-iptables}"

# Ownership marker for the MSS clamp, written only when THIS script inserted
# the rule. host-teardown removes the clamp only if the marker is there, so a
# host that already had an identical TCPMSS rule keeps its own firewall
# configuration instead of having it purged from under it. The marker lives
# inside /var/lib/e2b, which host-setup creates and host-teardown removes
# anyway, so the two scripts' path lists stay identical.
HS_MSS_MARKER="${HS_MSS_MARKER:-/var/lib/e2b/mss-clamp.owned}"

# Clamp TCP MSS on forwarded traffic. Cloud NICs commonly have an MTU below
# 1500 (GCE: 1460) while the sandbox tap and veth links are 1500; without the
# clamp downloads above a few MB inside sandboxes stall or arrive truncated
# (seen on every GCE VM). Idempotent; harmless on hosts whose MTU is already 1500.
MSS_RULE=(-p tcp --tcp-flags "SYN,RST" SYN -j TCPMSS --clamp-mss-to-pmtu)
clamp_mss() {
  # dockerd rewrites iptables whenever any container starts or stops, so a
  # -C check can lose the xtables lock race; -w 5 waits for it instead of
  # racing, and the status is inspected rather than used as a bare
  # loop/if condition so a lock collision cannot be misread as "absent".
  local rc=0 err
  err="$("$HS_IPTABLES" -w 5 -t mangle -C FORWARD "${MSS_RULE[@]}" 2>&1 1>/dev/null)" || rc=$?
  case "$rc" in
  0) log "MSS clamp already present" ;;
  1)
    "$HS_IPTABLES" -w 5 -t mangle -I FORWARD 1 "${MSS_RULE[@]}" ||
      die "iptables -t mangle -I FORWARD 1 ... -j TCPMSS --clamp-mss-to-pmtu failed" "install iptables on the host (apt-get install iptables), then start the stack again"
    # Claim the rule only here, after an insert of our own succeeded: the
    # already-present branch above leaves the marker absent, which is how
    # host-teardown knows to leave that rule alone. The directory is created
    # here rather than relied on, because this runs before the install -d of
    # step 4 below and can be the first thing to touch /var/lib/e2b.
    install -d -m 0755 "$(dirname "$HS_MSS_MARKER")" ||
      die "cannot create $(dirname "$HS_MSS_MARKER")" "check permissions and free space on the host, then start the stack again"
    printf 'The TCPMSS clamp in mangle/FORWARD was inserted by host-setup; host-teardown removes it.\n' > "$HS_MSS_MARKER" ||
      die "cannot write $HS_MSS_MARKER" "check permissions and free space on the host, then start the stack again"
    log "MSS clamp added"
    ;;
  *)
    die "iptables -w 5 -t mangle -C FORWARD ... TCPMSS check failed with status $rc${err:+: $err}" "run iptables -t mangle -S FORWARD by hand; if the TCPMSS rule needs removing, run iptables -w 5 -t mangle -D FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu, then start the stack again"
    ;;
  esac
}

# Tests source this file with HS_NO_MAIN=1 to reach the functions above.
# shellcheck disable=SC2317  # `return` only succeeds when sourced; `exit` is the executed-script fallback
if [ -n "${HS_NO_MAIN:-}" ]; then return 0 2>/dev/null || exit 0; fi

# The four /etc writes and the two /sys and /proc reads below each end in a
# FIX line of their own. Without that, an immutable or read-only /etc (the
# host shape this stack does not support) fails the
# script with a bare `bash: ...: Read-only file system` and no remedy.
ETC_FIX="check that /etc is writable on the host: an immutable or read-only root filesystem cannot run this stack, so use a host you can mutate, then start the stack again"

positive_int NBDS_MAX "$NBDS_MAX"
positive_int HUGEPAGES "$HUGEPAGES"

# 1. Modules, persisted for reboots and loaded now.
printf 'nbd\ntun\nkvm\n' > /etc/modules-load.d/e2b.conf ||
  die "cannot write /etc/modules-load.d/e2b.conf" "$ETC_FIX"
echo "options nbd nbds_max=${NBDS_MAX} max_part=${NBD_MAX_PART}" > /etc/modprobe.d/e2b-nbd.conf ||
  die "cannot write /etc/modprobe.d/e2b-nbd.conf" "$ETC_FIX"
modprobe nbd nbds_max="${NBDS_MAX}" max_part="${NBD_MAX_PART}" ||
  die "modprobe nbd nbds_max=${NBDS_MAX} max_part=${NBD_MAX_PART} failed" "load the module by hand and check dmesg on the host"
modprobe tun || die "modprobe tun failed" "load the module by hand and check dmesg on the host"
modprobe kvm || true
have_nbds="$(cat /sys/module/nbd/parameters/nbds_max)" ||
  die "cannot read /sys/module/nbd/parameters/nbds_max although modprobe nbd succeeded" "check that the nbd module is loaded on the host (lsmod | grep nbd) and check dmesg, then start the stack again"
[ "$have_nbds" -ge "$NBDS_MAX" ] ||
  die "nbd is loaded with nbds_max=$have_nbds, need $NBDS_MAX" "with the stack stopped, wait until ls /sys/block/nbd*/pid prints nothing (up to two minutes), run modprobe -r nbd on the host and start the stack again; or reboot so /etc/modprobe.d/e2b-nbd.conf takes effect"
[ -b /dev/nbd0 ] || die "/dev/nbd0 is missing after modprobe" "check dmesg for nbd errors on the host"

# 2. udev rule for the nbd devices (the same rule E2B's own Kubernetes node setup uses).
cat > /etc/udev/rules.d/97-nbd-device.rules <<'EOF' ||
KERNEL=="nbd*", GROUP="disk", MODE="0660"
EOF
  die "cannot write /etc/udev/rules.d/97-nbd-device.rules" "$ETC_FIX"
if command -v udevadm >/dev/null 2>&1; then { udevadm control --reload && udevadm trigger; } || true; fi

# 3. sysctls, persisted and applied.
cat > /etc/sysctl.d/90-e2b.conf <<EOF ||
vm.nr_hugepages=${HUGEPAGES}
net.ipv4.tcp_max_syn_backlog=65535
vm.max_map_count=1048576
EOF
  die "cannot write /etc/sysctl.d/90-e2b.conf" "$ETC_FIX"
sysctl -q -p /etc/sysctl.d/90-e2b.conf ||
  die "sysctl -p /etc/sysctl.d/90-e2b.conf failed" "apply the file by hand and check dmesg on the host"
have_hp="$(cat /proc/sys/vm/nr_hugepages)" ||
  die "cannot read /proc/sys/vm/nr_hugepages although sysctl -p succeeded" "check that /proc is mounted on the host, then start the stack again"
[ "$have_hp" -ge "$HUGEPAGES" ] ||
  die "only $have_hp of $HUGEPAGES 2 MiB hugepages could be reserved" "give the host more memory (12 GiB recommended) or lower HUGEPAGES"

# 3b. TCP MSS clamp for forwarded traffic, plus the ownership marker under
#     /var/lib/e2b that lets host-teardown tell our rule from the host's own.
clamp_mss

# 4. Directories the orchestrator, template-manager and fetch-artifacts use.
# host-teardown covers the same paths (it removes the parent directories and
# /var/run/netns only when empty).
install -d -m 0755 \
  /var/lib/e2b/bin /var/lib/e2b/storage/templates /var/lib/e2b/storage/build-cache \
  /fc-versions /fc-kernels /fc-busybox /fc-envd /fc-vm /orchestrator /var/run/netns ||
  die "install -d of the e2b directories failed" "check permissions and free space on the host, then rerun"

log "ok (nbds_max=$have_nbds hugepages=$have_hp)"
