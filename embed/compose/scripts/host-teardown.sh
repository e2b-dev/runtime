#!/usr/bin/env bash
# host-teardown: undo scripts/host-setup.sh. Covers the same paths as
# host-setup, in the same order so the two lists can be diffed by eye:
#   files:       /etc/modules-load.d/e2b.conf /etc/modprobe.d/e2b-nbd.conf
#                /etc/udev/rules.d/97-nbd-device.rules /etc/sysctl.d/90-e2b.conf
#   directories: /var/lib/e2b /fc-versions /fc-kernels /fc-busybox /fc-envd
#                /fc-vm /orchestrator /var/run/netns
# It also releases the hugepages, unloads nbd on a best-effort basis, removes
# the TCP MSS clamp if host-setup was the one that inserted it (a rule the host
# already had is left alone) and sweeps any sandbox cgroups a crashed
# orchestrator left behind. Refuses to run at all while an
# orchestrator process is still up (run `docker compose down -v` first), and
# refuses to delete the directories while anything is still mounted below them.
# Runs in PID 1's namespaces (nsenter) as root from the compose service of the
# same name (profile purge). HOST_ROOT prefixes every path the script reads or
# removes, and the privileged commands are overridable so the file logic is
# unit-testable; the cgroup sweep is the one exception, addressing the host's
# real cgroup tree through its own override HT_CGROUP_ROOT. Idempotent: a
# second run finds nothing to do and exits 0.
set -euo pipefail

HOST_ROOT="${HOST_ROOT:-}"
HT_SYSCTL="${HT_SYSCTL:-sysctl}"
HT_UDEVADM="${HT_UDEVADM:-udevadm}"
HT_MODPROBE="${HT_MODPROBE:-modprobe}"
HT_IPTABLES="${HT_IPTABLES:-iptables}"
HT_CGROUP_ROOT="${HT_CGROUP_ROOT:-/sys/fs/cgroup/e2b}"
HT_PGREP="${HT_PGREP:-pgrep}"
HT_MOUNTINFO="${HT_MOUNTINFO:-/proc/self/mountinfo}"

log() { echo "host-teardown: $*"; }
die() { echo "host-teardown: $1" >&2; echo "FIX: $2" >&2; exit 1; }

# Guard: refuse to purge while the stack is up. `pid: host` makes the host's
# process tree visible, so a running orchestrator means `docker compose down`
# (or `down -v`) has not happened yet; purging under it would kill live
# sandboxes and delete /var/lib/e2b out from under a running orchestrator.
# Runs before anything else so nothing has mutated when it fires. pgrep's
# status is inspected rather than used as a bare `if` condition, the same shape
# as the iptables -C checks below: 0 means a match, 1 means none, and anything
# else (no pgrep in the image, a pgrep error) means the answer is unknown and
# must not be read as "nothing is running".
pgrep_rc=0
"$HT_PGREP" -x orchestrator >/dev/null 2>&1 || pgrep_rc=$?
case "$pgrep_rc" in
0)
  die "the stack is still running (orchestrator is up)" "run docker compose down -v first, then docker compose --profile purge run --rm host-teardown"
  ;;
1) : ;; # no orchestrator process: safe to proceed
*)
  die "pgrep -x orchestrator failed with status $pgrep_rc, so whether the stack is still up is unknown; nothing was changed" "install pgrep on the host (apt-get install procps) or confirm the stack is down with docker compose ps, then run docker compose down -v followed by docker compose --profile purge run --rm host-teardown"
  ;;
esac

# 0. Sandboxes a crashed orchestrator may have left behind. `docker compose
#    down` already does this through the launcher; here it costs nothing.
ended=0
for d in "$HT_CGROUP_ROOT"/*/; do
  [ -d "$d" ] || continue
  if echo 1 > "$d/cgroup.kill" 2>/dev/null; then ended=$((ended + 1)); else log "warning: could not write $d/cgroup.kill"; fi
done
log "sweep: ended $ended sandbox cgroup(s)"

# 1. Configuration files host-setup wrote, then reload udev so the rule is gone.
for f in /etc/modules-load.d/e2b.conf /etc/modprobe.d/e2b-nbd.conf \
         /etc/udev/rules.d/97-nbd-device.rules /etc/sysctl.d/90-e2b.conf; do
  if [ -e "$HOST_ROOT$f" ]; then
    rm -f "$HOST_ROOT$f" || die "cannot remove $f" "remove $f by hand as root, then run docker compose --profile purge run --rm host-teardown again"
    log "removed $f"
  else
    log "$f already absent"
  fi
done
if command -v "$HT_UDEVADM" >/dev/null 2>&1; then "$HT_UDEVADM" control --reload || true; fi

# 2. Release the hugepages host-setup reserved. The other two sysctls are
#    harmless at their raised values and revert on reboot now that the file is gone.
"$HT_SYSCTL" -q -w vm.nr_hugepages=0 ||
  die "could not set vm.nr_hugepages=0" "check that nothing still maps hugepages (pgrep firecracker), then run docker compose --profile purge run --rm host-teardown again"
log "hugepages released"

# 2b. The TCP MSS clamp, but only the one host-setup inserted. An identical
# TCPMSS rule can predate the stack, in which case host-setup's -C check found
# it, added nothing, and left no marker; deleting every matching rule here
# would take away firewall configuration the host owns. host-setup writes the
# marker only after an insert of its own, so its presence is the whole
# ownership test. It sits under /var/lib/e2b, which step 4 below deletes with
# the rest of the directory, and this step runs before that, so it is still
# readable here; keep the two steps in this order.
#
# -w 5 waits out the xtables lock instead of racing it (dockerd rewrites
# iptables whenever any container starts or stops, and this purge is itself
# `docker compose run`); the -C status is inspected rather than used as a bare
# loop condition so a lock collision cannot be misread as "rule absent" and
# silently leave the host mutated at exit 0.
MSS_RULE=(-p tcp --tcp-flags "SYN,RST" SYN -j TCPMSS --clamp-mss-to-pmtu)
if [ ! -e "$HOST_ROOT/var/lib/e2b/mss-clamp.owned" ]; then
  log "MSS clamp: not added by host-setup, kept"
else
  removed=0
  while :; do
    rc=0
    err="$("$HT_IPTABLES" -w 5 -t mangle -C FORWARD "${MSS_RULE[@]}" 2>&1 1>/dev/null)" || rc=$?
    case "$rc" in
    1) break ;;
    0)
      "$HT_IPTABLES" -w 5 -t mangle -D FORWARD "${MSS_RULE[@]}" ||
        die "iptables -w 5 -t mangle -D FORWARD ... TCPMSS failed" "run iptables -t mangle -S FORWARD by hand, remove the TCPMSS rule with iptables -w 5 -t mangle -D FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu, then run docker compose --profile purge run --rm host-teardown again"
      removed=$((removed + 1))
      [ "$removed" -lt 16 ] ||
        die "iptables -t mangle -C FORWARD ... TCPMSS still present after 16 removals" "run iptables -t mangle -S FORWARD by hand and remove the remaining TCPMSS rule(s), then run docker compose --profile purge run --rm host-teardown again"
      ;;
    *)
      die "iptables -w 5 -t mangle -C FORWARD ... TCPMSS check failed with status $rc${err:+: $err}" "run iptables -t mangle -S FORWARD by hand; if the TCPMSS rule needs removing, run iptables -w 5 -t mangle -D FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu, then run docker compose --profile purge run --rm host-teardown again"
      ;;
    esac
  done
  log "MSS clamp: removed $removed rule(s)"
fi

# 3. Unload nbd on a best-effort basis. It fails while a device is still
#    connected; kvm and tun are left loaded because other software may use them.
#    Modules that stay loaded disappear on the next reboot.
if "$HT_MODPROBE" -r nbd 2>/dev/null; then
  log "nbd unloaded"
else
  log "nbd left loaded (in use or already gone); it unloads on reboot"
fi

# 4. Directories host-setup created, same list as host-setup.sh. /var/run/netns
#    is shared with `ip netns`, so it is only removed when empty.
HT_DIRS=(/var/lib/e2b /fc-versions /fc-kernels /fc-busybox /fc-envd /fc-vm /orchestrator)

# 4a. A crashed orchestrator can leave an nbd-backed rootfs or a per-sandbox
#     tmpfs mounted below /fc-vm or /orchestrator: the sweep above ends
#     processes, it does not unmount. `rm -rf` has no --one-file-system, so it
#     would delete everything inside the mounted filesystem and only then fail
#     on the mount point itself. Refuse before removing anything instead, which
#     is what the removal's own FIX line asks the operator to do anyway.
if [ -r "$HT_MOUNTINFO" ]; then
  mounted=()
  while read -r _ _ _ _ mp _; do
    for d in "${HT_DIRS[@]}"; do
      case "$mp" in "$HOST_ROOT$d" | "$HOST_ROOT$d"/*) mounted+=("$mp") ;; esac
    done
  done < "$HT_MOUNTINFO"
  [ "${#mounted[@]}" -eq 0 ] ||
    die "something is still mounted below the directories to remove: ${mounted[*]}" "unmount them first (umount ${mounted[*]}), then run docker compose --profile purge run --rm host-teardown again"
fi

for d in "${HT_DIRS[@]}"; do
  if [ -e "$HOST_ROOT$d" ]; then
    rm -rf "$HOST_ROOT$d" || die "cannot remove $d" "unmount anything below it (findmnt -R $d), then run docker compose --profile purge run --rm host-teardown again"
    log "removed $d"
  fi
done
if [ -d "$HOST_ROOT/var/run/netns" ]; then
  if rmdir "$HOST_ROOT/var/run/netns" 2>/dev/null; then
    log "removed /var/run/netns"
  else
    log "/var/run/netns kept (not empty)"
  fi
fi

log "if you ran a plain docker compose down (not down -v), run docker compose down -v before the next up: the database still lists templates whose files were just removed"
log "ok"
