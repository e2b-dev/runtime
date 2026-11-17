#!/usr/bin/env bash
# orchestrator-launch: PID 1 of the compose service "orchestrator". Starts the
# released orchestrator binary in PID 1's namespaces through nsenter and, on
# SIGTERM, ends every sandbox before forwarding the signal. Firecracker
# processes live in /sys/fs/cgroup/e2b/<sandbox>, outside compose's reach, and
# the orchestrator itself only drains (waits) on SIGTERM; killing the cgroups
# first is what makes `docker compose down`, `stop`, `restart` and a bare
# `docker stop` leave no sandbox behind and lets the drain finish at once.
# It sweeps again when the child dies on its own, so `restart: unless-stopped`
# cannot start a fresh orchestrator beside a crashed one's sandboxes.
# Before the start it refuses a node limit set to a value the orchestrator
# would ignore, stop on or run no sandbox with.
# Compose pre_stop hooks were verified not to fire on v5.5.0.
# Test hooks: OL_CGROUP_ROOT (fake cgroup tree), OL_COMMAND (child command line).
set -euo pipefail

OL_CGROUP_ROOT="${OL_CGROUP_ROOT:-/sys/fs/cgroup/e2b}"

log() { echo "orchestrator-launch: $*"; }
die() { echo "orchestrator-launch: $1" >&2; echo "FIX: $2" >&2; exit 1; }

# shellcheck disable=SC2329  # invoked from the SIGTERM trap
sweep() {
  local d ended=0 failed=0
  for d in "$OL_CGROUP_ROOT"/*/; do
    [ -d "$d" ] || continue
    if echo 1 > "$d/cgroup.kill" 2>/dev/null; then
      ended=$((ended + 1))
    else
      failed=$((failed + 1))
      log "warning: could not write $d/cgroup.kill"
    fi
  done
  log "sweep: ended $ended sandbox cgroup(s), $failed not writable"
}

# limit <variable> <pattern> <what it has to be>. Left to the orchestrator,
# text in either MAX_* limit, or a number too long for it to read, keeps the
# default without a word, a sandbox limit of zero or less refuses every
# create, and a start limit of zero or less or a pool size that is not a whole
# number stops it at start. Empty keeps the orchestrator's default.
limit() {
  local value="${!1:-}"
  [ -z "$value" ] || [[ "$value" =~ $2 ]] ||
    die "$1=$value is not $3" "set $1 to $3 in .env or the e2b-settings map, or leave it empty for the default"
}
# The pool size last: Compose hands it the sandbox limit's value when only
# that one is set, and the FIX line has to name the variable the operator set.
limit MAX_SANDBOXES_PER_NODE '^0*[1-9][0-9]{0,17}$' "a whole number above zero, of at most 18 digits"
limit MAX_STARTING_INSTANCES_PER_NODE '^0*[1-9][0-9]{0,17}$' "a whole number above zero, of at most 18 digits"
limit NETWORK_REUSED_SLOTS_POOL_SIZE '^[0-9]+$' "a whole number"
# A node has 32766 network slots, and the orchestrator stops at start on a pool
# above that, in a message that names only the pool. Under Compose a sandbox
# limit above it lands here too, so the FIX line names both.
pool="${NETWORK_REUSED_SLOTS_POOL_SIZE:-}"
pool="${pool#"${pool%%[!0]*}"}"
if [ "${#pool}" -gt 5 ] || { [ -n "$pool" ] && [ "$pool" -gt 32766 ]; }; then
  die "NETWORK_REUSED_SLOTS_POOL_SIZE=${NETWORK_REUSED_SLOTS_POOL_SIZE} is more than the 32766 network slots a node has" \
    "set NETWORK_REUSED_SLOTS_POOL_SIZE to 32766 or less in .env or the e2b-settings map; under Compose it takes the value of MAX_SANDBOXES_PER_NODE unless it is set itself"
fi

if [ -n "${OL_COMMAND:-}" ]; then
  # Test hook: a command line with shell quoting, evaluated on purpose.
  eval "set -- $OL_COMMAND"
else
  set -- nsenter -t 1 -m -u -i -n -C -w/ -- \
    /usr/bin/env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    /var/lib/e2b/bin/orchestrator
fi

# The trap is installed before the child starts so a SIGTERM arriving in the
# gap between fork and `trap` cannot skip the sweep and fall back to bash's
# default (immediate exit, sandboxes and child left running). `terminating`
# distinguishes an operator-requested stop from the child dying on its own,
# which decides below whether a non-zero exit gets a FIX line.
terminating=""
child=""

# shellcheck disable=SC2329  # invoked from the SIGTERM trap
# shellcheck disable=SC2317  # invoked only through the trap below; shellcheck 0.9 cannot see that
on_term() {
  terminating=1
  log "SIGTERM: ending sandboxes, then the orchestrator (pid ${child:-unknown})"
  sweep
  if [ -n "$child" ]; then
    kill -TERM "$child" 2>/dev/null || true
  fi
}
trap on_term TERM INT

"$@" &
child=$!
# Only claim success once the child is confirmed alive; a child that failed
# to exec (e.g. a missing /var/lib/e2b/bin/orchestrator) must not be logged
# as "started".
if kill -0 "$child" 2>/dev/null; then
  log "started $1 (pid $child)"
fi

# `wait` returns early when a trapped signal arrives; loop until the child is gone.
set +e
wait "$child"; status=$?
while kill -0 "$child" 2>/dev/null; do
  wait "$child"; status=$?
done
set -e
log "orchestrator exited with status $status"
if [ "$status" -ne 0 ] && [ -z "$terminating" ]; then
  # The child died on its own: a crash, or a failed exec. This is the one exit
  # path `restart: unless-stopped` reacts to, so sweep here as well. Without
  # it Compose starts a fresh orchestrator beside the Firecracker processes of
  # the dead one, which sit in host cgroups outside this container and which
  # nothing else would end. Logged before the FIX line, which stays last.
  sweep
  echo "FIX: read the orchestrator and api logs for the orchestrator's own error; if /var/lib/e2b/bin/orchestrator is missing, fetch-artifacts did not finish, so start the stack again (it runs before the orchestrator)" >&2
fi
exit "$status"
