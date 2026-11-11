#!/usr/bin/env bats

# orchestrator-launch.sh supervises a child and sweeps a cgroup tree on SIGTERM.
# OL_COMMAND replaces the nsenter line with a plain command; OL_CGROUP_ROOT
# points at a fake tree with one directory per "sandbox". Negative assertions
# use `run cmd` plus a status check, never a bare `! cmd`: in bats a
# `!`-negated command is exempt from errexit and cannot fail a test wherever it
# sits (shellcheck SC2314).
setup() {
  export OL_CGROUP_ROOT="$BATS_TEST_TMPDIR/e2b"
  mkdir -p "$OL_CGROUP_ROOT/sbx-a" "$OL_CGROUP_ROOT/sbx-b"
  : > "$OL_CGROUP_ROOT/sbx-a/cgroup.kill"
  : > "$OL_CGROUP_ROOT/sbx-b/cgroup.kill"
  script="$BATS_TEST_DIRNAME/../compose/scripts/orchestrator-launch.sh"
  out="$BATS_TEST_TMPDIR/out"
  # The child of the limit tests below leaves this file, so its absence says
  # the child never ran. A limit inherited from the runner's shell must not
  # reach the launcher in any test.
  marker="$BATS_TEST_TMPDIR/child-ran"
  unset MAX_SANDBOXES_PER_NODE MAX_STARTING_INSTANCES_PER_NODE NETWORK_REUSED_SLOTS_POOL_SIZE
}

# Start the launcher in the background, wait until it reports its child as
# started, send SIGTERM to the launcher itself (what docker stop does to PID 1),
# collect its exit status.
term_launcher() {
  bash "$script" > "$out" 2>&1 &
  local pid=$! i
  # shellcheck disable=SC2034  # i is only the retry counter
  for i in $(seq 1 50); do grep -q 'started ' "$out" 2>/dev/null && break; sleep 0.1; done
  kill -TERM "$pid"
  set +e; wait "$pid"; status=$?; set -e
}

@test "SIGTERM writes 1 to every cgroup.kill under the root before ending the child" {
  OL_COMMAND="sleep 30" term_launcher
  [ "$status" -eq 143 ]
  [ "$(cat "$OL_CGROUP_ROOT/sbx-a/cgroup.kill")" = "1" ]
  [ "$(cat "$OL_CGROUP_ROOT/sbx-b/cgroup.kill")" = "1" ]
  grep -q "sweep: ended 2 sandbox cgroup(s)" "$out"
}

@test "SIGTERM with an empty root ends the child and reports zero" {
  rm -rf "${OL_CGROUP_ROOT:?}"/*
  OL_COMMAND="sleep 30" term_launcher
  [ "$status" -eq 143 ]
  grep -q "sweep: ended 0 sandbox cgroup(s)" "$out"
}

@test "exits with the child's own status when the child ends by itself" {
  run env OL_COMMAND="sh -c 'exit 3'" bash "$script"
  [ "$status" -eq 3 ]
  [[ "$output" == *"orchestrator exited with status 3"* ]]
}

@test "warns and continues when a cgroup.kill is not writable" {
  [ "$(id -u)" -ne 0 ] || skip "root can write mode 000 files"
  chmod 000 "$OL_CGROUP_ROOT/sbx-b/cgroup.kill"
  OL_COMMAND="sleep 30" term_launcher
  chmod 644 "$OL_CGROUP_ROOT/sbx-b/cgroup.kill"
  [ "$status" -eq 143 ]
  grep -q "warning: could not write" "$out"
  grep -q "sweep: ended 1 sandbox cgroup(s)" "$out"
}

@test "prints exactly one FIX line when the child exits non-zero without a stop" {
  run env OL_COMMAND="sh -c 'exit 3'" bash "$script"
  [ "$status" -eq 3 ]
  [[ "${lines[-1]}" == "FIX: "* ]]
  [ "$(printf '%s\n' "${lines[@]}" | grep -c '^FIX: ')" -eq 1 ]
  # The launcher runs under compose and in the StatefulSet alike, so the hint
  # names the logs and the missing step rather than one shape's command.
  [[ "$output" == *"read the orchestrator and api logs"* ]]
  [[ "$output" == *"fetch-artifacts did not finish, so start the stack again (it runs before the orchestrator)"* ]]
  [[ "$output" != *"docker compose"* ]]
}

# A crash is the one exit path `restart: unless-stopped` reacts to, so the
# launcher has to sweep there as well: otherwise Compose starts a fresh
# orchestrator beside the Firecracker processes of the dead one, which live in
# host cgroups outside this container and nothing else would end.
@test "sweeps the sandbox cgroups when the child crashes, before the FIX line" {
  rm -rf "${OL_CGROUP_ROOT:?}"/*
  mkdir -p "$OL_CGROUP_ROOT/sbx-crash"; : > "$OL_CGROUP_ROOT/sbx-crash/cgroup.kill"
  run env OL_COMMAND="sh -c 'exit 2'" bash "$script"
  [ "$status" -eq 2 ]
  [ "$(cat "$OL_CGROUP_ROOT/sbx-crash/cgroup.kill")" = "1" ]
  [[ "$output" == *"sweep: ended 1 sandbox cgroup(s)"* ]]
  # the sweep is logged before the FIX line, which stays the last line
  [[ "${lines[-1]}" == "FIX: "* ]]
  [ "$(printf '%s\n' "${lines[@]}" | grep -n 'sweep: ended' | cut -d: -f1)" -lt \
    "$(printf '%s\n' "${lines[@]}" | grep -n '^FIX: ' | cut -d: -f1)" ]
}

@test "does not sweep when the child exits zero" {
  run env OL_COMMAND="sh -c 'exit 0'" bash "$script"
  [ "$status" -eq 0 ]
  [ "$(cat "$OL_CGROUP_ROOT/sbx-a/cgroup.kill")" = "" ]
  [[ "$output" != *"sweep: ended"* ]]
}

@test "SIGTERM ends the child with no FIX line" {
  OL_COMMAND="sleep 30" term_launcher
  [ "$status" -eq 143 ]
  run grep -q '^FIX:' "$out"
  [ "$status" -ne 0 ]
}

# The orchestrator keeps its default for a sandbox limit it cannot parse and
# says nothing, refuses every create on a sandbox limit of zero, and stops at
# start on a start limit of zero. The launcher refuses each before the child
# starts, with a FIX line that names the variable.
@test "refuses a sandbox limit that is not a whole number above zero, before the child starts" {
  local name bad
  for name in MAX_SANDBOXES_PER_NODE MAX_STARTING_INSTANCES_PER_NODE; do
    for bad in 0 00 -1 abc 3x 1.5 9999999999999999999; do
      run env "$name=$bad" OL_COMMAND="touch $marker" bash "$script"
      [ "$status" -eq 1 ]
      [[ "$output" == *"$name=$bad is not a whole number above zero"* ]]
      [[ "${lines[-1]}" == "FIX: set $name to a whole number above zero"* ]]
      [ "$(printf '%s\n' "${lines[@]}" | grep -c '^FIX: ')" -eq 1 ]
      [ ! -e "$marker" ]
    done
  done
}

# Compose hands the pool the sandbox limit's value when only that one is set,
# so the same text arrives in both, and the FIX line has to name the one the
# operator wrote.
@test "a bad sandbox limit that the slot pool follows is the one the FIX line names" {
  run env MAX_SANDBOXES_PER_NODE=many NETWORK_REUSED_SLOTS_POOL_SIZE=many OL_COMMAND="touch $marker" bash "$script"
  [ "$status" -eq 1 ]
  [[ "${lines[-1]}" == "FIX: set MAX_SANDBOXES_PER_NODE to "* ]]
  [ ! -e "$marker" ]
}

# Zero is a pool size: it keeps the orchestrator's default. Anything that is
# not a whole number stops the orchestrator at start.
@test "refuses a slot pool size that is not a whole number, before the child starts" {
  local bad
  for bad in -1 abc 1.5; do
    run env NETWORK_REUSED_SLOTS_POOL_SIZE="$bad" OL_COMMAND="touch $marker" bash "$script"
    [ "$status" -eq 1 ]
    [[ "$output" == *"NETWORK_REUSED_SLOTS_POOL_SIZE=$bad is not a whole number"* ]]
    [[ "${lines[-1]}" == "FIX: set NETWORK_REUSED_SLOTS_POOL_SIZE to a whole number in "* ]]
    [ ! -e "$marker" ]
  done
}

# A node has 32766 network slots, and under Compose the pool takes the sandbox
# limit's value unless it is set itself, so a sandbox limit above that arrives
# as a pool size the orchestrator stops on, in a message that names only the
# pool.
@test "refuses a slot pool above the node's network slots, naming both settings" {
  local bad
  for bad in 32767 100000; do
    run env MAX_SANDBOXES_PER_NODE="$bad" NETWORK_REUSED_SLOTS_POOL_SIZE="$bad" OL_COMMAND="touch $marker" bash "$script"
    [ "$status" -eq 1 ]
    [[ "$output" == *"NETWORK_REUSED_SLOTS_POOL_SIZE=$bad is more than the 32766 network slots a node has"* ]]
    [[ "${lines[-1]}" == "FIX: "*"MAX_SANDBOXES_PER_NODE"* ]]
    [ ! -e "$marker" ]
  done
  # A pool size of its own that is too long for the shell to compare as a number.
  run env NETWORK_REUSED_SLOTS_POOL_SIZE=99999999999999999999 OL_COMMAND="touch $marker" bash "$script"
  [ "$status" -eq 1 ]
  [[ "$output" == *"is more than the 32766 network slots a node has"* ]]
  [ ! -e "$marker" ]
  run env NETWORK_REUSED_SLOTS_POOL_SIZE=032766 OL_COMMAND="touch $marker" bash "$script"
  [ "$status" -eq 0 ]
  [ -e "$marker" ]
}

@test "starts the child on limits that are whole numbers, and on empty ones" {
  run env MAX_SANDBOXES_PER_NODE=300 MAX_STARTING_INSTANCES_PER_NODE=8 NETWORK_REUSED_SLOTS_POOL_SIZE=0 \
    OL_COMMAND="touch $marker" bash "$script"
  [ "$status" -eq 0 ]
  [ -e "$marker" ]
  rm "$marker"
  run env MAX_SANDBOXES_PER_NODE="" MAX_STARTING_INSTANCES_PER_NODE="" NETWORK_REUSED_SLOTS_POOL_SIZE="" \
    OL_COMMAND="touch $marker" bash "$script"
  [ "$status" -eq 0 ]
  [ -e "$marker" ]
}
