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
