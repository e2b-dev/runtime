#!/usr/bin/env bats

# shellcheck disable=SC2030,SC2031  # bats runs each @test body in its own
# shell, so an HT_* override set in a test is read by the script that test
# launches; shellcheck reads an @test block as a subshell whose changes are lost.

# host-teardown.sh works under HOST_ROOT; sysctl, udevadm and modprobe are
# replaced with `true` so the file logic runs unprivileged on any machine.
# The iptables assertions spell out the `-w 5` xtables-lock flag, so dropping
# it from a real invocation turns this file red.
setup() {
  export HOST_ROOT="$BATS_TEST_TMPDIR/host"
  export HT_SYSCTL=true HT_UDEVADM=true HT_MODPROBE=true
  export HT_CGROUP_ROOT="$BATS_TEST_TMPDIR/e2b"
  # An empty mount table: nothing is mounted below the directories to remove.
  export HT_MOUNTINFO="$BATS_TEST_TMPDIR/mountinfo"
  : > "$HT_MOUNTINFO"
  mkdir -p "$HT_CGROUP_ROOT"
  mkdir -p "$HOST_ROOT/etc/modules-load.d" "$HOST_ROOT/etc/modprobe.d" "$HOST_ROOT/etc/udev/rules.d" "$HOST_ROOT/etc/sysctl.d"
  for f in etc/modules-load.d/e2b.conf etc/modprobe.d/e2b-nbd.conf etc/udev/rules.d/97-nbd-device.rules etc/sysctl.d/90-e2b.conf; do
    echo x > "$HOST_ROOT/$f"
  done
  mkdir -p "$HOST_ROOT/var/lib/e2b/bin" "$HOST_ROOT/var/lib/e2b/storage/templates" "$HOST_ROOT/var/lib/e2b/storage/build-cache" \
           "$HOST_ROOT/fc-versions/v1.14-0.2.0/amd64" "$HOST_ROOT/fc-kernels" "$HOST_ROOT/fc-busybox" "$HOST_ROOT/fc-envd" \
           "$HOST_ROOT/fc-vm" "$HOST_ROOT/orchestrator" "$HOST_ROOT/var/run/netns"
  echo bin > "$HOST_ROOT/var/lib/e2b/bin/orchestrator"
  echo keep > "$HOST_ROOT/etc/hostname"
  # The fixture is a host host-setup has run on, so the MSS clamp is the
  # stack's: the ownership marker is present. The one test for a clamp the
  # stack does not own removes it again.
  : > "$HOST_ROOT/var/lib/e2b/mss-clamp.owned"
  # Default: no orchestrator process found, so the purge guard passes for
  # every test that does not override it. `false` always exits 1, matching
  # what `pgrep -x orchestrator` returns when nothing matches.
  export HT_PGREP=false
  export HT_IPTABLES="$BATS_TEST_TMPDIR/iptables" FAKE_LOG="$BATS_TEST_TMPDIR/calls" FAKE_CHECK_RC="$BATS_TEST_TMPDIR/check-rc"
  echo 1 > "$FAKE_CHECK_RC"          # default: no rule present, nothing to remove
  cat > "$HT_IPTABLES" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_LOG"
case " $* " in
  *" -C "*) exit "$(cat "$FAKE_CHECK_RC")" ;;
  *" -D "*) echo 1 > "$FAKE_CHECK_RC"; exit 0 ;;   # the rule is gone after one delete
  *) exit 0 ;;
esac
EOF
  chmod +x "$HT_IPTABLES"
}

@test "removes the four files host-setup wrote under /etc" {
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [ ! -e "$HOST_ROOT/etc/modules-load.d/e2b.conf" ]
  [ ! -e "$HOST_ROOT/etc/modprobe.d/e2b-nbd.conf" ]
  [ ! -e "$HOST_ROOT/etc/udev/rules.d/97-nbd-device.rules" ]
  [ ! -e "$HOST_ROOT/etc/sysctl.d/90-e2b.conf" ]
}

@test "removes the directories host-setup created and nothing else" {
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  for d in var/lib/e2b fc-versions fc-kernels fc-busybox fc-envd fc-vm orchestrator var/run/netns; do
    [ ! -e "$HOST_ROOT/$d" ]
  done
  [ -f "$HOST_ROOT/etc/hostname" ]
}

@test "keeps /var/run/netns when it is not empty" {
  : > "$HOST_ROOT/var/run/netns/leftover"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [ -d "$HOST_ROOT/var/run/netns" ]
  [[ "$output" == *"kept"* ]]
}

@test "is idempotent" {
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [[ "$output" == *"host-teardown: ok"* ]]
}

@test "ends with a FIX line when the hugepages cannot be released" {
  run env HT_SYSCTL=false bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"FIX:"* ]]
}

@test "sweeps every sandbox cgroup under the root" {
  mkdir -p "$HT_CGROUP_ROOT/s1" "$HT_CGROUP_ROOT/s2"; : > "$HT_CGROUP_ROOT/s1/cgroup.kill"; : > "$HT_CGROUP_ROOT/s2/cgroup.kill"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [ "$(cat "$HT_CGROUP_ROOT/s1/cgroup.kill")" = "1" ]
  [[ "$output" == *"sweep: ended 2 sandbox cgroup(s)"* ]]
}

@test "removes the MSS clamp rule host-setup inserted" {
  echo 0 > "$FAKE_CHECK_RC"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [[ "$output" == *"MSS clamp: removed 1 rule(s)"* ]]
  grep -q -- "-w 5 -t mangle -C FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu" "$FAKE_LOG"
  grep -q -- "-w 5 -t mangle -D FORWARD -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu" "$FAKE_LOG"
  # The marker goes with the directory it lives in.
  [ ! -e "$HOST_ROOT/var/lib/e2b/mss-clamp.owned" ]
}

# An identical TCPMSS rule can predate the stack: host-setup's -C check finds
# it, inserts nothing and leaves no marker. Deleting every matching rule here
# would strip firewall configuration the host owns, so with no marker the rule
# is left exactly as it is and iptables is not called at all.
@test "keeps an MSS clamp rule host-setup did not add" {
  rm -f "$HOST_ROOT/var/lib/e2b/mss-clamp.owned"
  echo 0 > "$FAKE_CHECK_RC"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [[ "$output" == *"MSS clamp: not added by host-setup, kept"* ]]
  [[ "$output" != *"MSS clamp: removed"* ]]
  [ ! -s "$FAKE_LOG" ]
  # The rest of the purge still ran.
  [ ! -e "$HOST_ROOT/var/lib/e2b" ]
  [[ "$output" == *"host-teardown: ok"* ]]
}

@test "dies with a FIX line when the iptables check hits the xtables lock" {
  cat > "$HT_IPTABLES" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_LOG"
case " $* " in
  *" -C "*) echo "Another app is currently holding the xtables lock." >&2; exit 4 ;;
  *) exit 0 ;;
esac
EOF
  chmod +x "$HT_IPTABLES"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"FIX:"* ]]
  [[ "$output" == *"iptables -t mangle -S FORWARD"* ]]
}

@test "purge guard: refuses to run while the orchestrator is up, before any mutation" {
  cat > "$BATS_TEST_TMPDIR/pgrep" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$BATS_TEST_TMPDIR/pgrep"
  export HT_PGREP="$BATS_TEST_TMPDIR/pgrep"
  mkdir -p "$HT_CGROUP_ROOT/s1"; : > "$HT_CGROUP_ROOT/s1/cgroup.kill"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"FIX: run docker compose down -v first, then docker compose --profile purge run --rm host-teardown"* ]]
  # nothing mutated: files, directories and the cgroup all untouched, iptables never invoked
  [ -e "$HOST_ROOT/etc/modules-load.d/e2b.conf" ]
  [ -e "$HOST_ROOT/var/lib/e2b/bin/orchestrator" ]
  [ -z "$(cat "$HT_CGROUP_ROOT/s1/cgroup.kill")" ]
  [ ! -s "$FAKE_LOG" ]
}

@test "purge guard passes when no orchestrator process is found" {
  # HT_PGREP=false from setup(): exit 1, what `pgrep -x` returns for no match.
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [[ "$output" == *"host-teardown: ok"* ]]
}

# Any status other than 0 or 1 means pgrep could not answer, so whether the
# stack is up is unknown. Treating that as "nothing running" would purge under
# a live orchestrator, so the script refuses instead.
@test "purge guard: an unusable pgrep is refused, not read as an absent orchestrator" {
  cat > "$BATS_TEST_TMPDIR/pgrep" <<'EOF'
#!/usr/bin/env bash
exit 127
EOF
  chmod +x "$BATS_TEST_TMPDIR/pgrep"
  export HT_PGREP="$BATS_TEST_TMPDIR/pgrep"
  mkdir -p "$HT_CGROUP_ROOT/s1"; : > "$HT_CGROUP_ROOT/s1/cgroup.kill"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"pgrep -x orchestrator failed with status 127"* ]]
  [[ "${lines[-1]}" == "FIX: "* ]]
  [[ "$output" == *"FIX: install pgrep on the host"* ]]
  # nothing mutated, same as the orchestrator-is-up path
  [ -e "$HOST_ROOT/etc/modules-load.d/e2b.conf" ]
  [ -e "$HOST_ROOT/var/lib/e2b/bin/orchestrator" ]
  [ -z "$(cat "$HT_CGROUP_ROOT/s1/cgroup.kill")" ]
  [ ! -s "$FAKE_LOG" ]
}

@test "purge guard: a pgrep that is not installed at all is refused" {
  export HT_PGREP="$BATS_TEST_TMPDIR/no-such-pgrep"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"pgrep -x orchestrator failed with status 127"* ]]
  [ -e "$HOST_ROOT/var/lib/e2b/bin/orchestrator" ]
}

# A crashed orchestrator can leave an nbd-backed rootfs or a per-sandbox tmpfs
# mounted below /fc-vm or /orchestrator. `rm -rf` would empty the mounted
# filesystem before failing on the mount point, so the script refuses first.
@test "refuses to delete the directories while something is mounted below them" {
  printf '%s\n' \
    "26 25 0:22 / / rw,relatime shared:1 - ext4 /dev/sda1 rw" \
    "99 26 43:0 / $HOST_ROOT/fc-vm/sbx-1/rootfs rw,relatime shared:9 - ext4 /dev/nbd3 rw" \
    > "$HT_MOUNTINFO"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"still mounted below the directories to remove"* ]]
  [[ "$output" == *"$HOST_ROOT/fc-vm/sbx-1/rootfs"* ]]
  [[ "${lines[-1]}" == "FIX: "* ]]
  [[ "$output" == *"FIX: unmount them first (umount "* ]]
  # the directories are all still there: nothing inside the mount was deleted
  for d in var/lib/e2b fc-versions fc-vm orchestrator; do
    [ -e "$HOST_ROOT/$d" ]
  done
}

@test "a mount elsewhere on the host does not block the purge" {
  printf '%s\n' \
    "26 25 0:22 / / rw,relatime shared:1 - ext4 /dev/sda1 rw" \
    "31 26 0:27 / $HOST_ROOT/var/lib/docker/overlay2/x rw,relatime shared:3 - overlay overlay rw" \
    > "$HT_MOUNTINFO"
  run bash "$BATS_TEST_DIRNAME/../compose/scripts/host-teardown.sh"
  [ "$status" -eq 0 ]
  [ ! -e "$HOST_ROOT/fc-vm" ]
  [[ "$output" == *"host-teardown: ok"* ]]
}
