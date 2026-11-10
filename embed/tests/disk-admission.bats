#!/usr/bin/env bats

# Snapshots and their uploads both live on the host's filesystem here, so a
# pause the disk cannot hold used to fail midway and kill the sandbox. The
# orchestrator refuses such a pause before it starts when its
# pause-admission-disk-headroom-mib flag names a headroom; the flag has no
# LaunchDarkly to come from here, so both install shapes set it through the
# PAUSE_ADMISSION_DISK_HEADROOM_MIB knob the orchestrator reads for its fallback.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

@test "the compose orchestrator refuses a pause the disk cannot hold" {
  grep -q 'PAUSE_ADMISSION_DISK_HEADROOM_MIB: "1024"' compose/compose.yaml
}

@test "the kubernetes orchestrator refuses a pause the disk cannot hold" {
  grep -q 'name: PAUSE_ADMISSION_DISK_HEADROOM_MIB, value: "1024"' kubernetes/statefulset.yaml
}
