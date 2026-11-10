#!/usr/bin/env bats

# A node refuses a pause it cannot take right now — its disk cannot hold the
# snapshot, or it is still persisting the sandbox's parent — with a retryable
# answer. What the api does with that answer is its pause-refusal-restore
# flag: on, the sandbox stays running and the client gets a 503 to retry; off,
# the sandbox is killed. The flag has no LaunchDarkly to come from here, so
# both install shapes turn it on through the PAUSE_REFUSAL_RESTORE knob the
# api reads for its fallback.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

@test "the compose api keeps a refused pause's sandbox running" {
  grep -q 'PAUSE_REFUSAL_RESTORE: "true"' compose/compose.yaml
}

@test "the kubernetes api keeps a refused pause's sandbox running" {
  grep -q 'name: PAUSE_REFUSAL_RESTORE, value: "true"' kubernetes/statefulset.yaml
}
