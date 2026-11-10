#!/usr/bin/env bats

# A killed sandbox gives its hugepages back a few hundred milliseconds after
# the kill returns, when its Firecracker has exited and the host side has
# unmapped the guest memory. With the pool sized for the sandboxes you run at
# once, a create sent in that window cannot map its guest memory. The
# orchestrator's create-memory-refusal flag makes it refuse such a create for
# the api to retry on this host, instead of failing it. The flag has no
# LaunchDarkly to come from here, so both install shapes turn it on through
# the CREATE_MEMORY_REFUSAL knob the orchestrator reads for its fallback.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

@test "the compose orchestrator refuses a create that collides with a stopping sandbox's memory" {
  grep -q 'CREATE_MEMORY_REFUSAL: "true"' compose/compose.yaml
}

@test "the kubernetes orchestrator refuses a create that collides with a stopping sandbox's memory" {
  grep -q 'name: CREATE_MEMORY_REFUSAL, value: "true"' kubernetes/statefulset.yaml
}
