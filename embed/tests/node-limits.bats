#!/usr/bin/env bats

# The orchestrator takes three limits of a node from its environment: how many
# sandboxes it runs or starts, how many start at once, and how many network
# slots returned by ended sandboxes its pool keeps ready. Compose hands them
# over from .env and the pod from the e2b-settings map. Unset, each keeps the
# orchestrator's default, and under Compose the slot pool follows the sandbox
# limit unless it is set itself.
#
# Rendering needs Docker with the compose plugin and jq, as otel.bats does,
# and kubectl for the manifest. Every compose render unsets the three first,
# so a value exported in the runner's own shell is not what these assertions
# see.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

# limits [NAME=value ...]: the three as compose hands them to the orchestrator,
# one `NAME=value` per line, each unset unless an assignment gives it.
limits() {
  env -u MAX_SANDBOXES_PER_NODE -u MAX_STARTING_INSTANCES_PER_NODE -u NETWORK_REUSED_SLOTS_POOL_SIZE "$@" \
    docker compose --project-directory compose config --format json |
    jq -r '.services.orchestrator.environment
      | "MAX_SANDBOXES_PER_NODE=\(.MAX_SANDBOXES_PER_NODE)",
        "MAX_STARTING_INSTANCES_PER_NODE=\(.MAX_STARTING_INSTANCES_PER_NODE)",
        "NETWORK_REUSED_SLOTS_POOL_SIZE=\(.NETWORK_REUSED_SLOTS_POOL_SIZE)"'
}

# Empty is the orchestrator's "keep the default" for the two sandbox limits.
# The pool size renders 0, which keeps the default as well and is a number to
# whatever reads it.
@test "unset, compose leaves all three at the orchestrator's defaults" {
  got="$(limits)"
  want="$(printf '%s\n' MAX_SANDBOXES_PER_NODE= MAX_STARTING_INSTANCES_PER_NODE= NETWORK_REUSED_SLOTS_POOL_SIZE=0)"
  [ "$got" = "$want" ] || { echo "compose renders:"; echo "$got"; return 1; }
}

# A node allowed more sandboxes ends more of them at once, and a slot returned
# to a full pool is torn down, so raising the one without the other slows the
# next burst of creates.
@test "the slot pool follows the sandbox limit" {
  got="$(limits MAX_SANDBOXES_PER_NODE=300)"
  want="$(printf '%s\n' MAX_SANDBOXES_PER_NODE=300 MAX_STARTING_INSTANCES_PER_NODE= NETWORK_REUSED_SLOTS_POOL_SIZE=300)"
  [ "$got" = "$want" ] || { echo "compose renders:"; echo "$got"; return 1; }
}

@test "a pool size of its own wins over the sandbox limit" {
  got="$(limits MAX_SANDBOXES_PER_NODE=300 MAX_STARTING_INSTANCES_PER_NODE=8 NETWORK_REUSED_SLOTS_POOL_SIZE=150)"
  want="$(printf '%s\n' MAX_SANDBOXES_PER_NODE=300 MAX_STARTING_INSTANCES_PER_NODE=8 NETWORK_REUSED_SLOTS_POOL_SIZE=150)"
  [ "$got" = "$want" ] || { echo "compose renders:"; echo "$got"; return 1; }
}

# The pod has no .env: an operator changes the three in the e2b-settings map,
# so the orchestrator has to read each from there. The parity test in
# kubernetes.bats holds the values the map ships to compose's.
@test "the pod's orchestrator reads all three from the e2b-settings map" {
  rendered="$(kubectl kustomize kubernetes)"
  for name in MAX_SANDBOXES_PER_NODE MAX_STARTING_INSTANCES_PER_NODE NETWORK_REUSED_SLOTS_POOL_SIZE; do
    entry="$(printf '%s\n' "$rendered" | grep -A4 "^ *- name: $name\$" || true)"
    printf '%s\n' "$entry" | grep -q "^ *key: $name\$" || { echo "the pod does not read $name from a ConfigMap key of that name"; return 1; }
    printf '%s\n' "$entry" | grep -q '^ *name: e2b-settings-' || { echo "$name does not come from the e2b-settings map"; return 1; }
  done
}
