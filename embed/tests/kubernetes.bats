#!/usr/bin/env bats

# The StatefulSet is a translation of compose/compose.yaml; these tests keep
# the two from drifting. Needs kubectl (for `kubectl kustomize`) and python3;
# the tests that compare against compose also need Docker with the compose
# plugin and jq, as team-api-key.bats already does.
#
# The rendered manifest is read as text, not through a YAML parser: python3's
# yaml module is not guaranteed on the test host. kustomize emits two-space
# indentation with sorted keys, so a container's `name:` and `startupProbe:`
# both sit at eight spaces and everything nested under them is deeper.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
  RENDERED="$(kubectl kustomize kubernetes)"
}

# `service period timeout failures` per container that has a startup probe.
startup_probes() {
  printf '%s\n' "$RENDERED" | python3 -c '
import re, sys
lines = sys.stdin.read().splitlines()
name = None
for i, line in enumerate(lines):
    m = re.match(r"^ {8}name: (\S+)$", line)
    if m:
        name = m.group(1)
        continue
    if line != "        startupProbe:" or name is None:
        continue
    v = {}
    for nxt in lines[i + 1:]:
        if not nxt.startswith(" " * 10):
            break
        k = re.match(r"^ {10}(periodSeconds|timeoutSeconds|failureThreshold): ([0-9]+)$", nxt)
        if k:
            v[k.group(1)] = k.group(2)
    print(name, v.get("periodSeconds", "-"), v.get("timeoutSeconds", "-"), v.get("failureThreshold", "-"))
'
}

# The shell of the ready container's command, found by structure rather than by
# any of the text it is about to be compared against: `command` sorts before
# `name`, so the first eight-space `name:` after a block scalar names the
# container the block belongs to.
rendered_ready_script() {
  printf '%s\n' "$RENDERED" | python3 -c '
import re, sys
lines = sys.stdin.read().splitlines()
i = 0
while i < len(lines):
    if lines[i] != "        - |":
        i += 1
        continue
    j, block = i + 1, []
    while j < len(lines) and (lines[j].startswith(" " * 10) or lines[j] == ""):
        block.append(lines[j][10:])
        j += 1
    owner = next(
        (m.group(1) for m in (re.match(r"^ {8}name: (\S+)$", x) for x in lines[j:]) if m),
        None,
    )
    if owner == "ready":
        sys.stdout.write("\n".join(block).rstrip("\n") + "\n")
        sys.exit(0)
    i = j
sys.exit("no ready container with a shell command in the rendered manifest")
'
}

# Both ready scripts start with a host-specific assignment prologue and share
# everything from this line down.
ready_body() {
  awk 'f || /^test -s \/run\/e2b\/team-api-key \|\| \{$/ { f = 1; print }'
}

env_value() { sed -n "s/^$1=\([^ #]*\).*/\1/p" compose/.env; }

# compose.yaml reads a handful of knobs from the invoking shell as
# ${VAR:-default} (the team key, hugepages, preflight's free-space floor,
# FORCE_REBUILD, the two api secrets). A value exported in the developer's
# shell would change the render and fail a comparison for reasons that are not
# drift, so the parity test renders with every such knob unset. The list is
# derived from compose.yaml, so a new knob is covered without editing this
# file; `$${` is the wrapper's escaped shell and is not a knob.
compose_knobs() {
  # The pattern is literal (SC2016); comment lines are dropped first so prose
  # like "${VAR:-default}" is not taken for a knob.
  # shellcheck disable=SC2016
  grep -v '^[[:space:]]*#' compose/compose.yaml | grep -o '[^$]${[A-Z_][A-Z_0-9]*:-' | sed -E 's/.*\$\{([A-Z_0-9]+):-/\1/' | sort -u
}

compose_config_json_clean() {
  local knobs args=()
  knobs="$(compose_knobs)"
  [ "$(printf '%s\n' "$knobs" | grep -c .)" -ge 4 ] || { echo "compose_knobs found fewer than four knobs; the grep no longer matches compose.yaml" >&2; return 1; }
  while IFS= read -r k; do args+=(-u "$k"); done <<<"$knobs"
  env "${args[@]}" docker compose --project-directory compose config --format json
}

@test "kustomization renders" {
  [ -n "$RENDERED" ]
}

@test "image tags equal the pins in compose/.env" {
  for var in E2B_API_IMAGE E2B_DB_MIGRATOR_IMAGE E2B_CLIENT_PROXY_IMAGE E2B_CLICKHOUSE_MIGRATOR_IMAGE E2B_DASHBOARD_API_IMAGE E2B_DASHBOARD_IMAGE E2B_TOOLS_IMAGE E2B_NODE_E2B_IMAGE E2B_SEED_IMAGE; do
    image="$(env_value "$var")"
    [ -n "$image" ]
    echo "$RENDERED" | grep -q "image: $image\$" || { echo "missing $image"; return 1; }
  done
}

@test "artifact versions equal the pins in compose/.env" {
  for var in E2B_ORCHESTRATOR_VERSION E2B_ENVD_VERSION E2B_KERNEL_VERSION E2B_FIRECRACKER_VERSION E2B_BUSYBOX_VERSION; do
    value="$(env_value "$var")"
    echo "$RENDERED" | grep -q "$var: $value\$" || { echo "missing $var=$value"; return 1; }
  done
}

# Nothing else pins these four: kustomization.yaml overrides only the E2B
# images, and no .env variable stands behind them. A bump applied to one file
# alone is invisible until the two shapes run different store versions.
@test "the store images equal compose's" {
  compose_json="$(docker compose --project-directory compose config --format json)"
  for store in postgres redis clickhouse vector; do
    want="$(printf '%s' "$compose_json" | jq -r --arg s "$store" '.services[$s].image')"
    [ -n "$want" ] && [ "$want" != null ] || { echo "compose has no image for $store"; return 1; }
    got="$(awk -v n="        - name: $store" '
      $0 == n { f = 1; next }
      f && /^ +image: / { print $2; exit }' kubernetes/statefulset.yaml)"
    [ "$got" = "$want" ] || {
      echo "$store: the manifest pins $got, compose pins $want"
      return 1
    }
  done
}

@test "the config copies are the documented derivation of the source configs" {
  python3 compose/scripts/dev/sync-configs.py --print-k8s compose/config/clickhouse/config.xml > "$BATS_TEST_TMPDIR/clickhouse"
  cmp "$BATS_TEST_TMPDIR/clickhouse" kubernetes/config/clickhouse-config.xml
  python3 compose/scripts/dev/sync-configs.py --print-k8s compose/config/vector/vector.toml > "$BATS_TEST_TMPDIR/vector"
  cmp "$BATS_TEST_TMPDIR/vector" kubernetes/config/vector.toml
  python3 compose/scripts/dev/sync-configs.py --print-k8s compose/config/otel/otel-collector.yaml > "$BATS_TEST_TMPDIR/otel"
  cmp "$BATS_TEST_TMPDIR/otel" kubernetes/config/otel-collector.yaml
}

@test "every store binds loopback on the host network" {
  run grep -rn '0.0.0.0' kubernetes/config
  [ "$status" -eq 1 ]
  grep -q 'http://127.0.0.1:8123' kubernetes/config/vector.toml
  grep -q 'address = "127.0.0.1:20006"' kubernetes/config/vector.toml
  echo "$RENDERED" | grep -q 'http://127.0.0.1:20006'
  # 30006 is inside the default NodePort range; a NodePort allocated it would
  # DNAT the pod's own loopback log POSTs away from Vector.
  run grep -n ':30006' <<<"$RENDERED"
  [ "$status" -eq 1 ]
  grep -q '<listen_host>127.0.0.1</listen_host>' kubernetes/config/clickhouse-config.xml
  [ "$(cat kubernetes/config/clickhouse-docker-related-config.xml)" = '<clickhouse/>' ]
  echo "$RENDERED" | grep -q -- '- listen_addresses=127.0.0.1$'
  echo "$RENDERED" | grep -q -- '- --bind$'
  echo "$RENDERED" | grep -q 'docker_related_config.xml'
  # The dashboard is the one process in the pod meant to be reached from off
  # the node: a browser opens it at the node's address, so its server binds
  # every interface, the same HOSTNAME compose gives it. Nothing else may, and
  # the env-parity test below is what ties that one line to that container.
  run grep -n '0\.0\.0\.0' <<< "$RENDERED"
  [ "$status" -eq 0 ]
  [ "$(printf '%s\n' "$output" | grep -c .)" -eq 1 ]
  printf '%s\n' "$RENDERED" | grep -B1 '^ *value: 0\.0\.0\.0$' | grep -q '^ *- name: HOSTNAME$'
}

@test "the pod shares the host network and pid namespaces" {
  echo "$RENDERED" | grep -q '^      hostNetwork: true$'
  echo "$RENDERED" | grep -q '^      hostPID: true$'
}

@test "the data lives on the node under /var/lib/e2b/data and the pod is pinned to it" {
  for d in postgres clickhouse seed-state; do
    echo "$RENDERED" | grep -q "path: /var/lib/e2b/data/$d\$" || { echo "missing hostPath for $d"; return 1; }
  done
  echo "$RENDERED" | grep -q 'e2b.dev/single-node: "true"'
  run grep -c volumeClaimTemplates <<<"$RENDERED"
  [ "$status" -eq 1 ]
}

@test "the orchestrator requests the hugepages its sandboxes fault and sweeps the host cgroup tree" {
  echo "$RENDERED" | grep -q 'hugepages-2Mi: 4Gi'
  echo "$RENDERED" | grep -q 'OL_CGROUP_ROOT'
  echo "$RENDERED" | grep -q 'mountPath: /host-cgroup'
}

@test "every startup probe carries its compose healthcheck timings" {
  probes="$(startup_probes)"
  [ "$(printf '%s\n' "$probes" | grep -c .)" -eq 9 ]
  compose_json="$(docker compose --project-directory compose config --format json)"
  for service in postgres redis clickhouse vector orchestrator api client-proxy dashboard-api dashboard; do
    # A compose duration is "10s" here, but older plugins emit nanoseconds.
    want="$(printf '%s' "$compose_json" | jq -r --arg s "$service" '
      def secs: tostring
        | if test("^[0-9]+s$") then rtrimstr("s")
          elif test("^[0-9]+$") then (tonumber / 1000000000 | floor | tostring)
          else . end;
      .services[$s].healthcheck
      | "\(.interval | secs) \(.timeout | secs) \(.retries)"')"
    got="$(printf '%s\n' "$probes" | sed -n "s/^$service //p")"
    [ -n "$got" ] || { echo "$service has no startup probe in the manifest"; return 1; }
    [ "$got" = "$want" ] || {
      echo "$service: probe period/timeout/failures is '$got', compose interval/timeout/retries is '$want'"
      return 1
    }
  done
}

@test "the hugepages host-setup reserves are the hugepages the orchestrator requests" {
  pages="$(printf '%s\n' "$RENDERED" | grep -A1 '^ *- name: HUGEPAGES$' | sed -n 's/^ *value: "\([0-9]*\)"$/\1/p')"
  [ -n "$pages" ] || { echo "no HUGEPAGES value in the manifest"; return 1; }
  # HUGEPAGES is deliberately absent from compose/.env: compose.yaml carries
  # the number as the default of ${HUGEPAGES:-N}, and that default is what a
  # compose host reserves. Both shapes have to hand host-setup the same one.
  # shellcheck disable=SC2016  # the ${HUGEPAGES:-N} is compose's, matched literally
  compose_pages="$(sed -n 's/^ *HUGEPAGES: \${HUGEPAGES:-\([0-9]*\)}$/\1/p' compose/compose.yaml)"
  [ -n "$compose_pages" ] || { echo "no HUGEPAGES default in compose/compose.yaml"; return 1; }
  [ "$pages" = "$compose_pages" ] || {
    echo "the manifest hands host-setup $pages pages, compose.yaml defaults to $compose_pages"
    return 1
  }
  reserved_mib=$((pages * 2))
  quantities="$(printf '%s\n' "$RENDERED" | sed -n 's/^ *hugepages-2Mi: \(.*\)$/\1/p')"
  # request and limit, and a hugepage resource must have both.
  [ "$(printf '%s\n' "$quantities" | grep -c .)" -eq 2 ]
  for q in $quantities; do
    case "$q" in
      *Gi) requested_mib=$(( ${q%Gi} * 1024 )) ;;
      *Mi) requested_mib=${q%Mi} ;;
      *) echo "hugepages-2Mi '$q' is neither Mi nor Gi"; return 1 ;;
    esac
    [ "$requested_mib" -eq "$reserved_mib" ] || {
      echo "host-setup reserves $pages pages = ${reserved_mib} MiB, the orchestrator asks for $q = ${requested_mib} MiB"
      return 1
    }
  done
}

@test "the ready script is compose's ready script" {
  # `docker compose config` re-escapes a literal $ as $$ so its output is a
  # compose file again; undo that before comparing with the manifest's shell.
  docker compose --project-directory compose config --format json |
    jq -r '.services.ready.command[2]' |
    sed 's/\$\$/$/g' |
    ready_body > "$BATS_TEST_TMPDIR/compose-ready"
  rendered_ready_script | ready_body > "$BATS_TEST_TMPDIR/k8s-ready"
  [ -s "$BATS_TEST_TMPDIR/compose-ready" ]
  [ -s "$BATS_TEST_TMPDIR/k8s-ready" ]
  # jq -r adds a newline to a string that already ends in one.
  printf '%s\n' "$(cat "$BATS_TEST_TMPDIR/compose-ready")" > "$BATS_TEST_TMPDIR/compose-ready.trimmed"
  printf '%s\n' "$(cat "$BATS_TEST_TMPDIR/k8s-ready")" > "$BATS_TEST_TMPDIR/k8s-ready.trimmed"
  run diff -u "$BATS_TEST_TMPDIR/compose-ready.trimmed" "$BATS_TEST_TMPDIR/k8s-ready.trimmed"
  [ "$status" -eq 0 ] || { echo "compose ready (-) against the manifest's (+):"; echo "$output"; return 1; }
}

# The parity test above compares only the shared body, so a syntax error in
# either prologue or in the epilogue it assigns slips past it. `/bin/sh` is
# what both containers hand the script to; `-n` is that same parser, stopped
# before it runs the loop.
@test "both ready scripts parse" {
  rendered_ready_script > "$BATS_TEST_TMPDIR/k8s-ready"
  [ -s "$BATS_TEST_TMPDIR/k8s-ready" ]
  run sh -n "$BATS_TEST_TMPDIR/k8s-ready"
  [ "$status" -eq 0 ] || { echo "the manifest's ready script does not parse:"; echo "$output"; return 1; }

  # As above: `docker compose config` writes a literal $ back out as $$.
  docker compose --project-directory compose config --format json |
    jq -r '.services.ready.command[2]' |
    sed 's/\$\$/$/g' > "$BATS_TEST_TMPDIR/compose-ready"
  [ -s "$BATS_TEST_TMPDIR/compose-ready" ]
  run sh -n "$BATS_TEST_TMPDIR/compose-ready"
  [ "$status" -eq 0 ] || { echo "compose's ready script does not parse:"; echo "$output"; return 1; }
}

# compose.yaml and the StatefulSet configure the same processes, so a variable
# one shape sets and the other does not, or sets to a different value, is a
# behavioural difference between two installs meant to be the same stack.
# Every difference that is deliberate is written out below as the exact pair
# of values the two sides carry, so a drift on either side fails here, and an
# entry that stops being needed fails as unused rather than quietly excusing a
# key forever.
#
# The manifest is read as text again (see the note at the top of this file).
# An `envFrom` is folded in and a `configMapKeyRef` resolved against the
# generated ConfigMap, so the versions the pod takes from e2b-versions compare
# as the literals compose sets. A `secretKeyRef` and a `fieldRef` have no
# literal to resolve to and compare as a marker naming the source.
@test "the pod's env blocks are compose's" {
  printf '%s\n' "$RENDERED" > "$BATS_TEST_TMPDIR/rendered.yaml"
  # The api's two secrets are generated per install, so a value in the
  # operator's own shell would otherwise be substituted into the render and
  # compared against the pod's Secret reference.
  compose_config_json_clean \
    > "$BATS_TEST_TMPDIR/compose.json"
  run python3 - "$BATS_TEST_TMPDIR/rendered.yaml" "$BATS_TEST_TMPDIR/compose.json" <<'PY'
import json, re, sys

# The deliberate differences, each as (what compose has, what the pod has).
# None on a side means any value, and is used only where another test already
# pins that side.
ALLOWED = {
    # The api's two secrets are per install. Under compose the api-secrets
    # service generates the pair into the seed-state volume and the api's
    # entrypoint reads it from there, which leaves both empty in the render;
    # the pod takes them from the e2b-api Secret instead.
    ("api", "ADMIN_TOKEN"):
        ("", "<secret:e2b-api/ADMIN_TOKEN>"),
    ("api", "SANDBOX_ACCESS_TOKEN_HASH_SEED"):
        ("", "<secret:e2b-api/SANDBOX_ACCESS_TOKEN_HASH_SEED>"),

    # dashboard-api holds the same admin token as the api and takes it the
    # same way each shape does for the api: the compose wrapper reads it from
    # the seed-state file, the pod from the e2b-api Secret.
    ("dashboard-api", "ADMIN_TOKEN"):
        ("", "<secret:e2b-api/ADMIN_TOKEN>"),

    # The browser has to reach client-proxy at an address it can open. Compose
    # renders the E2B_DASHBOARD_HOST knob's default; the pod substitutes the
    # node's own IP, the same address ready prints, through a fieldRef that
    # compose has no counterpart for.
    ("dashboard", "PUBLIC_SANDBOX_URL"):
        ("http://localhost:3002", "http://$(NODE_IP):3002"),
    ("dashboard", "NODE_IP"): ("<absent>", "<field:status.hostIP>"),

    # Vector's HTTP source listens on 30006 under compose and on 20006 in the
    # pod: 30006 is inside the default NodePort range, and a NodePort that
    # allocated it would DNAT the pod's own loopback log POSTs away from
    # Vector. The loopback test above holds the manifest to 20006.
    ("api", "LOGS_COLLECTOR_ADDRESS"):
        ("http://127.0.0.1:30006", "http://127.0.0.1:20006"),
    ("orchestrator", "LOGS_COLLECTOR_ADDRESS"):
        ("http://127.0.0.1:30006", "http://127.0.0.1:20006"),

    # compose leaves these three on its own network, so they address the
    # stores by compose service name. Every container in the pod shares the
    # host's network namespace and reaches the stores on loopback -- and so do
    # the compose services that already run with network_mode: host, which is
    # why the api, the orchestrator and client-proxy need no entry here.
    ("db-migrator", "POSTGRES_CONNECTION_STRING"): (
        "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable",
        "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"),
    ("seed", "POSTGRES_CONNECTION_STRING"): (
        "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable",
        "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"),
    ("clickhouse-migrator", "GOOSE_DBSTRING"): (
        "clickhouse://clickhouse:clickhouse@clickhouse:9000/default",
        "clickhouse://clickhouse:clickhouse@127.0.0.1:9000/default"),

    # The two stamps are inert; they exist so that `up` recreates a container
    # whose inline config changed, which compose does not do on its own.
    # kustomize content-hashes the ConfigMap names instead, so the pod rolls
    # without them. The compose side is a hash of a file, and
    # tests/config-hashes.bats is what keeps it right.
    ("clickhouse", "CLICKHOUSE_CONFIG_SHA256"): (None, "<absent>"),
    ("vector", "VECTOR_CONFIG_SHA256"): (None, "<absent>"),

    # compose gives the orchestrator `cgroup: host`, so the launcher's sweep
    # finds the sandbox cgroups at its own default, /sys/fs/cgroup/e2b. The
    # pod reaches the same host tree through the /host-cgroup mount.
    ("orchestrator", "OL_CGROUP_ROOT"): ("<absent>", "/host-cgroup/e2b"),

    # The SDK variables ready writes have to name an address its reader can
    # reach: localhost for compose, the node's own IP for a pod on the host
    # network. That assignment is the prologue the ready-script test above
    # excludes, for the same reason.
    ("ready", "NODE_IP"): ("<absent>", "<field:status.hostIP>"),
}

manifest = open(sys.argv[1]).read().splitlines()
compose = json.load(open(sys.argv[2]))["services"]

# The generated ConfigMaps, keyed by the rendered (hash-suffixed) name.
maps, data, i = {}, None, 0
while i < len(manifest):
    if manifest[i] == "data:":
        data, i = {}, i + 1
        while i < len(manifest) and re.match(r"^  \S", manifest[i]):
            m = re.match(r"^  ([A-Za-z0-9_.-]+): (.*)$", manifest[i])
            if m:
                data[m.group(1)] = m.group(2)
            i += 1
        continue
    m = re.match(r"^  name: (\S+)$", manifest[i])
    if m and data is not None:
        maps[m.group(1)], data = data, None
    i += 1

# A container is a list item under containers:/initContainers:, so it opens
# with `      - <key>:`. kustomize puts a sequence at its parent key's
# indentation, so both the container's own keys and its env entries sit at
# eight spaces; only the entries carry the `- `.
blocks, cur = [], None
for line in manifest:
    if re.match(r"^      - \S", line):
        cur = ["        " + line[8:]]
        blocks.append(cur)
    elif re.match(r"^ {0,7}\S", line):
        cur = None
    elif cur is not None:
        cur.append(line)


def section(block, key):
    """The lines under this container's eight-space `key:`."""
    out = []
    for i, line in enumerate(block):
        if line != "        " + key + ":":
            continue
        for nxt in block[i + 1:]:
            if re.match(r"^ {8}[A-Za-z]", nxt):
                break
            out.append(nxt)
    return out


def unquote(text):
    if len(text) > 1 and text[0] == text[-1] and text[0] in "\"'":
        return text[1:-1]
    return text


def value_of(chunk):
    """The literal an env entry carries, or a marker for what it points at."""
    for line in chunk:
        m = re.match(r"^ {10}value: (.*)$", line)
        if m:
            return unquote(m.group(1))
    ref = {}
    for line in chunk:
        m = re.match(r"^ {14}(key|name|fieldPath): (\S+)$", line)
        if m:
            ref[m.group(1)] = m.group(2)
    body = "\n".join(chunk)
    if "configMapKeyRef" in body:
        # kustomize quotes a map value only where YAML needs it, as it does
        # the empty collector endpoint.
        return unquote(maps.get(ref.get("name"), {}).get(ref.get("key"), "<configmap?>"))
    if "secretKeyRef" in body:
        return "<secret:%s/%s>" % (ref.get("name"), ref.get("key"))
    if "fieldRef" in body:
        return "<field:%s>" % ref.get("fieldPath")
    return "<?>"


containers = {}
for block in blocks:
    if not any(re.match(r"^ {8}image: ", l) for l in block):
        continue                      # a volume, not a container
    name = next((m.group(1) for m in
                 (re.match(r"^ {8}name: (\S+)$", l) for l in block) if m), None)
    env = {}
    for line in section(block, "envFrom"):
        m = re.match(r"^ {12}name: (\S+)$", line)
        if m:
            env.update(maps.get(m.group(1), {}))
    lines, i = section(block, "env"), 0
    while i < len(lines):
        m = re.match(r"^ {8}- name: (\S+)$", lines[i])
        if not m:
            i += 1
            continue
        key, i, chunk = m.group(1), i + 1, []
        while i < len(lines) and not re.match(r"^ {8}- name: ", lines[i]):
            chunk.append(lines[i])
            i += 1
        env[key] = value_of(chunk)
    containers[name] = env

problems, used = [], set()
for name in sorted(set(containers) & set(compose)):
    pod = containers[name]
    yml = dict((k, "" if v is None else str(v))
               for k, v in (compose[name].get("environment") or {}).items())
    for key in sorted(set(pod) | set(yml)):
        here, there = yml.get(key, "<absent>"), pod.get(key, "<absent>")
        if here == there:
            continue
        if (name, key) not in ALLOWED:
            problems.append("%s %s: compose says %r, the manifest %r"
                            % (name, key, here, there))
            continue
        used.add((name, key))
        want_here, want_there = ALLOWED[(name, key)]
        if (want_here is not None and want_here != here) or \
           (want_there is not None and want_there != there):
            problems.append(
                "%s %s: the allowed difference is %r against %r, the files now "
                "say %r against %r" % (name, key, want_here, want_there,
                                       here, there))
for name, key in sorted(set(ALLOWED) - used):
    problems.append("%s %s is allowlisted but the two shapes agree on it now; "
                    "drop the entry" % (name, key))

# A parser that stopped matching would pass every container vacuously.
for name in ("api", "orchestrator", "client-proxy", "postgres", "clickhouse",
             "vector", "ready", "dashboard", "dashboard-api"):
    if name not in containers:
        problems.append("no %s container was parsed out of the manifest" % name)

# The collector endpoint is empty by default on both shapes, so the comparison
# above would also pass with the variable missing from both. Every service
# that exports telemetry has to carry it, on each side.
for name in ("api", "orchestrator", "client-proxy", "dashboard-api"):
    if "OTEL_COLLECTOR_GRPC_ENDPOINT" not in containers.get(name, {}):
        problems.append("the manifest's %s has no OTEL_COLLECTOR_GRPC_ENDPOINT" % name)
    if "OTEL_COLLECTOR_GRPC_ENDPOINT" not in (compose.get(name, {}).get("environment") or {}):
        problems.append("compose's %s has no OTEL_COLLECTOR_GRPC_ENDPOINT" % name)

if problems:
    sys.exit("\n".join(problems))
PY
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }
}

# The dashboard pair has to be the last two init containers. A native sidecar
# (an init container with restartPolicy: Always) gates the next init container
# on its startup probe, so a dashboard that never turns healthy would stop the
# base template from being built if it sat any earlier. On compose only `ready`
# waits for the dashboard, and this is what keeps the two shapes agreeing.
# Nothing else pins the order: kustomize renders the list either way.
@test "the dashboard pair is the last two init containers" {
  names="$(awk '/^      initContainers:$/ { f = 1; next }
                /^      [a-zA-Z]/         { f = 0 }
                f && /^        - name: /  { print $3 }' kubernetes/statefulset.yaml)"
  # A parser that stopped matching would pass the test vacuously.
  [ -n "$names" ]

  diff <(printf '%s\n' "$names" | tail -3) \
       <(printf '%s\n' base-template dashboard-api dashboard) || {
    echo "the init containers now end (-) where they must end (+)"
    return 1
  }
}

# The patch line kustomization.yaml ships commented, turned on in a scratch
# copy of the directory so the checked-in file stays as it is.
PATCH_LINE='patches: [{ path: patches/otel-collector.yaml, target: { kind: StatefulSet, name: e2b } }]'

render_with_collector() {
  local dir="$BATS_TEST_TMPDIR/kubernetes"
  cp -R kubernetes "$dir"
  awk -v line="$PATCH_LINE" '$0 == "# " line { $0 = line } { print }' \
    kubernetes/kustomization.yaml > "$dir/kustomization.yaml"
  grep -qxF "$PATCH_LINE" "$dir/kustomization.yaml" || {
    echo "kustomization.yaml has no '# $PATCH_LINE' line to uncomment" >&2
    return 1
  }
  kubectl kustomize "$dir"
}

# The built-in collector is opt-in: without the patch there is no container
# for it, but its ConfigMap is generated either way, so turning it on is the
# one commented line.
@test "the collector's ConfigMap is generated and its container is not" {
  grep -qxF "# $PATCH_LINE" kubernetes/kustomization.yaml
  run grep -n '^        name: otel-collector$' <<<"$RENDERED"
  [ "$status" -eq 1 ]
  printf '%s\n' "$RENDERED" | grep -q '^  name: otel-config-[a-z0-9]*$'
}

# With the patch the collector is one more native sidecar, after the dashboard
# pair, on the image compose pins, reading the generated ConfigMap at the path
# its --config names, and probed over HTTP since the image has no shell.
@test "the patch line adds the collector as the last sidecar with its config mounted" {
  render_with_collector > "$BATS_TEST_TMPDIR/patched.yaml"
  image="$(docker compose --project-directory compose --profile otel config --format json |
    jq -r '.services["otel-collector"].image')"
  [ -n "$image" ] && [ "$image" != null ]
  run python3 - "$BATS_TEST_TMPDIR/patched.yaml" "$image" <<'PY'
import re, sys

lines = open(sys.argv[1]).read().splitlines()
image = sys.argv[2]
problems = []


def blocks_under(key):
    """The list items under the pod spec's six-space `key:`."""
    start = lines.index("      %s:" % key)
    out, cur = [], None
    for line in lines[start + 1:]:
        if line.startswith("      - "):
            cur = [line]
            out.append(cur)
        elif re.match(r"^ {0,7}\S", line):
            break
        elif cur is not None:
            cur.append(line)
    return out


def name_of(block, indent):
    for line in block:
        m = re.match(r"^ {%d}name: (\S+)$" % indent, line)
        if m:
            return m.group(1)
    return None


inits = blocks_under("initContainers")
names = [name_of(b, 8) for b in inits]
if names[-3:] != ["dashboard-api", "dashboard", "otel-collector"]:
    problems.append("the init containers end %r" % names[-3:])
collector = inits[names.index("otel-collector")] if "otel-collector" in names else []
text = "\n".join(collector)
for want in ("        image: %s" % image,
             "        restartPolicy: Always",
             "        - --config=/etc/otelcol-contrib/config.yaml",
             "        - mountPath: /etc/otelcol-contrib/config.yaml\n          name: otel-config\n          subPath: config.yaml",
             "          httpGet:\n            host: 127.0.0.1\n            path: /\n            port: 13133"):
    if want not in text:
        problems.append("the collector container lacks %r" % want)

volumes = {name_of(b, 8): "\n".join(b) for b in blocks_under("volumes")}
m = re.search(r"^ {10}name: (otel-config-\S+)$", volumes.get("otel-config", ""), re.M)
if not m:
    problems.append("no otel-config volume naming the generated ConfigMap")
elif "  name: %s" % m.group(1) not in lines:
    problems.append("the otel-config volume names %s, which is not rendered" % m.group(1))

if problems:
    sys.exit("\n".join(problems))
PY
  [ "$status" -eq 0 ] || { echo "$output"; return 1; }

  # Still one wildcard bind, the dashboard's: the collector adds none.
  [ "$(grep -c '0\.0\.0\.0' "$BATS_TEST_TMPDIR/patched.yaml")" -eq 1 ]
}
