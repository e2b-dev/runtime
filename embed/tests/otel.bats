#!/usr/bin/env bats

# One setting points the stack's telemetry at an OpenTelemetry collector:
# E2B_OTEL_COLLECTOR_GRPC_ENDPOINT in .env becomes OTEL_COLLECTOR_GRPC_ENDPOINT
# on the four services that export (api, orchestrator, client-proxy and
# dashboard-api). Empty, the default, is the runtime's "telemetry off", which
# is how the stack ran before the setting existed. The built-in collector is
# one more service behind the otel profile, off unless COMPOSE_PROFILES turns
# it on, and nothing waits for it.
#
# Rendering needs Docker with the compose plugin and jq, as secrets.bats does.
# Every render unsets the setting and COMPOSE_PROFILES first, so a value
# exported in the runner's own shell is not what these assertions see.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

# render [NAME=value ...]: the compose file as JSON with every profile on, so
# the services behind one are checked too, and with the setting unset unless
# an assignment gives it.
render() {
  env -u E2B_OTEL_COLLECTOR_GRPC_ENDPOINT -u COMPOSE_PROFILES "$@" \
    docker compose --project-directory compose --profile '*' config --format json
}

# services [NAME=value ...]: the services Compose would run, one per line, with
# the profiles the assignments (or the shipped .env) turn on.
services() {
  env -u E2B_OTEL_COLLECTOR_GRPC_ENDPOINT -u COMPOSE_PROFILES "$@" \
    docker compose --project-directory compose config --services
}

# endpoints [NAME=value ...]: `service value` for every service that carries
# OTEL_COLLECTOR_GRPC_ENDPOINT, sorted by service.
endpoints() {
  render "$@" | jq -r '.services | to_entries[]
    | select(.value.environment | has("OTEL_COLLECTOR_GRPC_ENDPOINT"))
    | "\(.key) \(.value.environment.OTEL_COLLECTOR_GRPC_ENDPOINT)"' | sort
}

# Turning the built-in collector on is these two lines, uncommented. Compose
# reads COMPOSE_PROFILES from the project's .env itself, so the second one
# needs nothing in compose.yaml.
@test ".env ships the endpoint and the otel profile as commented lines" {
  grep -qx '#E2B_OTEL_COLLECTOR_GRPC_ENDPOINT=127.0.0.1:4317' compose/.env
  grep -qx '#COMPOSE_PROFILES=otel' compose/.env
  run grep -nE '^(E2B_OTEL_COLLECTOR_GRPC_ENDPOINT|COMPOSE_PROFILES)=' compose/.env
  [ "$status" -eq 1 ]
}

@test "the endpoint reaches exactly api, orchestrator, client-proxy and dashboard-api" {
  got="$(endpoints E2B_OTEL_COLLECTOR_GRPC_ENDPOINT=127.0.0.1:4317)"
  want="$(printf '%s 127.0.0.1:4317\n' api client-proxy dashboard-api orchestrator)"
  [ "$got" = "$want" ] || { echo "services carrying the endpoint:"; echo "$got"; return 1; }
}

# An empty string, and not null: a null value is Compose's "take it from the
# host environment", which would hand the services whatever the operator
# happens to export instead of what .env says.
@test "unset, the four carry an empty endpoint, which the runtime reads as telemetry off" {
  got="$(endpoints)"
  want="$(printf '%s \n' api client-proxy dashboard-api orchestrator)"
  [ "$got" = "$want" ] || { echo "services carrying the endpoint:"; echo "$got"; return 1; }
  types="$(render | jq -r '[.services[] | .environment
    | select(has("OTEL_COLLECTOR_GRPC_ENDPOINT")) | .OTEL_COLLECTOR_GRPC_ENDPOINT | type]
    | unique | join(" ")')"
  [ "$types" = string ]
}

@test "the collector runs only with the otel profile" {
  run services
  [ "$status" -eq 0 ]
  # A render that listed nothing would pass the absence check vacuously.
  printf '%s\n' "$output" | grep -qx ready
  run grep -x otel-collector <<<"$output"
  [ "$status" -eq 1 ]

  run services COMPOSE_PROFILES=otel
  [ "$status" -eq 0 ]
  printf '%s\n' "$output" | grep -qx otel-collector
}

# The operator's path: the shipped .env with its profile line uncommented, and
# nothing set in the shell.
@test "uncommenting the .env line is what turns the profile on" {
  dir="$BATS_TEST_TMPDIR/compose"
  mkdir "$dir"
  cp compose/compose.yaml "$dir/"
  sed 's/^#COMPOSE_PROFILES=otel$/COMPOSE_PROFILES=otel/' compose/.env > "$dir/.env"
  grep -qx 'COMPOSE_PROFILES=otel' "$dir/.env"
  run env -u COMPOSE_PROFILES docker compose --project-directory "$dir" config --services
  [ "$status" -eq 0 ]
  printf '%s\n' "$output" | grep -qx otel-collector
}

# `up --wait` must not hang on a collector the operator did not ask for, and a
# collector that fails must not take the stack's readiness with it.
@test "nothing waits for the collector, ready included" {
  json="$(render)"
  [ "$(printf '%s' "$json" | jq -r '.services | has("otel-collector")')" = true ]
  [ "$(printf '%s' "$json" | jq -r '.services.ready.depends_on | has("otel-collector")')" = false ]
  waiting="$(printf '%s' "$json" | jq -r '.services | to_entries[]
    | select((.value.depends_on // {}) | has("otel-collector")) | .key')"
  [ -z "$waiting" ] || { echo "these services depend on otel-collector: $waiting"; return 1; }
}

@test "the collector is pinned inline and runs on the host network with its config mounted" {
  json="$(render)"
  svc() { printf '%s' "$json" | jq -r ".services[\"otel-collector\"]$1"; }
  # Pinned in compose.yaml like the four stores, not through .env.
  grep -qx '    image: otel/opentelemetry-collector-contrib:[0-9][0-9.]*' compose/compose.yaml
  [ "$(svc .image)" = otel/opentelemetry-collector-contrib:0.146.0 ]
  [ "$(svc '.profiles | join(" ")')" = otel ]
  [ "$(svc .network_mode)" = host ]
  [ "$(svc '.ports // [] | length')" = 0 ]
  [ "$(svc '.configs[0].source')" = otel-collector-config ]
  target="$(svc '.configs[0].target')"
  [ "$(svc '.command | join(" ")')" = "--config=$target" ]
}

# The collector shares the machine's network with the services, so its own
# listeners are the only thing keeping 4317 and 13133 off the network. The
# address .env ships is the receiver's, so uncommenting both lines connects
# the services to it. Those two are its only listeners: left at its default,
# the collector's internal telemetry serves its own metrics on 8888, a port
# the config never names and that a taken port would stop it starting on.
@test "the collector config binds loopback only, where .env points" {
  config=compose/config/otel/otel-collector.yaml
  run grep -n '0\.0\.0\.0' "$config"
  [ "$status" -eq 1 ]
  grep -qx '        endpoint: 127.0.0.1:4317' "$config"
  grep -qx '    endpoint: 127.0.0.1:13133' "$config"
  others="$(grep -E '^[[:space:]]*endpoint:' "$config" |
    grep -vE 'endpoint: (tcp://)?127\.0\.0\.1:[0-9]+$' || true)"
  [ -z "$others" ] || { echo "endpoints off loopback: $others"; return 1; }
  telemetry="$(awk '/^  telemetry:$/ { f = 1; next } f && !/^    / { f = 0 } f && !/^ *#/' "$config")"
  want="$(printf '%s\n' '    logs:' '      level: warn' '    metrics:' '      level: none')"
  [ "$telemetry" = "$want" ] || { echo "service.telemetry:"; echo "$telemetry"; return 1; }
  shipped="$(sed -n 's/^#E2B_OTEL_COLLECTOR_GRPC_ENDPOINT=//p' compose/.env)"
  [ "$shipped" = 127.0.0.1:4317 ]
}

# pipeline NAME: the lines of that pipeline under service.pipelines, without
# its comments. Only there: service.telemetry has a `logs:` key too.
pipeline() {
  awk -v name="    $1:" '
    /^  pipelines:$/ { p = 1; next }
    p && /./ && !/^   / { p = 0 }
    p && $0 == name { f = 1; next }
    f && !/^      / { f = 0 }
    f && !/^ *#/' compose/config/otel/otel-collector.yaml
}

# Metrics into the stack's own ClickHouse, through the filter, so nothing but
# the e2b.* metrics gets there. Traces and logs have pipelines too, ones that
# accept them and discard them: without those the receiver answers
# Unimplemented and every service logs each batch it refused.
@test "the collector writes the e2b metrics into ClickHouse and discards the rest" {
  config=compose/config/otel/otel-collector.yaml
  pipelines="$(awk '/^  pipelines:$/ { f = 1; next } f && /^    [a-z]/ { sub(":$", "", $1); print $1 }' "$config" | xargs)"
  [ "$pipelines" = "metrics/external traces logs" ] || { echo "pipelines: $pipelines"; return 1; }

  block="$(pipeline metrics/external)"
  want="$(printf '%s\n' '      receivers: [otlp]' '      processors: [filter/external_metrics, batch/clickhouse]' '      exporters: [clickhouse]')"
  [ "$block" = "$want" ] || { echo "metrics/external:"; echo "$block"; return 1; }
  for signal in traces logs; do
    block="$(pipeline "$signal")"
    want="$(printf '%s\n' '      receivers: [otlp]' '      processors: [batch]' '      exporters: [nop]')"
    [ "$block" = "$want" ] || { echo "$signal:"; echo "$block"; return 1; }
  done
  grep -qx '  nop:' "$config"
  # Forwarding stays the operator's choice: the upstream exporter ships commented.
  run grep -n '^ *otlp/upstream:' "$config"
  [ "$status" -eq 1 ]

  grep -qx '    endpoint: tcp://127.0.0.1:9000' "$config"
  grep -qx '    create_schema: false' "$config"
  grep -qx '        name: metrics_gauge' "$config"
  grep -qx '        name: metrics_sum' "$config"
  grep -qx '          - "e2b.\*"' "$config"
}
