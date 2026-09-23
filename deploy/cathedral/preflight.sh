#!/bin/sh
set -eu

usage() {
  echo "usage: $0 --url HTTPS_URL --api-key-file FILE --template-id ID --build-id ID --cpu N --memory-gib N" >&2
  exit 2
}

url=
key_file=
template_id=
build_id=
cpu=
memory_gib=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --url) [ "$#" -ge 2 ] || usage; url=$2; shift 2 ;;
    --api-key-file) [ "$#" -ge 2 ] || usage; key_file=$2; shift 2 ;;
    --template-id) [ "$#" -ge 2 ] || usage; template_id=$2; shift 2 ;;
    --build-id) [ "$#" -ge 2 ] || usage; build_id=$2; shift 2 ;;
    --cpu) [ "$#" -ge 2 ] || usage; cpu=$2; shift 2 ;;
    --memory-gib) [ "$#" -ge 2 ] || usage; memory_gib=$2; shift 2 ;;
    *) usage ;;
  esac
done

[ -n "$url" ] && [ -n "$key_file" ] && [ -n "$template_id" ] && \
  [ -n "$build_id" ] && \
  [ -n "$cpu" ] && [ -n "$memory_gib" ] || usage
case "$url" in https://*) ;; *) echo "runtime URL must use HTTPS" >&2; exit 1 ;; esac
[ -r "$key_file" ] || { echo "API key file is not readable" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
jq -en --arg value "$cpu" '$value | test("^[1-9][0-9]{0,3}$")' >/dev/null && \
  [ "$cpu" -le 1024 ] || { echo "CPU must be a canonical integer from 1 to 1024" >&2; exit 1; }
jq -en --arg value "$memory_gib" '$value | test("^[1-9][0-9]{0,6}$")' >/dev/null && \
  [ "$memory_gib" -le 1048576 ] || { echo "memory GiB must be a canonical integer from 1 to 1048576" >&2; exit 1; }

umask 077
api_key=$(tr -d '\r\n' < "$key_file")
[ -n "$api_key" ] || { echo "API key file is empty" >&2; exit 1; }
base=${url%/}
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
header_file="$tmp_dir/curl-headers"
printf 'X-API-Key: %s\n' "$api_key" > "$header_file"
unset api_key

curl -q --fail-with-body --silent --show-error --proto '=https' \
  --proto-redir '=https' --max-redirs 0 --connect-timeout 3 --max-time 10 \
  --header "@$header_file" \
  "$base/v1/cathedral/capabilities" > "$tmp_dir/capabilities.json"

jq -e '.schema == 1 and .durable_create_idempotency == true and
  .operation_lookup == true and .durable_lifecycle_operations == true and
  .safe_delete == true and .safe_pause == true and
  .preserves_remaining_lifetime == true and .execution_identity == true' \
  "$tmp_dir/capabilities.json" >/dev/null || {
  echo "runtime capability contract does not match Cathedral schema 1" >&2
  jq '{schema, durable_create_idempotency, operation_lookup, durable_lifecycle_operations, safe_delete, safe_pause, preserves_remaining_lifetime, execution_identity, safe_fork}' "$tmp_dir/capabilities.json" >&2
  exit 1
}

curl -q --fail-with-body --silent --show-error --proto '=https' \
  --proto-redir '=https' --max-redirs 0 --connect-timeout 3 --max-time 10 \
  --header "@$header_file" \
  "$base/v2/templates?limit=100" > "$tmp_dir/templates.json"

memory_mb=$((memory_gib * 1024))
count=$(jq --arg id "$template_id" '[.[] | select(.templateID == $id)] | length' "$tmp_dir/templates.json")
[ "$count" -eq 1 ] || {
  echo "expected exactly one visible template with the requested ID; found $count" >&2
  exit 1
}

jq -e --arg id "$template_id" --arg build "$build_id" --argjson cpu "$cpu" --argjson memory "$memory_mb" '
  .[] | select(.templateID == $id) |
  .cpuCount == $cpu and .memoryMB == $memory and .buildStatus == "ready" and
  .buildID == $build and
  (.envdVersion | type == "string" and length > 0)
' "$tmp_dir/templates.json" >/dev/null || {
  echo "template shape, build status, build ID, or envd version does not match" >&2
  jq --arg id "$template_id" '.[] | select(.templateID == $id) | {templateID, buildID, cpuCount, memoryMB, buildStatus, envdVersion}' "$tmp_dir/templates.json" >&2
  exit 1
}

jq --arg id "$template_id" '.[] | select(.templateID == $id) | {templateID, buildID, cpuCount, memoryMB, buildStatus, envdVersion}' "$tmp_dir/templates.json"
echo "read-only Cathedral runtime preflight passed"
