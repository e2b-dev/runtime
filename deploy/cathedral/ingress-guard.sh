#!/usr/bin/env bash
set -euo pipefail

# All Compose control-plane listeners use network_mode: host and bind 0.0.0.0.
# On this dedicated host, reject non-loopback ingress before starting Compose.
# The host's reverse proxy or a tunnel may connect over loopback.
table=cathedral_proof
ports='{ 3000, 3001, 3002, 3003, 3010, 5007, 5008, 5009, 5109, 6060, 6061, 30006 }'
expected_ports='[3000,3001,3002,3003,3010,5007,5008,5009,5109,6060,6061,30006]'
case "${1:-}" in
  install)
    test "$(id -u)" -eq 0 || { echo 'run ingress guard as root' >&2; exit 1; }
    command -v nft >/dev/null || { echo 'nft is required on the proof host' >&2; exit 1; }
    if nft list table inet "$table" >/dev/null 2>&1; then
      "$0" check
      exit 0
    fi
    nft add table inet "$table"
    nft 'add chain inet cathedral_proof input { type filter hook input priority -100; policy accept; }'
    nft add rule inet "$table" input iifname != lo tcp dport "$ports" drop
    "$0" check
    ;;
  check)
    command -v nft >/dev/null || { echo 'nft is required on the proof host' >&2; exit 1; }
    command -v jq >/dev/null || { echo 'jq is required on the proof host' >&2; exit 1; }
    rules=$(nft -j -y list chain inet "$table" input) || { echo 'Cathedral ingress guard is absent' >&2; exit 1; }
    jq -e --argjson ports "$expected_ports" '
      [.nftables[] | select(has("chain")) | .chain] as $chains |
      [.nftables[] | select(has("rule")) | .rule] as $rules |
      ($chains | length) == 1 and ($rules | length) == 1 and
      ($chains[0] | .family == "inet" and .table == "cathedral_proof" and
        .name == "input" and .type == "filter" and .hook == "input" and
        .prio == -100 and .policy == "accept") and
      ($rules[0] | .family == "inet" and .table == "cathedral_proof" and
        .chain == "input" and (.expr | length) == 3 and
        .expr[0] == {"match":{"op":"!=","left":{"meta":{"key":"iifname"}},"right":"lo"}} and
        (.expr[1] | keys) == ["match"] and
        (.expr[1].match | keys) == ["left","op","right"] and
        .expr[1].match.op == "==" and
        .expr[1].match.left == {"payload":{"protocol":"tcp","field":"dport"}} and
        (.expr[1].match.right | keys) == ["set"] and
        ((.expr[1].match.right.set | sort) == ($ports | sort)) and
        .expr[2] == {"drop":null})
    ' <<< "$rules" >/dev/null || { echo 'Cathedral ingress guard semantics do not match' >&2; exit 1; }
    ;;
  *) echo "usage: $0 install|check" >&2; exit 2 ;;
esac
