#!/usr/bin/env bash
# Model-checks SandboxStore.tla with TLC.
#
#   ./check.sh                       the committed model, every invariant
#   ./check.sh NAME=VALUE ...        same, with those constants overridden
#                                    (e.g. RemoveDeletesBeforeSrem=TRUE for mutation (a))
#   INVARIANTS="A B" ./check.sh ...  check only the invariants named
#
# Exit status is TLC's: 0 when no error is found, non-zero with the trace on a
# violation. When TLA2TOOLS_JAR names a TLC 2.19 tla2tools.jar, java (a JRE,
# 11 or later, on PATH) runs it; else TLC is taken from PATH (tlc), else run
# through nix from a pinned nixpkgs revision.
#
# TLC runs with -deadlock, which turns its deadlock check off: every run of this
# model ends in a state with no enabled action once the fault budgets are spent
# and the pods are idle, and that end is not a bug.
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
spec="$dir/SandboxStore.tla"
cfg="$dir/SandboxStore.cfg"

nixpkgs_rev=58deea654ec5dd0644f31c840a07c29fa0592528   # nixpkgs-unstable, tlaplus 1.7.4 = TLC 2.19

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

effective="$work/SandboxStore.cfg"
cp "$cfg" "$effective"
for arg in "$@"; do
  name=${arg%%=*}
  value=${arg#*=}
  if [ "$name" = "$arg" ] || ! grep -Eq "^  ${name} = " "$effective"; then
    echo "check.sh: unknown constant '$arg'; expected NAME=VALUE with NAME from $cfg" >&2
    exit 2
  fi
  sed -i.bak -E "s|^  ${name} = .*|  ${name} = ${value}|" "$effective"
done
if [ -n "${INVARIANTS:-}" ]; then
  sed -i.bak -E '/^INVARIANTS$/,$d' "$effective"
  { echo "INVARIANTS"; for inv in $INVARIANTS; do echo "  $inv"; done; } >> "$effective"
fi
rm -f "$effective.bak"
cp "$spec" "$work/"

tlc_args=(-workers auto -deadlock -metadir "$work/states" -config "$effective" "$work/SandboxStore.tla")
# TLC extracts the standard modules into Java's temp directory on start; two runs
# started together would read each other's half-written copies, so each gets its own.
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$work${JAVA_TOOL_OPTIONS:+ $JAVA_TOOL_OPTIONS}"
status=0
if [ -n "${TLA2TOOLS_JAR:-}" ]; then
  java -XX:+UseParallelGC -cp "$TLA2TOOLS_JAR" tlc2.TLC "${tlc_args[@]}" || status=$?
elif command -v tlc >/dev/null 2>&1; then
  tlc "${tlc_args[@]}" || status=$?
elif command -v nix >/dev/null 2>&1; then
  # Stock nix has neither feature on; the flag is harmless where they are.
  nix --extra-experimental-features 'nix-command flakes' shell "github:NixOS/nixpkgs/${nixpkgs_rev}#tlaplus" --command tlc "${tlc_args[@]}" || status=$?
else
  echo "check.sh: set TLA2TOOLS_JAR to a TLC 2.19 tla2tools.jar, or put tlc or nix on PATH" >&2
  exit 2
fi
exit "$status"
