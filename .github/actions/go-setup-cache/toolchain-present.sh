#!/usr/bin/env bash
# Prints version=<go toolchain version from go.work> and present=<true|false>:
# whether that exact version is already in the runner tool cache where
# actions/setup-go looks for it (<tool cache>/go/<version>/<arch> plus the
# <arch>.complete marker actions/tool-cache writes), so the caller can skip
# restoring it from the actions cache.
set -euo pipefail

go_work=$1
tool_cache=$2
runner_arch=$3

version=$(awk '$1 == "go" { v = $2 } $1 == "toolchain" { v = substr($2, 3) } END { print v }' "$go_work")
echo "version=$version"

# setup-go keys the tool cache by Node's os.arch(): x64 or arm64.
case "$runner_arch" in
  X64) arch=x64 ;;
  ARM64) arch=arm64 ;;
  *) arch= ;;
esac

tool="$tool_cache/go/$version/$arch"
present=false
if [ -n "$arch" ] && [ -x "$tool/bin/go" ] && [ -f "$tool.complete" ]; then
  present=true
fi
echo "present=$present"
