#!/usr/bin/env bash
set -euo pipefail

# Keep the upstream verified envd/kernel/Firecracker/BusyBox installation and
# then replace the stock orchestrator with the locally built, pinned binary.
/bin/bash /opt/e2b/scripts/fetch-artifacts.sh
source=/opt/cathedral/artifacts/orchestrator
want_file=/opt/cathedral/artifacts/orchestrator.sha256
test -s "$source" && test -s "$want_file" || {
  echo 'Cathedral orchestrator artifact or checksum is missing' >&2
  exit 1
}
want=$(cut -d ' ' -f 1 < "$want_file")
[[ "$want" =~ ^[0-9a-f]{64}$ ]] || exit 1
got=$(sha256sum "$source" | cut -d ' ' -f 1)
test "$got" = "$want" || { echo 'Cathedral orchestrator checksum mismatch' >&2; exit 1; }
destination="${HOST_ROOT:-/host}/var/lib/e2b/bin/orchestrator"
install -m 0755 "$source" "$destination.tmp"
mv -f "$destination.tmp" "$destination"
test "$(sha256sum "$destination" | cut -d ' ' -f 1)" = "$want"
echo 'Cathedral pinned orchestrator installed'
