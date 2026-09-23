#!/usr/bin/env bash
set -euo pipefail
deploy_dir=$(cd "$(dirname "$0")/.." && pwd -P)
repo_dir=$(cd "$deploy_dir/../.." && pwd -P)
fixture="$deploy_dir/tests/guard-valid.json"
export PATH="$deploy_dir/tests/mock-bin:$PATH"
export CATHEDRAL_NFT_TEST_FIXTURE="$fixture"
export CATHEDRAL_NFT_TEST_FILTER=.
"$deploy_dir/ingress-guard.sh" check
for filter in \
  '.nftables[1].chain.hook="output"' \
  '.nftables[1].chain.prio=0' \
  '.nftables[2].rule.expr[1].match.right.set -= [5008]' \
  '.nftables += [{"rule":{"family":"inet","table":"cathedral_proof","chain":"input","expr":[{"accept":null}]}}]'; do
  export CATHEDRAL_NFT_TEST_FILTER="$filter"
  if "$deploy_dir/ingress-guard.sh" check >/dev/null 2>&1; then
    echo "ingress guard accepted bad nft fixture: $filter" >&2
    exit 1
  fi
done
command -v docker >/dev/null || { echo 'Docker Compose is required for config check' >&2; exit 1; }
CATHEDRAL_API_IMAGE=cathedral-test-api:fixed \
CATHEDRAL_DB_MIGRATOR_IMAGE=cathedral-test-migrator:fixed \
CATHEDRAL_ARTIFACT_DIR=/tmp/cathedral-artifacts \
CATHEDRAL_DEPLOY_DIR="$deploy_dir" \
  docker compose -p cathedral-proof --env-file "$repo_dir/embed/compose/.env" \
    -f "$repo_dir/embed/compose/compose.yaml" -f "$deploy_dir/compose.proof.yaml" \
    config --format json | jq -e '
      .services.api.image == "cathedral-test-api:fixed" and
      .services["db-migrator"].image == "cathedral-test-migrator:fixed" and
      .services["cathedral-ready"].depends_on["base-template"].condition == "service_completed_successfully" and
      .services["cathedral-ready"].depends_on["client-proxy"].condition == "service_healthy" and
      .services["cathedral-ready"].restart == "no" and
      .services["cathedral-ready"].volumes == null and
      .services.ready == null and .services.dashboard == null
    ' >/dev/null
echo 'Cathedral proof config checks passed'
