#!/usr/bin/env bash
set -euo pipefail

usage() { echo "usage: $0 PINNED_SOURCE_DIR ARTIFACT_DIR" >&2; exit 2; }
test "$#" -eq 2 || usage
source_dir=$(cd "$1" && pwd -P)
artifact_dir=$2
case "$artifact_dir" in /*) ;; *) echo 'ARTIFACT_DIR must be absolute' >&2; exit 2 ;; esac
revision=f11618e72af6996d4df3290078a6d54fd3099649
test "$(git -C "$source_dir" rev-parse HEAD)" = "$revision" || {
  echo "source must be checked out at $revision" >&2; exit 1;
}
test -z "$(git -C "$source_dir" status --porcelain)" || {
  echo 'pinned source worktree must be clean' >&2; exit 1;
}
command -v docker >/dev/null || { echo 'Docker is required' >&2; exit 1; }
command -v sha256sum >/dev/null || { echo 'sha256sum is required' >&2; exit 1; }
mkdir -p "$artifact_dir"
artifact_dir=$(cd "$artifact_dir" && pwd -P)
test ! -e "$artifact_dir/.runtime-lock.env" || {
  echo 'artifact lock already exists; use a new artifact directory' >&2; exit 1;
}
api_tag="cathedral-proof-api:$revision"
migrator_tag="cathedral-proof-db-migrator:$revision"
docker buildx build --platform linux/amd64 --load \
  --build-arg "COMMIT_SHA=$revision" -t "$api_tag" \
  -f "$source_dir/packages/api/Dockerfile" "$source_dir/packages"
docker buildx build --platform linux/amd64 --load \
  -t "$migrator_tag" -f "$source_dir/packages/db/Dockerfile" "$source_dir/packages"
docker buildx build --platform linux/amd64 --target artifacts \
  --output "type=local,dest=$artifact_dir" \
  --build-arg "COMMIT_SHA=$revision" \
  -f "$source_dir/packages/orchestrator/Dockerfile" "$source_dir/packages"
test -s "$artifact_dir/orchestrator"
sha256sum "$artifact_dir/orchestrator" > "$artifact_dir/orchestrator.sha256"
api_id=$(docker image inspect --format '{{.Id}}' "$api_tag")
migrator_id=$(docker image inspect --format '{{.Id}}' "$migrator_tag")
case "$api_id:$migrator_id" in sha256:*:sha256:*) ;; *) echo 'image IDs are unavailable' >&2; exit 1 ;; esac
deploy_dir=$(cd "$(dirname "$0")" && pwd -P)
umask 077
lock="$artifact_dir/.runtime-lock.env"
printf 'CATHEDRAL_RUNTIME_GIT_REVISION=%s\nCATHEDRAL_ARTIFACT_DIR=%s\nCATHEDRAL_DEPLOY_DIR=%s\nCATHEDRAL_API_IMAGE=%s\nCATHEDRAL_API_IMAGE_ID=%s\nCATHEDRAL_DB_MIGRATOR_IMAGE=%s\nCATHEDRAL_DB_MIGRATOR_IMAGE_ID=%s\n' \
  "$revision" "$artifact_dir" "$deploy_dir" "$api_tag" "$api_id" "$migrator_tag" "$migrator_id" > "$lock"
chmod 600 "$lock"
echo "Cathedral local artifacts and lock: $lock"
