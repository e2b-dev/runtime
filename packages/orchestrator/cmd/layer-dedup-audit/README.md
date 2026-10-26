# layer-dedup-audit

READ-ONLY (mark-only) audit of historical cross-scope duplicate template
layers in the **build-cache** bucket.

Layer caching is content-addressed (`cache.HashKeys` = sha256 of layer
content, no team), but the storage path is prefixed by `cacheScope`, which the
API historically set to `teamID`. So two teams that build byte-identical
layers each hold a private copy at `{teamID}/files/{hash}.tar` (+ the tiny
`{teamID}/index/{hash}`). This tool lists the bucket, groups `files/{hash}.tar`
by hash across every scope, and reports the duplicates, wasted bytes, and a
candidate manifest.

**It deletes nothing.** Physical deletion is a separate, operator-gated step
that consumes `out/candidates.json` after review.

## Relation to template-gc (#3742)

Orthogonal, complementary, both mark-only, each on a different bucket:

| | template-gc (#3742) | layer-dedup-audit |
|---|---|---|
| bucket | TEMPLATE_BUCKET `{buildID}/` | BUILD_CACHE_BUCKET `{scope}/files/{hash}.tar` |
| criterion | liveness (orphan builds outside the live-root ancestor closure) | content (same hash under multiple scopes) |
| reclaims | unreferenced build artifacts (memfile/rootfs) | cross-scope duplicate layer tars |

template-gc cannot dedup cross-team duplicates (each is still referenced by its
own live template, so both are kept); this tool cannot reclaim orphan build
artifacts in the template bucket. They compose — see the PR description.

## Usage (read-only)

```bash
export S3_ENDPOINT=https://tos-s3-cn-beijing.volces.com
export S3_REGION=cn-beijing
export S3_BUCKET=<build-cache bucket>
export S3_ACCESS_KEY_ID=...  S3_SECRET_ACCESS_KEY=...
export S3_USE_PATH_STYLE=true   # TOS/MinIO
go run ./cmd/layer-dedup-audit   # writes out/duplicates.json, out/candidates.json; deletes 0 objects
```

## Follow-up

Once #3742 merges, this can be refactored onto its `storage.ObjectLister`
(which also adds a filesystem backend) instead of the direct S3 client here.
