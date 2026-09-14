# Releasing packages to e2b-artifacts

This repository is a read-only mirror: its source of truth is E2B's internal
monorepo, which exports the code here via copybara. Releases are cut there
too — the package list, SemVer release PRs, auto-deploy tags, and the
publish workflow all live in the monorepo. Each directory in the table
below is what gets tagged and published. No release tags or GitHub Releases
appear in this repository.

| Component | Package directory | Published artifact |
|---|---|---|
| api | `packages/api` | images `us-docker.pkg.dev/e2b-artifacts/api/api` and `…/api/db-migrator` (released as one unit) |
| client-proxy | `packages/client-proxy` | image `us-docker.pkg.dev/e2b-artifacts/client-proxy/client-proxy` |
| clickhouse-migrator | `packages/clickhouse` | image `us-docker.pkg.dev/e2b-artifacts/clickhouse-migrator/clickhouse-migrator` |
| dashboard-api | `packages/dashboard-api` | image `us-docker.pkg.dev/e2b-artifacts/dashboard-api/dashboard-api` |
| embed | `embed` | images `us-docker.pkg.dev/e2b-artifacts/embed/tools`, `…/embed/node-e2b` and `…/embed/seed` (released as one unit; the release moves every platform pin in `embed/compose/.env` and `embed/kubernetes/kustomization.yaml` — api, db-migrator, client-proxy, clickhouse-migrator, orchestrator and these three — to its own version) |
| envd | `packages/envd` | binary `https://storage.googleapis.com/e2b-artifact-binaries/envd/v<version>/envd` |
| nomad-nodepool-apm | `packages/nomad-nodepool-apm` | binaries `nomad-nodepool-apm`, `nomad-deployment-aware-target` under `…/nomad-nodepool-apm/v<version>/` |
| orchestrator | `packages/orchestrator` | binaries `orchestrator`, `clean-nfs-cache` under `…/orchestrator/v<version>/` |

## How a release happens

Two identities:

1. Every commit on the monorepo's default branch that touches a package
   directory is tagged
   `<component>-vMAJOR.MINOR.YYYYMMDDHHMM-<sha>` for that package only, on
   that package's current version line. That publish does not increment
   the patch. The timestamp sits in the patch slot, so these tags sort
   above every `MAJOR.MINOR.x` release — pin exact tags, not "highest".
2. Conventional commits (`feat:`, `fix:`) accumulate into a release PR.
   api, client-proxy, clickhouse-migrator, dashboard-api, embed,
   nomad-nodepool-apm and orchestrator share one coordinated SemVer with
   the rest of the platform: merging that PR tags each of them
   `<component>-vX.Y.Z` at the same number, with changelog. envd is
   versioned on its own and has its own release PR. (The same commits
   arrive here through the copybara export.)
3. Either tag is a git tag only — no GitHub Release. The tag push
   publishes to `e2b-artifacts` — images as `:v<version>`, binaries as
   versioned objects in the public `e2b-artifact-binaries` bucket at
   `<component>/v<version>/`, each with a `<name>.sha256` beside it
   (sha256sum format). Client-bucket copies use the same string:
   `<name>.v<version>`, never a bare commit SHA. The `v` is on every tag,
   image tag, bucket path and object suffix; pin with it.

A manual tag (below) still publishes that commit.

## Release candidates / manual publishes

Push a `<component>-v<version>` tag by hand in the monorepo (e.g.
`client-proxy-v2.0.0-rc1`) and the publish workflow publishes that commit as
`:v2.0.0-rc1`. The registry has immutable tags and the binaries bucket is
create-only, so a manual tag can never overwrite an existing version — a bad
published version is fixed by cutting the next version, never by
re-publishing.

## Recovery

All recovery happens in the monorepo:

- **Publish failed**: re-run that tag's publish run (any time), or dispatch
  the publish workflow manually with the tag name.
- **Release merged but never tagged** (e.g. the merge wasn't the head commit
  of its push): push the `<component>-v<version>` tag on the merge commit by
  hand, and swap the release PR's label from `autorelease: pending` to
  `autorelease: tagged` — otherwise release-please refuses to open new
  release PRs for any package.
