# Changelog

## 0.14.1 (2026-09-09)


### Bug Fixes

* **api:** rebuild template aliases on the current cluster (c74f369)

## 0.14.0 (2026-09-09)


### Features

* **api:** expose outstanding node work (045df33)


### Code Refactoring

* **api:** remove ignored free disk request field (53b8e04)

## 0.13.0 (2026-09-09)


### Features

* **api:** add minimum free disk request name (b03cd83)

## 0.12.0 (2026-09-07)


### Features

* **orchestrator:** add shutting-down status (c7bb7eb)
* **templates:** configure free disk targets (b88aab9)


### Bug Fixes

* **deps:** update module google.golang.org/grpc to v1.83.1 [security] (27006d5)

## 0.11.0 (2026-09-04)


### Features

* **api:** optional LOKI_URL and a LOGS_READ_CONFIG override for deployments without LaunchDarkly (6a6edf5)
* **api:** restore a refused pause to running and bound auto-pause overstay (flag-gated) (cc07fd6)


### Bug Fixes

* **api:** stagger Redis lock-release retries (5f3e739)


### Miscellaneous Chores

* **api:** drop x-not-implemented from the filesystem-only resume field (103e11f)

## 0.10.0 (2026-09-03)


### Features

* **api:** report egress proxy usage in product analytics (1049c80)


### Bug Fixes

* **api:** chunk Reconcile MGETs to 256 keys per command (bc61e07)
* **api:** stop retrying pause on already-killing leftovers (df365d4)


### Performance Improvements

* **api:** bound sandbox-store MGETs to 256 keys per command (a49f2d1)

## 0.9.0 (2026-09-02)


### Features

* **api:** add service JWT authentication (3374207)
* **api:** make network rule domain limit configurable (291ae2c)
* **orchestrator:** snapshot-admission pre-flight for pause and checkpoint (flag-gated) (b47217c)

## 0.8.0 (2026-09-01)


### Features

* **api:** accept HTTPS backend ports on sandbox create (c395e9b)
* **api:** filter sandbox placement by orchestrator release (7e433ac)

## 0.7.0 (2026-08-28)


### Features

* **servicediscovery:** union orchestrator discovery across both schedulers (df09b1d)

## 0.6.2 (2026-08-27)


### Bug Fixes

* **api:** drop the evictor's fs-only version degrade (d765459)


### Miscellaneous Chores

* **api:** fix comment & linter (d0903da)

## 0.6.1 (2026-08-27)


### Bug Fixes

* **api:** don't check for fs-only pause support (15f5a24)

## 0.6.0 (2026-08-27)


### Features

* **network:** accept wildcard transform domains (838ed57)


### Bug Fixes

* **api:** include template and build IDs in create errors (929b6b7)

## 0.5.1 (2026-08-27)


### Code Refactoring

* **api:** retire the api's second discovery mechanism (1360629)
* name node identity for what it identifies (62af6f7)
* **servicediscovery:** one package per backend (ae36dd9)

## 0.5.0 (2026-08-26)


### Features

* **api:** admin rig passthrough for remote clusters (6586f22)
* **snapshots:** gate in-place checkpoint and fs-only snapshots on FC release &gt;= 0.2.0 (c7b5dff)


### Bug Fixes

* **api:** deregister node when its grpc connection is shut down (44d9dad)
* **api:** fail the snapshot build when a pause is rejected (036a174)
* **api:** finish snapshot builds on client disconnect (bd31fc4)


### Code Refactoring

* **api:** reuse the filesystem-only snapshot predicate (7829b43)
* rename the discovery package for what it discovers (2c8ad0d)

## 0.4.0 (2026-08-25)


### Features

* **api:** confine filesystem-only resume to one CPU model (e6a0985)


### Bug Fixes

* **api:** record terminal template build status on a live context (fe829d4)


### Dependencies

* bump go-redis to v9.21.0 across the OSS modules (1aa38f8)


### Code Refactoring

* **api:** extract service-discovery construction from the store (2924266)
* **shared:** unify service discovery into one nodediscovery package (f214e11)

## 0.3.0 (2026-08-24)


### Features

* **api:** remove deprecated E2B access token auth (a301352)
* **shared:** use TypeID encoding for object IDs (4a5075b)

## 0.2.0 (2026-08-20)


### Features

* **api:** add sandbox list sorting and filters (f8a9bec)
* **api:** expose secrets management (b4ce6c5)
* **api:** semantic error codes and differentiated statuses for sandbox placement failures (0a4dfb1)
* **auth:** share the security-requirements error selection (2fe094c)
* **checkpoint:** in-place checkpoint — snapshot a sandbox without stopping it (0001909)
* **db:** add PostgreSQL session primitives (22f0ccb)
* filesystem-only resume of memory-inclusive snapshots (6061743)
* **orchestrator-ee:** resolve customer secrets for egress headers (36c966f)
* **redis:** password auth and explicit TLS enablement (1c49ecf)

## [0.1.0](https://github.com/e2b-dev/infra/compare/api-v0.0.1...api-v0.1.0) (2026-08-17)


### Features

* **api:** add secret management RPC client ([#1725](https://github.com/e2b-dev/infra/issues/1725)) ([62223a9](https://github.com/e2b-dev/infra/commit/62223a9bd59abace02fcdbb90d74d81f9039073d))
* **api:** let a removal pin the sandbox incarnation it meant to remove ([#1609](https://github.com/e2b-dev/infra/issues/1609)) ([23f6eb1](https://github.com/e2b-dev/infra/commit/23f6eb172ceda25808e1a4b70be0d3917746b09b))
* **api:** track whether a node is reachable, apart from its status ([#1617](https://github.com/e2b-dev/infra/issues/1617)) ([fecfd7b](https://github.com/e2b-dev/infra/commit/fecfd7b9b92ddf421745c7fa8dd88ae1de68fd87))
* **storage:** Azure Blob storage provider behind an azblob:// URL ([#1585](https://github.com/e2b-dev/infra/issues/1585)) ([c0f65b6](https://github.com/e2b-dev/infra/commit/c0f65b6730e4b2131f55bc33b1e62c0cb9d97e36))


### Bug Fixes

* **api:** forward detailed placement-failure reasons to clients ([#1752](https://github.com/e2b-dev/infra/issues/1752)) ([71c9d69](https://github.com/e2b-dev/infra/commit/71c9d691b994afc736f7bf60bcb30ba022cd71e5))
* **api:** send team_id as a flat property on PostHog events ([#1758](https://github.com/e2b-dev/infra/issues/1758)) ([f517312](https://github.com/e2b-dev/infra/commit/f5173120113e3c6b64990cc0eb0d9b9562751928))
* **deps:** update module cloud.google.com/go/storage to v1.64.0 ([#1793](https://github.com/e2b-dev/infra/issues/1793)) ([e66f1a8](https://github.com/e2b-dev/infra/commit/e66f1a8610c8119df23292620d16ed84e98e700e))
* **deps:** update module github.com/getkin/kin-openapi to v0.146.0 ([#1582](https://github.com/e2b-dev/infra/issues/1582)) ([5307ba8](https://github.com/e2b-dev/infra/commit/5307ba8613599106061c0b55f71be75718c2ad3c))
* **deps:** update module google.golang.org/api to v0.292.0 ([#1699](https://github.com/e2b-dev/infra/issues/1699)) ([a10138c](https://github.com/e2b-dev/infra/commit/a10138ce9b7db87b614444ac85c581ae2d7839c8))
* **deps:** update module google.golang.org/grpc to v1.83.0 ([#1583](https://github.com/e2b-dev/infra/issues/1583)) ([acbefda](https://github.com/e2b-dev/infra/commit/acbefdae0e45d5c816cc8659dc8d34784ffa0056))
* **deps:** update opentelemetry ([#1700](https://github.com/e2b-dev/infra/issues/1700)) ([ea3100a](https://github.com/e2b-dev/infra/commit/ea3100a890ad7e2ba890b0112c41576589eb0e9d))
* **deps:** update opentelemetry-go-contrib monorepo ([#1607](https://github.com/e2b-dev/infra/issues/1607)) ([5852c94](https://github.com/e2b-dev/infra/commit/5852c942f9b5cff8a66ad008196d7025273febea))
* **deps:** update testcontainers-go monorepo to v0.43.0 ([#1701](https://github.com/e2b-dev/infra/issues/1701)) ([08b1cbe](https://github.com/e2b-dev/infra/commit/08b1cbe6b6e4e4cddf4b91c068e0b4632ce2e94c))

## 0.0.1 (2026-07-30)


### Features

* add workspace admin API foundations ([#3314](https://github.com/e2b-dev/infra/issues/3314)) ([0f72030](https://github.com/e2b-dev/infra/commit/0f72030b3c54927d68b700b77281f544d567e7d6))
* **api:** add admin team API key routes ([#2825](https://github.com/e2b-dev/infra/issues/2825)) ([4a1e083](https://github.com/e2b-dev/infra/commit/4a1e0837e5454b47aedc9c935717d30a35e17080))
* **api:** add feature flag to stop accepting E2B access tokens ([#3240](https://github.com/e2b-dev/infra/issues/3240)) ([2cf489b](https://github.com/e2b-dev/infra/commit/2cf489bd8b63f6b2848fe6e9da7c4718d8651966))
* **api:** add sandbox fork endpoint ([#3202](https://github.com/e2b-dev/infra/issues/3202)) ([643d726](https://github.com/e2b-dev/infra/commit/643d726f0ff4f3fd8ac0c1592812ffa88e62e2d3))
* **api:** add sandbox IAM workload token configuration ([13ddb3d](https://github.com/e2b-dev/infra/commit/13ddb3dedc992dbb2683b086cf85a5bbbd7f9720))
* **api:** add sandbox workload identity permission ([#3319](https://github.com/e2b-dev/infra/issues/3319)) ([13ddb3d](https://github.com/e2b-dev/infra/commit/13ddb3dedc992dbb2683b086cf85a5bbbd7f9720))
* **api:** add user agent integration attribution to PostHog events ([#3303](https://github.com/e2b-dev/infra/issues/3303)) ([d83be18](https://github.com/e2b-dev/infra/commit/d83be180bb0f187dfdbc5a570f30f23be0c23895))
* **api:** discover orchestrators via nomad service ([#3176](https://github.com/e2b-dev/infra/issues/3176)) ([32af250](https://github.com/e2b-dev/infra/commit/32af250ca3171b2923c292c41fe7be995d297a8d))
* **api:** e2b access token deprecation feature flag rename ([#3110](https://github.com/e2b-dev/infra/issues/3110)) ([ebc2daa](https://github.com/e2b-dev/infra/commit/ebc2daa314af931998a1c1d0b44811467e8ccc49))
* **api:** enforce blocked-team restrictions at mutating API endpoints ([#2659](https://github.com/e2b-dev/infra/issues/2659)) ([db848ab](https://github.com/e2b-dev/infra/commit/db848ab73917ca763307bddde44dbf451dc45470))
* **api:** filter snapshots by name ([#3184](https://github.com/e2b-dev/infra/issues/3184)) ([6fa1bc7](https://github.com/e2b-dev/infra/commit/6fa1bc76b8cb2dc5a8606d2eca4e35cf9e8a133e))
* **api:** gate access token issuance behind feature flag ([#3101](https://github.com/e2b-dev/infra/issues/3101)) ([2f7811e](https://github.com/e2b-dev/infra/commit/2f7811e902ea2765d4fbc0d5255ab106571358ed))
* **api:** LD-gated ClickHouse read switcher ([#3061](https://github.com/e2b-dev/infra/issues/3061)) ([29e74ca](https://github.com/e2b-dev/infra/commit/29e74ca75aba785dedc5252957c750fa293fb036))
* **api:** limit template build name to 128 characters ([#3109](https://github.com/e2b-dev/infra/issues/3109)) ([84aa186](https://github.com/e2b-dev/infra/commit/84aa186bd28616a524b7cece74bb1a7dc9b7e052))
* **api:** paginated GET /v2/templates (EN-603) ([#3059](https://github.com/e2b-dev/infra/issues/3059)) ([91e02e4](https://github.com/e2b-dev/infra/commit/91e02e48aa350af7df1d70d59ce0a433c9fc6839))
* **api:** per-region volume type defaults from node-derived region ([#3435](https://github.com/e2b-dev/infra/issues/3435)) ([baf5559](https://github.com/e2b-dev/infra/commit/baf5559a5b88fc6f2227eeb33d878e9194f70738))
* **api:** pin resume retries to the node a previous resume timed out on ([#3066](https://github.com/e2b-dev/infra/issues/3066)) ([a4fd0f2](https://github.com/e2b-dev/infra/commit/a4fd0f2e5ec419fa44b78e56e6737e7cec74f6d0))
* **api:** SOCKS5 egress proxy on sandbox network config (BYOP) ([#2642](https://github.com/e2b-dev/infra/issues/2642)) ([1fc3820](https://github.com/e2b-dev/infra/commit/1fc3820d7319ee2bef18c1a66bfa8294b8dc053b))
* **api:** soft-delete build layers in DB on user delete ([#3121](https://github.com/e2b-dev/infra/issues/3121)) ([ee88776](https://github.com/e2b-dev/infra/commit/ee887768cf28e657f36aa7038495fad37859c144))
* **auth:** support admin token team auth ([#2934](https://github.com/e2b-dev/infra/issues/2934)) ([5496666](https://github.com/e2b-dev/infra/commit/549666684dedf3ce3418b2c1a58f0487cadb00ff))
* dynamic sandbox log routing and ClickHouse-backed log reads ([#3236](https://github.com/e2b-dev/infra/issues/3236)) ([1b19a3b](https://github.com/e2b-dev/infra/commit/1b19a3bcb37d1fb44171ae8dee3a126cf3d39c34))
* **evictor:** make max concurrent evictions a feature flag ([#2727](https://github.com/e2b-dev/infra/issues/2727)) ([0b33013](https://github.com/e2b-dev/infra/commit/0b330130b91398c5c99fe7857c3e76be3588b880))
* **metrics:** distinguish joined from regular requests (ENG-4072) ([#2699](https://github.com/e2b-dev/infra/issues/2699)) ([390e296](https://github.com/e2b-dev/infra/commit/390e29615df439133f8be7d469068be3af712bc7))
* **observability:** add kill_reason to sandbox.lifecycle.killed ([#2833](https://github.com/e2b-dev/infra/issues/2833)) ([e45418f](https://github.com/e2b-dev/infra/commit/e45418fd59eafa0aba1f34b34eae83723730bb6c))
* **observability:** include kill_reason in kill-path structured logs ([#2846](https://github.com/e2b-dev/infra/issues/2846)) ([33c49f7](https://github.com/e2b-dev/infra/commit/33c49f725fc8e9d0feba16a55987461c09de9680))
* **orchestrator:** add dummy orchestrator binary for local API dev ([#2744](https://github.com/e2b-dev/infra/issues/2744)) ([ab56e25](https://github.com/e2b-dev/infra/commit/ab56e25437d479af1b8c683afca859bad6543987))
* **orchestrator:** report hugepage metrics to API ([#3182](https://github.com/e2b-dev/infra/issues/3182)) ([7735bae](https://github.com/e2b-dev/infra/commit/7735bae35bd3cd3251a929e13fa9adf2333fd1e8))
* **orchestrator:** track and report last status change timestamp ([#2980](https://github.com/e2b-dev/infra/issues/2980)) ([f79be77](https://github.com/e2b-dev/infra/commit/f79be779ecbc1c38add07664ffbbe154bf7755b5))
* **otel:** instrument auth service HTTP client with otelhttp ([#2722](https://github.com/e2b-dev/infra/issues/2722)) ([69b085d](https://github.com/e2b-dev/infra/commit/69b085d2f7e045efa03117e220d3fc970075c3df))
* per-team events TTL limit (tier + addons) ([#3181](https://github.com/e2b-dev/infra/issues/3181)) ([f76b2cb](https://github.com/e2b-dev/infra/commit/f76b2cb622efde2e4958caa8358dfc95ba0b4ce7))
* **storage:** stamp provenance custom metadata on uploaded objects (incl. headers) ([#3033](https://github.com/e2b-dev/infra/issues/3033)) ([ba8604e](https://github.com/e2b-dev/infra/commit/ba8604e0621baefd61aaf1ea34bec27805c126cf))


### Bug Fixes

* added api and orch ([#3454](https://github.com/e2b-dev/infra/issues/3454)) ([fda5e45](https://github.com/e2b-dev/infra/commit/fda5e4579c59729cb8510dd47965c5192813ac8b))
* **api:** check template alias tags in exists endpoint ([#2916](https://github.com/e2b-dev/infra/issues/2916)) ([9574cdf](https://github.com/e2b-dev/infra/commit/9574cdf114f0efb0274d0d9fb3c06c26c864cbb9))
* **api:** copy auth/internal into api and dashboard-api image builds ([#3323](https://github.com/e2b-dev/infra/issues/3323)) ([bda1fee](https://github.com/e2b-dev/infra/commit/bda1fee3a288a26beb91a06a469dc70730386319))
* **api:** discover the local orchestrator as a template builder ([#3386](https://github.com/e2b-dev/infra/issues/3386)) ([9ea005a](https://github.com/e2b-dev/infra/commit/9ea005a609c0101a2eb387d7224ccd248c900516))
* **api:** expose pagination headers via CORS ([#3388](https://github.com/e2b-dev/infra/issues/3388)) ([e832b1e](https://github.com/e2b-dev/infra/commit/e832b1ed40f488dd1159fdece027bad004522be0))
* **api:** handle corrupted data in sandbox stop time ([#3203](https://github.com/e2b-dev/infra/issues/3203)) ([a98a178](https://github.com/e2b-dev/infra/commit/a98a1786c9a5428d8012256faf1b47d9561d49c5))
* **api:** include exhaustion reason in "Node exhausted" placement warning ([#3279](https://github.com/e2b-dev/infra/issues/3279)) ([eb3797b](https://github.com/e2b-dev/infra/commit/eb3797bfacb03574df534756b1a8aae63af2c4cf))
* **api:** invalidate auth cache on API key deletion ([#3324](https://github.com/e2b-dev/infra/issues/3324)) ([8b02910](https://github.com/e2b-dev/infra/commit/8b029108271d21273fab388146b9fe6b9fc547e8))
* **api:** keep API alive until in-flight requests finish ([#2708](https://github.com/e2b-dev/infra/issues/2708)) ([06378c7](https://github.com/e2b-dev/infra/commit/06378c733a799d6fdf0ed4ac0ddd80d983f4e1fc))
* **api:** let the analytics collector address carry a port ([#3394](https://github.com/e2b-dev/infra/issues/3394)) ([6d41cb5](https://github.com/e2b-dev/infra/commit/6d41cb594af2dfe989b3fc952bdf805f8c859c6c))
* **api:** parse the pause body regardless of Content-Length ([#3056](https://github.com/e2b-dev/infra/issues/3056)) ([d66aab8](https://github.com/e2b-dev/infra/commit/d66aab8c92ff4c6478d00ae82ada5048597ea391))
* **api:** prevent uint64 underflow in node allocated metrics ([#3216](https://github.com/e2b-dev/infra/issues/3216)) ([fed38e1](https://github.com/e2b-dev/infra/commit/fed38e11654f60aff527a9e2da661da9d11e4f4f))
* **api:** push api and db-migrator images to both latest and commit S… ([#2951](https://github.com/e2b-dev/infra/issues/2951)) ([6f010fc](https://github.com/e2b-dev/infra/commit/6f010fceb60987859de0d58e4bf33b81729fecbe))
* **api:** reject non-positive timeout on sandbox create, resume, and fork ([#3419](https://github.com/e2b-dev/infra/issues/3419)) ([b672bd1](https://github.com/e2b-dev/infra/commit/b672bd174a0d131b4b14c917e1a866d28ecc8bb9))
* **api:** report invalid tag errors as bad requests ([#2799](https://github.com/e2b-dev/infra/issues/2799)) ([10085a1](https://github.com/e2b-dev/infra/commit/10085a15d83e05485d3871b1a7077931451ad98d))
* **api:** stop evicting the local node during sync ([#2881](https://github.com/e2b-dev/infra/issues/2881)) ([5455905](https://github.com/e2b-dev/infra/commit/545590525f327d92823bc0b4ea9d4e54d382a77e))
* **api:** use correct error variable in processCustomErrors ([#3135](https://github.com/e2b-dev/infra/issues/3135)) ([a131a00](https://github.com/e2b-dev/infra/commit/a131a00507f58e7f3bda3f2196162ab4435ed40f))
* **auth:** rename X-Team-Id header to X-Team-ID ([#2723](https://github.com/e2b-dev/infra/issues/2723)) ([f92ecc0](https://github.com/e2b-dev/infra/commit/f92ecc04c9a77b3b72df5bb6dec8173623651367))
* correct 3 CVES ([#3218](https://github.com/e2b-dev/infra/issues/3218)) ([076823b](https://github.com/e2b-dev/infra/commit/076823bc5cbffb9f8c04670c886562445e50ead7))
* **orchestrator:** reject standby while draining ([#3325](https://github.com/e2b-dev/infra/issues/3325)) ([475a7ee](https://github.com/e2b-dev/infra/commit/475a7eee7c94f35a834b1978b189b75890c2bbe7))
* Support snapshots for non-default clusters ([#2947](https://github.com/e2b-dev/infra/issues/2947)) ([28eeb72](https://github.com/e2b-dev/infra/commit/28eeb72242fdd685c8a474744e6c39f1c36c34f9))


### Performance Improvements

* **api:** wake reservation waiters via pub/sub instead of 20ms polling [ENG-4070] ([#2729](https://github.com/e2b-dev/infra/issues/2729)) ([2944d06](https://github.com/e2b-dev/infra/commit/2944d061447fe1d1db51a56319ff6c52bdd8b856))
