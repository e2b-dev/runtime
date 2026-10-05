# Changelog

## 0.4.1 (2026-09-11)


### Bug Fixes

* **clickhouse-migrator:** bump goose to v3.28.0 and upgrade Alpine packages (dadae1b)
* **deps:** move the workspace to the Go 1.26.8 toolchain (e7116f5)
* **deps:** update module github.com/stretchr/testify to v1.12.1 (a58bcb5)
* **deps:** update module google.golang.org/api to v0.297.0 (852f328)
* **deps:** update module google.golang.org/grpc to v1.83.1 [security] (27006d5)
* **deps:** update module google.golang.org/grpc to v1.83.2 [security] (7123140)
* **deps:** update opentelemetry (34ecda9)

## 0.4.0 (2026-08-21)


### Features

* **clickhouse:** record the outbound destinations sandboxes connect to (24ef5c5)

## [0.3.0](https://github.com/e2b-dev/infra/compare/clickhouse-migrator-v0.2.0...clickhouse-migrator-v0.3.0) (2026-08-17)


### Features

* **azure:** add azure provider arms to byoc-path build tooling ([d910cfa](https://github.com/e2b-dev/infra/commit/d910cfa3548968e36de0b4ea06e845d0028a891f))
* **azure:** add azure provider arms to byoc-path build tooling ([#1361](https://github.com/e2b-dev/infra/issues/1361)) ([ec32c42](https://github.com/e2b-dev/infra/commit/ec32c421386920ac218c90e69dec3720294420e1))
* **fcversion:** parse vX.Y-&lt;e2b-semver&gt; tags alongside {tag}_{sha} ([#1679](https://github.com/e2b-dev/infra/issues/1679)) ([a8cb6fb](https://github.com/e2b-dev/infra/commit/a8cb6fb8ce04d60fadfe566ef23bdc59b82c089d))
* **storage:** Azure Blob storage provider behind an azblob:// URL ([#1585](https://github.com/e2b-dev/infra/issues/1585)) ([c0f65b6](https://github.com/e2b-dev/infra/commit/c0f65b6730e4b2131f55bc33b1e62c0cb9d97e36))


### Bug Fixes

* **deps:** update module google.golang.org/grpc to v1.83.0 ([#1583](https://github.com/e2b-dev/infra/issues/1583)) ([acbefda](https://github.com/e2b-dev/infra/commit/acbefdae0e45d5c816cc8659dc8d34784ffa0056))
* **deps:** update opentelemetry ([#1700](https://github.com/e2b-dev/infra/issues/1700)) ([ea3100a](https://github.com/e2b-dev/infra/commit/ea3100a890ad7e2ba890b0112c41576589eb0e9d))
* **deps:** update opentelemetry-go-contrib monorepo ([#1607](https://github.com/e2b-dev/infra/issues/1607)) ([5852c94](https://github.com/e2b-dev/infra/commit/5852c942f9b5cff8a66ad008196d7025273febea))

## [0.1.0](https://github.com/e2b-dev/infra/compare/clickhouse-migrator-v0.0.1...clickhouse-migrator-v0.1.0) (2026-07-31)


### Features

* dynamic sandbox log routing and ClickHouse-backed log reads ([#3236](https://github.com/e2b-dev/infra/issues/3236)) ([1b19a3b](https://github.com/e2b-dev/infra/commit/1b19a3bcb37d1fb44171ae8dee3a126cf3d39c34))

## [0.1.0](https://github.com/e2b-dev/infra/compare/clickhouse-migrator-v0.0.1...clickhouse-migrator-v0.1.0) (2026-07-30)


### Features

* dynamic sandbox log routing and ClickHouse-backed log reads ([#3236](https://github.com/e2b-dev/infra/issues/3236)) ([1b19a3b](https://github.com/e2b-dev/infra/commit/1b19a3bcb37d1fb44171ae8dee3a126cf3d39c34))

## 0.0.1 (2026-07-10)


### Features

* Adding client-proxy and clickhouse to e2b-artifacts ([#3210](https://github.com/e2b-dev/infra/issues/3210)) ([5686d88](https://github.com/e2b-dev/infra/commit/5686d881e4c5c8a1712a5bd09a74b198172701b3))
* **api:** LD-gated ClickHouse read switcher ([#3061](https://github.com/e2b-dev/infra/issues/3061)) ([29e74ca](https://github.com/e2b-dev/infra/commit/29e74ca75aba785dedc5252957c750fa293fb036))
* **clickhouse:** implement multi-cluster fan-out for events and stats ([#2925](https://github.com/e2b-dev/infra/issues/2925)) ([39594c6](https://github.com/e2b-dev/infra/commit/39594c6eacba37a124ed5f2c8a8af95319c87ead))
* **migrations:** add webhook deliveries table to ClickHouse ([#2741](https://github.com/e2b-dev/infra/issues/2741)) ([f55a5bd](https://github.com/e2b-dev/infra/commit/f55a5bdd45804ad8866e9b8733ce73bb058eb820))
* **orchestrator:** LD-gated ClickHouse write fan-out feature flag ([#3152](https://github.com/e2b-dev/infra/issues/3152)) ([f046fcf](https://github.com/e2b-dev/infra/commit/f046fcf626a7e91f99c204507a8bf2ceed39e3e6))
* per-team events TTL limit (tier + addons) ([#3181](https://github.com/e2b-dev/infra/issues/3181)) ([f76b2cb](https://github.com/e2b-dev/infra/commit/f76b2cb622efde2e4958caa8358dfc95ba0b4ce7))


### Bug Fixes

* correct 3 CVES ([#3218](https://github.com/e2b-dev/infra/issues/3218)) ([076823b](https://github.com/e2b-dev/infra/commit/076823bc5cbffb9f8c04670c886562445e50ead7))
* push clickhouse-migrator image to both latest and commit SHA tags ([#2954](https://github.com/e2b-dev/infra/issues/2954)) ([3b780d5](https://github.com/e2b-dev/infra/commit/3b780d5d7620d022b1d8c5e4f79d0bc4b3c5ee81))
* reset artifacts ([#3259](https://github.com/e2b-dev/infra/issues/3259)) ([93f7eb5](https://github.com/e2b-dev/infra/commit/93f7eb57ce66fb72607bc0f3c1c40358a3c46c8a))
