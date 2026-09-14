# Changelog

## 0.3.0 (2026-09-11)


### Features

* **local-dev:** support isolated worker execution (baa2a23)
* **orchestrator:** publish sandbox routing record behind feature flags (3bc98a0)


### Bug Fixes

* **deps:** move the workspace to the Go 1.26.8 toolchain (e7116f5)
* **deps:** update module github.com/stretchr/testify to v1.12.1 (a58bcb5)
* **deps:** update module github.com/txn2/txeh to v1.8.1 (36a09d6)
* **deps:** update module golang.org/x/oauth2 to v0.37.0 (92ca7bd)
* **deps:** update module google.golang.org/api to v0.297.0 (852f328)
* **deps:** update module google.golang.org/grpc to v1.83.1 [security] (27006d5)
* **deps:** update module google.golang.org/grpc to v1.83.2 [security] (7123140)
* **deps:** update opentelemetry (34ecda9)
* **deps:** update testcontainers-go monorepo to v0.44.0 (5473119)
* upgrade Alpine packages in every alpine runtime stage (5a61a32)

## 0.2.1 (2026-08-25)


### Dependencies

* bump go-redis to v9.21.0 across the OSS modules (1aa38f8)

## 0.2.0 (2026-08-24)


### Features

* **redis:** password auth and explicit TLS enablement (1c49ecf)
* **shared:** use TypeID encoding for object IDs (4a5075b)

## [0.1.0](https://github.com/e2b-dev/infra/compare/client-proxy-v0.0.1...client-proxy-v0.1.0) (2026-08-17)


### Features

* **azure:** add azure provider arms to byoc build tooling ([d910cfa](https://github.com/e2b-dev/infra/commit/d910cfa3548968e36de0b4ea06e845d0028a891f))
* **azure:** add azure provider arms to byoc build tooling ([#1361](https://github.com/e2b-dev/infra/issues/1361)) ([ec32c42](https://github.com/e2b-dev/infra/commit/ec32c421386920ac218c90e69dec3720294420e1))
* **fcversion:** parse vX.Y-&lt;e2b-semver&gt; tags alongside {tag}_{sha} ([#1679](https://github.com/e2b-dev/infra/issues/1679)) ([a8cb6fb](https://github.com/e2b-dev/infra/commit/a8cb6fb8ce04d60fadfe566ef23bdc59b82c089d))


### Bug Fixes

* **deps:** update module google.golang.org/grpc to v1.83.0 ([#1583](https://github.com/e2b-dev/infra/issues/1583)) ([acbefda](https://github.com/e2b-dev/infra/commit/acbefdae0e45d5c816cc8659dc8d34784ffa0056))
* **deps:** update opentelemetry ([#1700](https://github.com/e2b-dev/infra/issues/1700)) ([ea3100a](https://github.com/e2b-dev/infra/commit/ea3100a890ad7e2ba890b0112c41576589eb0e9d))
* **deps:** update opentelemetry-go-contrib monorepo ([#1607](https://github.com/e2b-dev/infra/issues/1607)) ([5852c94](https://github.com/e2b-dev/infra/commit/5852c942f9b5cff8a66ad008196d7025273febea))
* **deps:** update testcontainers-go monorepo to v0.43.0 ([#1701](https://github.com/e2b-dev/infra/issues/1701)) ([08b1cbe](https://github.com/e2b-dev/infra/commit/08b1cbe6b6e4e4cddf4b91c068e0b4632ce2e94c))

## 0.0.1 (2026-07-11)


### Features

* Adding client-proxy and clickhouse to e2b-artifacts ([#3210](https://github.com/e2b-dev/infra/issues/3210)) ([5686d88](https://github.com/e2b-dev/infra/commit/5686d881e4c5c8a1712a5bd09a74b198172701b3))


### Bug Fixes

* added changelog file to trigger client-proxy buld ([#3268](https://github.com/e2b-dev/infra/issues/3268)) ([70b0ee9](https://github.com/e2b-dev/infra/commit/70b0ee9c05022003564e0bf3fd081ac836527d73))
* correct 3 CVES ([#3218](https://github.com/e2b-dev/infra/issues/3218)) ([076823b](https://github.com/e2b-dev/infra/commit/076823bc5cbffb9f8c04670c886562445e50ead7))
* **local-dev:** rename API_GRPC_ADDRESS to API_INTERNAL_GRPC_ADDRESS in local dev env ([#2589](https://github.com/e2b-dev/infra/issues/2589)) ([6c0bcb1](https://github.com/e2b-dev/infra/commit/6c0bcb15d80c2f33e23102c18f22cc6ea49cfd8e))
* push client-proxy, dashboard-api, and docker-reverse-proxy image… ([#2953](https://github.com/e2b-dev/infra/issues/2953)) ([1d930ee](https://github.com/e2b-dev/infra/commit/1d930ee60fd74b3ad1d5c167165b1005baa471fe))
* reset artifacts ([#3259](https://github.com/e2b-dev/infra/issues/3259)) ([93f7eb5](https://github.com/e2b-dev/infra/commit/93f7eb57ce66fb72607bc0f3c1c40358a3c46c8a))
