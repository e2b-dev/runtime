# Changelog

## 0.30.0 (2026-11-06)


### Features

* **clean-nfs-cache:** run budget and multi-sample runs (9fd10b7)
* **embed:** run the dashboard on the api alone, without dashboard-api (9d9370a)
* **featureflags:** make the Firecracker v1.17 line the default for new template builds (09f5c85)


### Bug Fixes

* **deps:** move the workspace to the Go 1.27.3 toolchain (839f8d7)
* **embed:** start ClickHouse without the image's temporary init server (7a44804)
* **orchestrator:** the volume mount test passes the envd version its request takes (5a1c076)


### Code Refactoring

* **dashboard-api:** remove the cluster routes now served by the API (d1ab3fa)
