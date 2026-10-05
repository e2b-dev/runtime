# Changelog

## 0.9.0 (2026-09-10)


### Features

* **orch:** report and record the memory protection on envd's cgroup chain per start (8a1809e)


### Bug Fixes

* **deps:** update module google.golang.org/grpc to v1.83.2 [security] (7123140)

## 0.8.0 (2026-09-07)


### Features

* **envd:** name versioned uploads and promotes by release version (0887574)


### Bug Fixes

* **deps:** update module google.golang.org/grpc to v1.83.1 [security] (27006d5)
* **envd:** flush logs before pause snapshots (f924188)
* **envd:** report process-start resource exhaustion as resource_exhausted (4e4ca69)
* **envd:** stop counting a vanished cgroup as a freeze or thaw failure (9c5f26e)
* **orch:** re-send a sandbox's default user so a live envd upgrade cannot lose it (366ea8f)

## 0.7.0 (2026-08-24)


### Features

* **envd:** freeze the customer's cgroups before a pause, not only our own (e117443)
* **envd:** wait for the pre-pause freeze to stop the workload (99ca5cc)
* **orch:** forbid envd's private endpoints in the sandbox proxy (b6de7ae)
* **shared:** use TypeID encoding for object IDs (4a5075b)


### Bug Fixes

* **deps:** update module cloud.google.com/go/storage to v1.64.0 (e66f1a8)
* **deps:** update module github.com/getkin/kin-openapi to v0.146.0 (5307ba8)
* **deps:** update module google.golang.org/grpc to v1.83.0 (acbefda)
* **deps:** update opentelemetry-go-contrib monorepo (5852c94)
* **deps:** update testcontainers-go monorepo to v0.43.0 (08b1cbe)

## 0.0.1 (2026-07-29)


### Features

* **envd:** add --no-cgroups flag to disable cgroup management ([#2811](https://github.com/e2b-dev/infra/issues/2811)) ([e10814c](https://github.com/e2b-dev/infra/commit/e10814cecc073614a26d6fc346f4aa889c3b4208))
* **envd:** add optional EntryInfo to watch FilesystemEvent ([#2930](https://github.com/e2b-dev/infra/issues/2930)) ([bbbc7c8](https://github.com/e2b-dev/infra/commit/bbbc7c88a60e5feb147fb4c9769a3e9f4cee828f))
* **envd:** allow opting into watching network mounts ([#2982](https://github.com/e2b-dev/infra/issues/2982)) ([9799dd0](https://github.com/e2b-dev/infra/commit/9799dd02631fb863d225adf75459416af220cf84))
* **envd:** give envd realtime IO priority, reset for user processes ([#2681](https://github.com/e2b-dev/infra/issues/2681)) ([f4bd1b2](https://github.com/e2b-dev/infra/commit/f4bd1b24ba77185a6eafcd6435bb4a3330ee7998))
* **envd:** split collapse stats into real migrations vs already-huge ([#3021](https://github.com/e2b-dev/infra/issues/3021)) ([0d77614](https://github.com/e2b-dev/infra/commit/0d7761465acf9bd78ec1049c67769c7db008e35b))
* **envd:** support user-defined file metadata via xattrs ([#2732](https://github.com/e2b-dev/infra/issues/2732)) ([da8fbe4](https://github.com/e2b-dev/infra/commit/da8fbe49c344028441c98afcd79753d8774b5bb8))
* freeze user cgroup across pause/resume to keep envd /init responsive ([#2688](https://github.com/e2b-dev/infra/issues/2688)) ([eceb741](https://github.com/e2b-dev/infra/commit/eceb7419f9d06de2923bae4c4ff7ca694dd9e81d))
* **orch:** collapse envd's heap into 2 MiB hugepages before pause to cut cold-resume faults ([#2997](https://github.com/e2b-dev/infra/issues/2997)) ([6677f73](https://github.com/e2b-dev/infra/commit/6677f7375c96096d1e9047e13ab923d60d12306c))
* **orch:** distro-aware template base-image provisioning ([#3411](https://github.com/e2b-dev/infra/issues/3411)) ([f8c7b5b](https://github.com/e2b-dev/infra/commit/f8c7b5b03a031d4df2de41322c4e0ea3ef472759))


### Bug Fixes

* added envd to artifact repository ([#3432](https://github.com/e2b-dev/infra/issues/3432)) ([6c4f0e2](https://github.com/e2b-dev/infra/commit/6c4f0e2610ab03cad12fc90eaaea9b74762350db))
* correct 3 CVES ([#3218](https://github.com/e2b-dev/infra/issues/3218)) ([076823b](https://github.com/e2b-dev/infra/commit/076823bc5cbffb9f8c04670c886562445e50ead7))
* **envd:** avoid Start deadlock after request cancellation ([#3256](https://github.com/e2b-dev/infra/issues/3256)) ([04317f8](https://github.com/e2b-dev/infra/commit/04317f8a2270ba7c5d4991937c34c5b388a4bd07))
* **envd:** bound the in-memory logs queue ([#2676](https://github.com/e2b-dev/infra/issues/2676)) ([05c9939](https://github.com/e2b-dev/infra/commit/05c9939f3f4f4a2e5d01801934ca1448ebac5781))
* **envd:** discard output when no subscriber is connected ([#2639](https://github.com/e2b-dev/infra/issues/2639)) ([8cf1795](https://github.com/e2b-dev/infra/commit/8cf17952ccfb2996ea8ef0867c1e70d87904aaba))
* **envd:** fall back to lazy unmount when forced NFS umount fails ([#2683](https://github.com/e2b-dev/infra/issues/2683)) ([5346a0d](https://github.com/e2b-dev/infra/commit/5346a0d238ed89971f767414e39779962797c867))
* **envd:** ignore closed pty read errors ([#2769](https://github.com/e2b-dev/infra/issues/2769)) ([6118672](https://github.com/e2b-dev/infra/commit/6118672453ff404592b5d23916f8abc2f1936884))
* **envd:** include suppressed count in exporter error logs ([#2680](https://github.com/e2b-dev/infra/issues/2680)) ([35c1141](https://github.com/e2b-dev/infra/commit/35c1141139460b3e4e7584741b9df537884e0318))
* **envd:** make /init lock ctx-aware to prevent retry pile-up ([#2702](https://github.com/e2b-dev/infra/issues/2702)) ([173afd4](https://github.com/e2b-dev/infra/commit/173afd472fbbc497368e3a1dbf9e21f5e40c7c84))
* **envd:** make CA install lock ctx-aware ([#2690](https://github.com/e2b-dev/infra/issues/2690)) ([83ee89f](https://github.com/e2b-dev/infra/commit/83ee89f9b303cf66560c30a2a68b080dae4cb82a))
* **envd:** replace env vars in /init instead of merging ([#2706](https://github.com/e2b-dev/infra/issues/2706)) ([1b52e9a](https://github.com/e2b-dev/infra/commit/1b52e9a43f3f57f416a96bff5006562d25c13117))
* **envd:** replace time.Sleep with ticker in ScanAndBroadcast for prompt shutdown ([#3374](https://github.com/e2b-dev/infra/issues/3374)) ([002fd9f](https://github.com/e2b-dev/infra/commit/002fd9f1abf35f311f8434eba21733c87738c838))
* **envd:** self-heal MMDS routing on /init lookup failure ([#2701](https://github.com/e2b-dev/infra/issues/2701)) ([90944d5](https://github.com/e2b-dev/infra/commit/90944d51208908bca779f96d0accae911ac5d0dd))
* **envd:** stop freezing socat cgroup across pause/resume ([#2923](https://github.com/e2b-dev/infra/issues/2923)) ([8b6f2b9](https://github.com/e2b-dev/infra/commit/8b6f2b97e673cc9ea8d62f255540bcb4fcee6ec4))
* **envd:** stop misleading CA install cancel errors on rapid /init ([#3206](https://github.com/e2b-dev/infra/issues/3206)) ([91d09e4](https://github.com/e2b-dev/infra/commit/91d09e4b17ddde628d6b37e1b923aba2e9430531))
* **envd:** suppress repeat MMDS poll failures ([#2678](https://github.com/e2b-dev/infra/issues/2678)) ([73d691a](https://github.com/e2b-dev/infra/commit/73d691a95d0a37f57472ebb8b0be1aea96c7da31))
* **envd:** tolerate busy tmpfs cleanup in tests ([#2938](https://github.com/e2b-dev/infra/issues/2938)) ([a485834](https://github.com/e2b-dev/infra/commit/a48583444ac8dcf86b5769ab57331400fd5798c4))
* **envd:** use constant-time comparison for signature validation ([#3145](https://github.com/e2b-dev/infra/issues/3145)) ([fcf92fa](https://github.com/e2b-dev/infra/commit/fcf92faacc7e4b7b27847aefd48673b8b04950bd))
* **envd:** use WithoutCancel for CA cleanup goroutine ctx ([#3207](https://github.com/e2b-dev/infra/issues/3207)) ([ee7bf84](https://github.com/e2b-dev/infra/commit/ee7bf84b195364e893c855ea6e8d4034bb56c01d))


### Performance Improvements

* **envd:** stop logging streamed payload content ([#2755](https://github.com/e2b-dev/infra/issues/2755)) ([db3868c](https://github.com/e2b-dev/infra/commit/db3868c60a345f6fe78ed2bfd4473e6de433004e))
* **sandbox:** keep envd logging out of journald ([#2675](https://github.com/e2b-dev/infra/issues/2675)) ([f6943ca](https://github.com/e2b-dev/infra/commit/f6943ca7f6370eac50bdca6b09e17b5dc410d7d4))
