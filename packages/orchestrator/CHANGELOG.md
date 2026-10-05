# Changelog

## 0.16.1 (2026-09-10)


### Bug Fixes

* **clean-nfs-cache:** install CA certificates (ec5bb0c)

## 0.16.0 (2026-09-10)


### Features

* **orchestrator:** expose sandbox limit metric (e567524)


### Bug Fixes

* **deps:** move the workspace to the Go 1.26.8 toolchain (e7116f5)
* **deps:** update module github.com/stretchr/testify to v1.12.1 (a58bcb5)
* **deps:** update module github.com/txn2/txeh to v1.8.1 (36a09d6)
* **deps:** update module golang.org/x/oauth2 to v0.37.0 (92ca7bd)
* **deps:** update module google.golang.org/api to v0.297.0 (852f328)
* **deps:** update module google.golang.org/grpc to v1.83.2 [security] (7123140)
* **deps:** update opentelemetry (34ecda9)
* **deps:** update testcontainers-go monorepo to v0.44.0 (5473119)
* **orchestrator:** bound sandbox proxy shutdown (ef855d0)
* **orchestrator:** label a journal replay from the superblock, not e2fsck's exit code (949045d)


### Performance Improvements

* **orchestrator:** confine volume filesystems with openat2 instead of mount namespaces (358f142)

## 0.15.0 (2026-09-10)


### Features

* **orchestrator:** publish sandbox routing record behind feature flags (3bc98a0)
* **orch:** grant envd's memory protection through a system.slice drop-in (90bf96d)
* **orch:** report and record the memory protection on envd's cgroup chain per start (8a1809e)

## 0.14.0 (2026-09-09)


### Features

* **orchestrator:** add shutting-down status (c7bb7eb)
* **orchestrator:** report outstanding work (b51e116)


### Bug Fixes

* **deps:** update module google.golang.org/grpc to v1.83.1 [security] (27006d5)
* **orchestrator:** bound peer blob sends below the receive limit (4e98e00)


### Code Refactoring

* **orchestrator:** track outstanding node work (ffab524)

## 0.13.0 (2026-09-07)


### Features

* **orchestrator:** snapshot-admission pre-flight for pause and checkpoint (flag-gated) (b47217c)
* **templates:** configure free disk targets (b88aab9)
* **templates:** separate build and free disk targets (432ab9a)


### Bug Fixes

* **envd:** stop counting a vanished cgroup as a freeze or thaw failure (9c5f26e)
* **orchestrator:** guard draining status transitions (faf3d8a)
* **orch:** re-send a sandbox's default user so a live envd upgrade cannot lose it (366ea8f)


### Performance Improvements

* **orch:** cache the host envd binary instead of re-reading it per resume (0ab5687)


### Code Refactoring

* **orchestrator:** derive sandbox log fields from the runtime metadata (b8a47d3)

## 0.12.0 (2026-09-02)


### Features

* **ext4:** allow enabling the directory index on the rootfs (4c50658)
* **orchestrator:** stamp build.result on template build spans (087d56a)


### Bug Fixes

* **orchestrator:** close nbd descriptor before data-path teardown (57f7c58)
* **orchestrator:** provision findutils so template finalize survives minimal RPM-family images (12b48d9)
* **orchestrator:** publish cache cleaner image (3a0869d)
* **orchestrator:** remove the fs-only pause version gate (3ecf457)


### Code Refactoring

* **orchestrator:** inject the logger into the uffd memory backend (4ca4bf9)

## 0.11.0 (2026-08-29)


### Features

* **orch:** make a blocked NBD close attributable in traces (71495c2)


### Bug Fixes

* **orchestrator:** remove the cache file when a diff turns out to be empty (acc670b)
* rate-limit stale log drop warnings (9e4d5c4)

## 0.10.0 (2026-08-27)


### Features

* **orchestrator:** evaluate feature flags with an instance group context (cb05c1f)
* **orchestrator:** let the sandbox proxy dial HTTPS backends (7434ff9)
* **orchestrator:** manage the build-time envd via a build-envd-version flag, like kernel and firecracker (35f5a63)
* **orchestrator:** pre-boot filesystem recovery for cold-boot resumes (e6b0067)
* **snapshots:** gate in-place checkpoint and fs-only snapshots on FC release &gt;= 0.2.0 (c7b5dff)


### Bug Fixes

* **orchestrator:** drop raw debugfs output from offline-swap errors (621cdd9)
* **orchestrator:** let release-please stamp the hard-coded version constant (7aa055b)
* **orchestrator:** treat a failed or signalled e2fsck as a failed integrity check (6b03458)

## [0.2.0](https://github.com/e2b-dev/infra/compare/orchestrator-v0.1.0...orchestrator-v0.2.0) (2026-08-24)


### Features

* **checkpoint:** capture the in-place memory export through an async CoW window ([#1948](https://github.com/e2b-dev/infra/issues/1948)) ([8f14236](https://github.com/e2b-dev/infra/commit/8f1423617974aaa2cd1992baaadfdacfb394ec3d))
* **envd:** freeze the customer's cgroups before a pause, not only our own ([#1929](https://github.com/e2b-dev/infra/issues/1929)) ([e117443](https://github.com/e2b-dev/infra/commit/e117443ac949de0810e8eaa9bec9f5d0126d1fa4))
* **envd:** wait for the pre-pause freeze to stop the workload ([#1900](https://github.com/e2b-dev/infra/issues/1900)) ([99ca5cc](https://github.com/e2b-dev/infra/commit/99ca5ccbfdb47a05c3a2914df2c21b0bdfcefa95))
* filesystem-only resume of memory-inclusive snapshots ([#1958](https://github.com/e2b-dev/infra/issues/1958)) ([6061743](https://github.com/e2b-dev/infra/commit/60617431dda40837e8530508f2ed29ac077c2319))
* **shared:** use TypeID encoding for object IDs ([#2016](https://github.com/e2b-dev/infra/issues/2016)) ([4a5075b](https://github.com/e2b-dev/infra/commit/4a5075b135262c8ecd9c53d87fc6617d8811dfd7))


### Bug Fixes

* **network:** match wildcard transform domains ([#1897](https://github.com/e2b-dev/infra/issues/1897)) ([0521db3](https://github.com/e2b-dev/infra/commit/0521db34f974a129cbb62a4e32b8347c5b6cfdad))
* **orchestrator:** apply egress CIDRs whose range ends at 255.255.255.255 ([#2012](https://github.com/e2b-dev/infra/issues/2012)) ([bceeaec](https://github.com/e2b-dev/infra/commit/bceeaec13483548db307a6358b90005e00087f17))
* **orchestrator:** copy skeleton files without invoking cd ([#1949](https://github.com/e2b-dev/infra/issues/1949)) ([b8238ac](https://github.com/e2b-dev/infra/commit/b8238ac7512482d5a87fd4c8e52dfd61387cf411))
* **orchestrator:** stop flushing Firecracker metrics after the VM exits ([#1335](https://github.com/e2b-dev/infra/issues/1335)) ([454a1ae](https://github.com/e2b-dev/infra/commit/454a1aea3f038b01ef01be9aaba006cfc3258de4))
* **proxy:** add CORS to synthesized responses and answer proxy-only preflights ([#2068](https://github.com/e2b-dev/infra/issues/2068)) ([26faf4f](https://github.com/e2b-dev/infra/commit/26faf4f4dae8f87ca5682bcfca85c03f2a27b215))

## [0.1.0](https://github.com/e2b-dev/infra/compare/orchestrator-v0.0.2...orchestrator-v0.1.0) (2026-08-18)


### Features

* **checkpoint:** in-place checkpoint — snapshot a sandbox without stopping it ([#1852](https://github.com/e2b-dev/infra/issues/1852)) ([0001909](https://github.com/e2b-dev/infra/commit/0001909770c6a8f7ff25331ddbc0970e1f289967))
* **data-exporter:** add export pipeline metrics ([#1324](https://github.com/e2b-dev/infra/issues/1324)) ([46df4d2](https://github.com/e2b-dev/infra/commit/46df4d2320ea38bb12e991cfda6d5da860b9fbfb))
* **fcversion:** parse vX.Y-&lt;e2b-semver&gt; tags alongside {tag}_{sha} ([#1679](https://github.com/e2b-dev/infra/issues/1679)) ([a8cb6fb](https://github.com/e2b-dev/infra/commit/a8cb6fb8ce04d60fadfe566ef23bdc59b82c089d))
* **orchestrator:** install ss in template builds and the NixOS base image ([#1724](https://github.com/e2b-dev/infra/issues/1724)) ([e14d760](https://github.com/e2b-dev/infra/commit/e14d760bd84f68849e58a9c00d69883ad10d16be))
* **orchestrator:** nftables sandbox network datapath behind NETWORK_VERSION=2 ([#1718](https://github.com/e2b-dev/infra/issues/1718)) ([9aafb50](https://github.com/e2b-dev/infra/commit/9aafb5027f983ee9d2de62bbf803873b2f295ea3))
* **orchestrator:** record sandbox execution duration by stop reason ([#1464](https://github.com/e2b-dev/infra/issues/1464)) ([9708b49](https://github.com/e2b-dev/infra/commit/9708b49a4a802aea54166ae3bd4ff2c334687b13))
* **orch:** export the virtio-blk queue notification count ([#1914](https://github.com/e2b-dev/infra/issues/1914)) ([31df2e3](https://github.com/e2b-dev/infra/commit/31df2e3383670add37b159175dd17efe540d9e62))
* **orch:** forbid envd's private endpoints in the sandbox proxy ([#1862](https://github.com/e2b-dev/infra/issues/1862)) ([b6de7ae](https://github.com/e2b-dev/infra/commit/b6de7ae33d1d687fe7c115680916d9f3ddbc8feb))
* **orch:** separate egress DSCP for template builds and sandboxes ([#1356](https://github.com/e2b-dev/infra/issues/1356)) ([f464c74](https://github.com/e2b-dev/infra/commit/f464c74bbe777c154461e0c4d979389ea666dda4))
* **orch:** supply guest kernel cmdline parameters per team from a feature flag ([#1790](https://github.com/e2b-dev/infra/issues/1790)) ([65e33c4](https://github.com/e2b-dev/infra/commit/65e33c4cab9a7c27cb18117ecc6a70f49e7e771e))
* **orch:** swap rootfs envd binary before fs-only reboot ([d3385ef](https://github.com/e2b-dev/infra/commit/d3385ef3d609382cee82cc42dc5d1eb755a3b303))
* **orch:** upgrade envd in filesystem-only snapshots via offline rootfs swap ([#1363](https://github.com/e2b-dev/infra/issues/1363)) ([695992b](https://github.com/e2b-dev/infra/commit/695992bfb00b275b8269369311dcfbe0fe537aff))
* **redis:** password auth and explicit TLS enablement ([#1587](https://github.com/e2b-dev/infra/issues/1587)) ([1c49ecf](https://github.com/e2b-dev/infra/commit/1c49ecfc08c1b65d280a440c42cddd45b113744d))
* **registry:** Azure Container Registry providers ([#1586](https://github.com/e2b-dev/infra/issues/1586)) ([8bc0e78](https://github.com/e2b-dev/infra/commit/8bc0e78dd211d547b20f6a9095b8e1b319648bc8))
* **storage:** Azure Blob storage provider behind an azblob:// URL ([#1585](https://github.com/e2b-dev/infra/issues/1585)) ([c0f65b6](https://github.com/e2b-dev/infra/commit/c0f65b6730e4b2131f55bc33b1e62c0cb9d97e36))
* **uffd:** export sync-WP burn-in metrics (wp_mode, dirty divergence) ([#1676](https://github.com/e2b-dev/infra/issues/1676)) ([a6bb2bf](https://github.com/e2b-dev/infra/commit/a6bb2bf0dab8a0395a9835d663fc4507afece4e1))
* **uffd:** implement handling of synchronous WP faults in UFFD handler and use them to track dirty state ([#1459](https://github.com/e2b-dev/infra/issues/1459)) ([2808228](https://github.com/e2b-dev/infra/commit/2808228b628086ebc2d333a309667046a122830b))
* **uffd:** serve the pause-time dirty set from the page tracker under sync-WP ([#1759](https://github.com/e2b-dev/infra/issues/1759)) ([fdc3793](https://github.com/e2b-dev/infra/commit/fdc3793cde85c7cfb4e7de9522fbf135f66c1e5b))


### Bug Fixes

* **deps:** update module cloud.google.com/go/artifactregistry to v1.26.0 ([#1791](https://github.com/e2b-dev/infra/issues/1791)) ([7d1778c](https://github.com/e2b-dev/infra/commit/7d1778cb404c723a3e05f9becc8bdf6210c4989c))
* **deps:** update module cloud.google.com/go/storage to v1.64.0 ([#1793](https://github.com/e2b-dev/infra/issues/1793)) ([e66f1a8](https://github.com/e2b-dev/infra/commit/e66f1a8610c8119df23292620d16ed84e98e700e))
* **deps:** update module github.com/getkin/kin-openapi to v0.146.0 ([#1582](https://github.com/e2b-dev/infra/issues/1582)) ([5307ba8](https://github.com/e2b-dev/infra/commit/5307ba8613599106061c0b55f71be75718c2ad3c))
* **deps:** update module google.golang.org/api to v0.292.0 ([#1699](https://github.com/e2b-dev/infra/issues/1699)) ([a10138c](https://github.com/e2b-dev/infra/commit/a10138ce9b7db87b614444ac85c581ae2d7839c8))
* **deps:** update module google.golang.org/grpc to v1.83.0 ([#1583](https://github.com/e2b-dev/infra/issues/1583)) ([acbefda](https://github.com/e2b-dev/infra/commit/acbefdae0e45d5c816cc8659dc8d34784ffa0056))
* **deps:** update opentelemetry ([#1700](https://github.com/e2b-dev/infra/issues/1700)) ([ea3100a](https://github.com/e2b-dev/infra/commit/ea3100a890ad7e2ba890b0112c41576589eb0e9d))
* **deps:** update opentelemetry-go-contrib monorepo ([#1607](https://github.com/e2b-dev/infra/issues/1607)) ([5852c94](https://github.com/e2b-dev/infra/commit/5852c942f9b5cff8a66ad008196d7025273febea))
* **deps:** update testcontainers-go monorepo to v0.43.0 ([#1701](https://github.com/e2b-dev/infra/issues/1701)) ([08b1cbe](https://github.com/e2b-dev/infra/commit/08b1cbe6b6e4e4cddf4b91c068e0b4632ce2e94c))
* **orchestrator:** harden local network slot storage ([#1490](https://github.com/e2b-dev/infra/issues/1490)) ([b3f5cd2](https://github.com/e2b-dev/infra/commit/b3f5cd27a66c7c2f31ee2fd46f0fbc915f6c752b))
* **orchestrator:** recover lost ancestor entries when persisting headers ([#1389](https://github.com/e2b-dev/infra/issues/1389)) ([48d6a6b](https://github.com/e2b-dev/infra/commit/48d6a6b4267bc320a563d51764fbeff641b1d2b7))
* **orchestrator:** restore dpkg-owned paths under /etc/ssl/certs before packing the cert bundle ([#1574](https://github.com/e2b-dev/infra/issues/1574)) ([7800eac](https://github.com/e2b-dev/infra/commit/7800eac55fb393504b56912eb60beb6b33cab8ff))
* **orch:** keep the harvested prefetch mapping when the snapshot upload is slow ([#1765](https://github.com/e2b-dev/infra/issues/1765)) ([38f6593](https://github.com/e2b-dev/infra/commit/38f659329c3015a1138454137cb817407f91dfeb))
* **orch:** report NBD device writeback failures at flush ([#1865](https://github.com/e2b-dev/infra/issues/1865)) ([539375e](https://github.com/e2b-dev/infra/commit/539375e40cbb139f7bf3f353643231e1e3b6443e))
* **orch:** smoketest never ran on infra — envd locator predates the restructure ([#1386](https://github.com/e2b-dev/infra/issues/1386)) ([139a6bd](https://github.com/e2b-dev/infra/commit/139a6bd8e1a1516096bbfc965744969a80fe290d))
* **oss:** upload with gcloud storage instead of gsutil ([#1302](https://github.com/e2b-dev/infra/issues/1302)) ([5f05b8f](https://github.com/e2b-dev/infra/commit/5f05b8f247c3b3b1ddc1bce0f8a837c5568dd70a))
* **smoketest:** wait for envd before probing the freshly-resumed guest ([#1898](https://github.com/e2b-dev/infra/issues/1898)) ([041ff92](https://github.com/e2b-dev/infra/commit/041ff929ee6001dd4ed00da9020dbb6b13612995))

## [0.0.2](https://github.com/e2b-dev/infra/compare/orchestrator-v0.0.1...orchestrator-v0.0.2) (2026-07-31)


### Bug Fixes

* **orch:** choose the chrony time source at boot instead of at build time ([#3440](https://github.com/e2b-dev/infra/issues/3440)) ([10b1bae](https://github.com/e2b-dev/infra/commit/10b1bae7c8c6d95baf599ada006981347d507568))
* **orch:** pin nixpkgs to a supported release for the NixOS base image ([#3478](https://github.com/e2b-dev/infra/issues/3478)) ([6e8e9d3](https://github.com/e2b-dev/infra/commit/6e8e9d35ddeb45bc54a1e30322d04d363c1923cc))
* **template-build:** create trailing-slash COPY targets before moving files ([#3458](https://github.com/e2b-dev/infra/issues/3458)) ([dc53fa7](https://github.com/e2b-dev/infra/commit/dc53fa7fd7989eec69a166e7a0456eabd18101f4))

## 0.0.1 (2026-07-30)


### Features

* **api:** add sandbox IAM workload token configuration ([13ddb3d](https://github.com/e2b-dev/infra/commit/13ddb3dedc992dbb2683b086cf85a5bbbd7f9720))
* **api:** add sandbox workload identity permission ([#3319](https://github.com/e2b-dev/infra/issues/3319)) ([13ddb3d](https://github.com/e2b-dev/infra/commit/13ddb3dedc992dbb2683b086cf85a5bbbd7f9720))
* **api:** SOCKS5 egress proxy on sandbox network config (BYOP) ([#2642](https://github.com/e2b-dev/infra/issues/2642)) ([1fc3820](https://github.com/e2b-dev/infra/commit/1fc3820d7319ee2bef18c1a66bfa8294b8dc053b))
* **cfg:** add DISABLE_STARTUP_RECLAIM flag ([#3081](https://github.com/e2b-dev/infra/issues/3081)) ([7677ca6](https://github.com/e2b-dev/infra/commit/7677ca67e96b7679185ed7d36c9c710b31a91089))
* **clickhouse:** implement multi-cluster fan-out for events and stats ([#2925](https://github.com/e2b-dev/infra/issues/2925)) ([39594c6](https://github.com/e2b-dev/infra/commit/39594c6eacba37a124ed5f2c8a8af95319c87ead))
* dynamic sandbox log routing and ClickHouse-backed log reads ([#3236](https://github.com/e2b-dev/infra/issues/3236)) ([1b19a3b](https://github.com/e2b-dev/infra/commit/1b19a3bcb37d1fb44171ae8dee3a126cf3d39c34))
* **envd:** give envd realtime IO priority, reset for user processes ([#2681](https://github.com/e2b-dev/infra/issues/2681)) ([f4bd1b2](https://github.com/e2b-dev/infra/commit/f4bd1b24ba77185a6eafcd6435bb4a3330ee7998))
* **envd:** split collapse stats into real migrations vs already-huge ([#3021](https://github.com/e2b-dev/infra/issues/3021)) ([0d77614](https://github.com/e2b-dev/infra/commit/0d7761465acf9bd78ec1049c67769c7db008e35b))
* **envd:** support user-defined file metadata via xattrs ([#2732](https://github.com/e2b-dev/infra/issues/2732)) ([da8fbe4](https://github.com/e2b-dev/infra/commit/da8fbe49c344028441c98afcd79753d8774b5bb8))
* **featureflags:** support per-service context providers ([#3100](https://github.com/e2b-dev/infra/issues/3100)) ([65297c1](https://github.com/e2b-dev/infra/commit/65297c10ab1f9a06ce5f9c5e6afa88a005d59902))
* freeze user cgroup across pause/resume to keep envd /init responsive ([#2688](https://github.com/e2b-dev/infra/issues/2688)) ([eceb741](https://github.com/e2b-dev/infra/commit/eceb7419f9d06de2923bae4c4ff7ca694dd9e81d))
* **metrics:** break down pause-snapshot latency by step ([#3426](https://github.com/e2b-dev/infra/issues/3426)) ([657559e](https://github.com/e2b-dev/infra/commit/657559e5f49a41ad4d4437315a6c65291d16d701))
* **metrics:** label pause telemetry by fs_only ([#3425](https://github.com/e2b-dev/infra/issues/3425)) ([411b63e](https://github.com/e2b-dev/infra/commit/411b63edf6dea8edd265d1c5e442f41a80b76b2a))
* **observability:** add kill_reason to sandbox.lifecycle.killed ([#2833](https://github.com/e2b-dev/infra/issues/2833)) ([e45418f](https://github.com/e2b-dev/infra/commit/e45418fd59eafa0aba1f34b34eae83723730bb6c))
* **observability:** include kill_reason in kill-path structured logs ([#2846](https://github.com/e2b-dev/infra/issues/2846)) ([33c49f7](https://github.com/e2b-dev/infra/commit/33c49f725fc8e9d0feba16a55987461c09de9680))
* **orch:** add envd-version to LaunchDarkly sandbox context ([#3051](https://github.com/e2b-dev/infra/issues/3051)) ([37d3b92](https://github.com/e2b-dev/infra/commit/37d3b92b4ccbc56cc4bbb19f347e4546e885db6e))
* **orch:** add less, nftables, iputils-ping, and jq to base provisioning ([#2736](https://github.com/e2b-dev/infra/issues/2736)) ([a1e010e](https://github.com/e2b-dev/infra/commit/a1e010ea550a563f8385cd901e18bef9a5e7f1f1))
* **orch:** collapse envd's heap into 2 MiB hugepages before pause to cut cold-resume faults ([#2997](https://github.com/e2b-dev/infra/issues/2997)) ([6677f73](https://github.com/e2b-dev/infra/commit/6677f7375c96096d1e9047e13ab923d60d12306c))
* **orch:** debug a sandbox guest kernel with resume-build -gdb ([#3040](https://github.com/e2b-dev/infra/issues/3040)) ([37bb0dc](https://github.com/e2b-dev/infra/commit/37bb0dcc0a7fb88b9761d1a412a0afc4c2b993f8))
* **orch:** decouple warm resume from memfile dedup ([#3166](https://github.com/e2b-dev/infra/issues/3166)) ([77f25a0](https://github.com/e2b-dev/infra/commit/77f25a0de4f5cf6d375349d0afada5bc28109db9))
* **orch:** distro-aware template base-image provisioning ([#3411](https://github.com/e2b-dev/infra/issues/3411)) ([f8c7b5b](https://github.com/e2b-dev/infra/commit/f8c7b5b03a031d4df2de41322c4e0ea3ef472759))
* **orchestrator/cgroup:** list and destroy leaked sandbox cgroups ([#3086](https://github.com/e2b-dev/infra/issues/3086)) ([bce1d84](https://github.com/e2b-dev/infra/commit/bce1d84c45909438e08bbaeab913e9579fe5c359))
* **orchestrator/nbd:** inspect and disconnect connected devices ([#3087](https://github.com/e2b-dev/infra/issues/3087)) ([4d47148](https://github.com/e2b-dev/infra/commit/4d47148ff8f86a561d8112d87e381f8a1b30b348))
* **orchestrator/network:** list slot namespaces ([#3089](https://github.com/e2b-dev/infra/issues/3089)) ([c23dbc7](https://github.com/e2b-dev/infra/commit/c23dbc796fab828bab8d391f9cccf916984d28fb))
* **orchestrator/network:** list slot namespaces ([#3090](https://github.com/e2b-dev/infra/issues/3090)) ([fbfce25](https://github.com/e2b-dev/infra/commit/fbfce25cb8726fe0d899f126b2401dd9c26e3ec1))
* **orchestrator:** add -force-reboot to resume-build to cold-boot memory-snaphsot builds ([#3208](https://github.com/e2b-dev/infra/issues/3208)) ([cf8f15b](https://github.com/e2b-dev/infra/commit/cf8f15bda826b909d7f2770b1dcceb17ee8ba85f))
* **orchestrator:** add allocated resource metrics for sandboxes ([#2943](https://github.com/e2b-dev/infra/issues/2943)) ([95cb6d3](https://github.com/e2b-dev/infra/commit/95cb6d38229c949eddcbb9495b7f3bf1baf4550a))
* **orchestrator:** add dummy orchestrator binary for local API dev ([#2744](https://github.com/e2b-dev/infra/issues/2744)) ([ab56e25](https://github.com/e2b-dev/infra/commit/ab56e25437d479af1b8c683afca859bad6543987))
* **orchestrator:** add NetworkAssignHook for sandbox lifecycle extensions ([#3290](https://github.com/e2b-dev/infra/issues/3290)) ([3261963](https://github.com/e2b-dev/infra/commit/3261963a0c6b3adcee8f7519196c80da6f9d584e))
* **orchestrator:** add soft-delete marker label to the check metric ([#3144](https://github.com/e2b-dev/infra/issues/3144)) ([1ce64f8](https://github.com/e2b-dev/infra/commit/1ce64f8a2ece42a5480cb1cb9792e2c71e70469f))
* **orchestrator:** add v4HeaderForUncompressed FF bit ([#2669](https://github.com/e2b-dev/infra/issues/2669)) ([1f459ee](https://github.com/e2b-dev/infra/commit/1f459eec6536491641e01cc4591dc48b5c4e7690))
* **orchestrator:** always include execution metrics in sandbox webhook events ([#2852](https://github.com/e2b-dev/infra/issues/2852)) ([440edfe](https://github.com/e2b-dev/infra/commit/440edfe2d5ad1870d0652b2a30e3cd341a76df09))
* **orchestrator:** classify envd-init by exit type ([#3139](https://github.com/e2b-dev/infra/issues/3139)) ([1e39a4f](https://github.com/e2b-dev/infra/commit/1e39a4f42e7d56b3bb36ad2f8a73aadc5ba1baf8))
* **orchestrator:** graceful sandbox drain on shutdown ([#3069](https://github.com/e2b-dev/infra/issues/3069)) ([6ce68e3](https://github.com/e2b-dev/infra/commit/6ce68e3d52b8784b0bcb61f693d92c041d54cabe))
* **orchestrator:** graceful template-build drain on shutdown ([#3079](https://github.com/e2b-dev/infra/issues/3079)) ([1b3001c](https://github.com/e2b-dev/infra/commit/1b3001c81bd1d6bd3d23a58f66528ed7b78c75d5))
* **orchestrator:** improved read-path telemetry ([#3063](https://github.com/e2b-dev/infra/issues/3063)) ([bc3fe84](https://github.com/e2b-dev/infra/commit/bc3fe8450ca632024d0f47de88fdead8316d792d))
* **orchestrator:** LD-gated ClickHouse write fan-out feature flag ([#3152](https://github.com/e2b-dev/infra/issues/3152)) ([f046fcf](https://github.com/e2b-dev/infra/commit/f046fcf626a7e91f99c204507a8bf2ceed39e3e6))
* **orchestrator:** make build-reserved-disk-space-mb default 256MB ([#3065](https://github.com/e2b-dev/infra/issues/3065)) ([d473f98](https://github.com/e2b-dev/infra/commit/d473f989fc921a246126fddc638d58281131a12d))
* **orchestrator:** record upload compression metrics ([#2761](https://github.com/e2b-dev/infra/issues/2761)) ([9092e35](https://github.com/e2b-dev/infra/commit/9092e35b75254bcc98ef5312de0482b08dd6b472))
* **orchestrator:** report hugepage metrics to API ([#3182](https://github.com/e2b-dev/infra/issues/3182)) ([7735bae](https://github.com/e2b-dev/infra/commit/7735bae35bd3cd3251a929e13fa9adf2333fd1e8))
* **orchestrator:** run startup reclaim on boot ([#3123](https://github.com/e2b-dev/infra/issues/3123)) ([79b838e](https://github.com/e2b-dev/infra/commit/79b838e4a6583c06d920bf32bf5e35e90a4ea81f))
* **orchestrator:** single-instance flock on startup ([#3143](https://github.com/e2b-dev/infra/issues/3143)) ([1320d6e](https://github.com/e2b-dev/infra/commit/1320d6e3827f32f40a142b55888cf7f7fa46753d))
* **orchestrator:** soft-delete consumer enforcement for storage index ([#3034](https://github.com/e2b-dev/infra/issues/3034)) ([fbfc918](https://github.com/e2b-dev/infra/commit/fbfc918e781fc84765e90fd22f8560f03ab5405f))
* **orchestrator:** tag envd-init meters with start_type ([#3125](https://github.com/e2b-dev/infra/issues/3125)) ([4466b48](https://github.com/e2b-dev/infra/commit/4466b48731159a9a86fab8dea49a709f0fc418c6))
* **orchestrator:** track and report last status change timestamp ([#2980](https://github.com/e2b-dev/infra/issues/2980)) ([f79be77](https://github.com/e2b-dev/infra/commit/f79be779ecbc1c38add07664ffbbe154bf7755b5))
* **orchestrator:** track sandbox lifecycles ([#2998](https://github.com/e2b-dev/infra/issues/2998)) ([057f20c](https://github.com/e2b-dev/infra/commit/057f20c97dcb504abfa7ce433996c061eec3930a))
* **orchestrator:** write layer sizes (logical/mapped/diff) to object metadata ([#3122](https://github.com/e2b-dev/infra/issues/3122)) ([11869c0](https://github.com/e2b-dev/infra/commit/11869c0fffb03f5f03b1c43154f148139fac9b33))
* **orch:** harvest resume-prefetch trace on pause ([#3067](https://github.com/e2b-dev/infra/issues/3067)) ([97bd4a5](https://github.com/e2b-dev/infra/commit/97bd4a55df42d54012732d699065b9e1b672e496))
* **orch:** last-cycle memory prefetch on resume ([#3258](https://github.com/e2b-dev/infra/issues/3258)) ([b22e820](https://github.com/e2b-dev/infra/commit/b22e820540d4cfe40d0fb3279e0a4edaaed9bd90))
* **orch:** make resume-build -gdb work on real nodes + add copy-build -gdb ([#3108](https://github.com/e2b-dev/infra/issues/3108)) ([c684bd2](https://github.com/e2b-dev/infra/commit/c684bd25c97ec6438f31a8eb9b72bbaf04c7e07e))
* **orch:** opt-in DSCP marker for sandbox egress (SANDBOX_EGRESS_DSCP) ([#3039](https://github.com/e2b-dev/infra/issues/3039)) ([a98cf2c](https://github.com/e2b-dev/infra/commit/a98cf2c86d38969ba4c76ef82beccdd9b4798964))
* **orch:** per-start UFFD startup working-set metric ([#2960](https://github.com/e2b-dev/infra/issues/2960)) ([dc386b2](https://github.com/e2b-dev/infra/commit/dc386b242c9652b2562496d3867ea9035611528e))
* **orch:** premade NixOS base-image support ([#3412](https://github.com/e2b-dev/infra/issues/3412)) ([776ba39](https://github.com/e2b-dev/infra/commit/776ba39ed7f46c2b0743114ce1b5a4bb564f9570))
* **orch:** record envd init duration histogram on failure with success attribute ([#2749](https://github.com/e2b-dev/infra/issues/2749)) ([afa7458](https://github.com/e2b-dev/infra/commit/afa7458d846bf0ea77f1875b797aaad604365e36))
* **orch:** snapshot fragmentation metrics ([#2931](https://github.com/e2b-dev/infra/issues/2931)) ([842b007](https://github.com/e2b-dev/infra/commit/842b0073f898d357e6d56bd74f215328af2bb984))
* per-team events TTL limit (tier + addons) ([#3181](https://github.com/e2b-dev/infra/issues/3181)) ([f76b2cb](https://github.com/e2b-dev/infra/commit/f76b2cb622efde2e4958caa8358dfc95ba0b4ce7))
* **shared:** add OTEL instrumentation to AWS S3 storage client ([#3172](https://github.com/e2b-dev/infra/issues/3172)) ([25b0fd1](https://github.com/e2b-dev/infra/commit/25b0fd1f427850410eb71429063ccff76ce298d8))
* **storage:** per-role storage URLs, env-free storage library ([#3246](https://github.com/e2b-dev/infra/issues/3246)) ([fcbe909](https://github.com/e2b-dev/infra/commit/fcbe9097d1c6cbda0e74e1c7ca706f2a9a315475))
* **storage:** stamp provenance custom metadata on uploaded objects (incl. headers) ([#3033](https://github.com/e2b-dev/infra/issues/3033)) ([ba8604e](https://github.com/e2b-dev/infra/commit/ba8604e0621baefd61aaf1ea34bec27805c126cf))
* **storage:** write-through compressed templates to NFS on upload ([#2827](https://github.com/e2b-dev/infra/issues/2827)) ([57503c1](https://github.com/e2b-dev/infra/commit/57503c1a9969d9dce76b27ce87b4c9d2fc11ee9e))


### Bug Fixes

* added api and orch ([#3454](https://github.com/e2b-dev/infra/issues/3454)) ([fda5e45](https://github.com/e2b-dev/infra/commit/fda5e4579c59729cb8510dd47965c5192813ac8b))
* **block:** rephrase misleading error message in pwritevAll ([#2816](https://github.com/e2b-dev/infra/issues/2816)) ([1555f1b](https://github.com/e2b-dev/infra/commit/1555f1b7bcbb53cbb714a3e568a708e9d469b21a))
* **cache:** use 512-byte units for stat.Blocks in FileSize ([#2949](https://github.com/e2b-dev/infra/issues/2949)) ([0f632a9](https://github.com/e2b-dev/infra/commit/0f632a9fb125e17fcd575671d3fd5f7637cd820b))
* **clean-nfs-cache:** exclude zombies from delete_age ([#3191](https://github.com/e2b-dev/infra/issues/3191)) ([3fa2aeb](https://github.com/e2b-dev/infra/commit/3fa2aeb935526810c4e93248a1e36677ea65b582))
* **compression:** correctness findings from compression audit ([#2803](https://github.com/e2b-dev/infra/issues/2803)) ([d21a6a9](https://github.com/e2b-dev/infra/commit/d21a6a9ddc1c70302d4e86e31c9d86744ae535e6))
* **copy-build:** resolve compression suffix for build data files ([#2859](https://github.com/e2b-dev/infra/issues/2859)) ([8966f7e](https://github.com/e2b-dev/infra/commit/8966f7e2203c9a3b6738197f9f7b4c84f04495a2))
* correct 3 CVES ([#3218](https://github.com/e2b-dev/infra/issues/3218)) ([076823b](https://github.com/e2b-dev/infra/commit/076823bc5cbffb9f8c04670c886562445e50ead7))
* **envd:** stop freezing socat cgroup across pause/resume ([#2923](https://github.com/e2b-dev/infra/issues/2923)) ([8b6f2b9](https://github.com/e2b-dev/infra/commit/8b6f2b97e673cc9ea8d62f255540bcb4fcee6ec4))
* **inspect-build:** adapt validate to new Chunker upstream API ([#2989](https://github.com/e2b-dev/infra/issues/2989)) ([2e0d3da](https://github.com/e2b-dev/infra/commit/2e0d3dac867ebc6e86148ab3fe3d974e191e8a35))
* **nbd:** adjust status poll sleep from 100ns to 100µs ([02bf51b](https://github.com/e2b-dev/infra/commit/02bf51bc6204c39892a0cc0b6ca1938ab14fb48f))
* **nbd:** change NBD status poll sleep from 100ns to 100µs to avoid useless busy spinning ([#2884](https://github.com/e2b-dev/infra/issues/2884)) ([02bf51b](https://github.com/e2b-dev/infra/commit/02bf51bc6204c39892a0cc0b6ca1938ab14fb48f))
* **nfsproxy:** deflake TestRoundTrip EADDRINUSE ([#2987](https://github.com/e2b-dev/infra/issues/2987)) ([55f4d18](https://github.com/e2b-dev/infra/commit/55f4d18d870a127b357c9c6580b37502d1c0f402))
* **orch:** denormalize upload metric file type ([#2865](https://github.com/e2b-dev/infra/issues/2865)) ([b1646ca](https://github.com/e2b-dev/infra/commit/b1646ca204f3d8fde0051d3b81aec8dcc154f367))
* **orch:** disable the chronyd seccomp filter on Alpine when using PHC ([#3453](https://github.com/e2b-dev/infra/issues/3453)) ([e58af28](https://github.com/e2b-dev/infra/commit/e58af2843f113a88c49be126302bebf70e073a92))
* **orchestrator:** anchor rsync CWD to root in template file copy ([#2835](https://github.com/e2b-dev/infra/issues/2835)) ([7160db9](https://github.com/e2b-dev/infra/commit/7160db9e594270d5fde9e2d57e7e3eeebb4cf627))
* **orchestrator:** atomically replace metadata ([#3321](https://github.com/e2b-dev/infra/issues/3321)) ([0c4ad6b](https://github.com/e2b-dev/infra/commit/0c4ad6b8d25ce1c5ffa82e23929e40d7a83cb8e7))
* **orchestrator:** avoid serializing upload headers twice ([#2762](https://github.com/e2b-dev/infra/issues/2762)) ([9b7b149](https://github.com/e2b-dev/infra/commit/9b7b149f929eed06710ab733c79bba888578c76b))
* **orchestrator:** chunk readiness bug in P2P-&gt;compressed ([#3185](https://github.com/e2b-dev/infra/issues/3185)) ([74a6e5b](https://github.com/e2b-dev/infra/commit/74a6e5b354a637ad9ce5fddbfc7c1e3c4aae7023))
* **orchestrator:** deschedule flaky eviction-loop race in TestDiffSto… ([#3173](https://github.com/e2b-dev/infra/issues/3173)) ([88ff17c](https://github.com/e2b-dev/infra/commit/88ff17c3017abf3db938bf927c241f9ab04e6278))
* **orchestrator:** discard poisoned nftables conn on firewall errors ([#3008](https://github.com/e2b-dev/infra/issues/3008)) ([03f10e0](https://github.com/e2b-dev/infra/commit/03f10e07003de504cbb00d6e688fc994992cdf7b))
* **orchestrator:** drop stale pre-init logs ([#3297](https://github.com/e2b-dev/infra/issues/3297)) ([8ec4be5](https://github.com/e2b-dev/infra/commit/8ec4be5ffd52c9a86953a6a96eaa52c7141f8ab3))
* **orchestrator:** emit compression ratios as fractions, not BP ([#2772](https://github.com/e2b-dev/infra/issues/2772)) ([866f4c1](https://github.com/e2b-dev/infra/commit/866f4c106f2ee91b2c888a6c2cc8164697cee359))
* **orchestrator:** export dirty-page stall counter from process start ([#2992](https://github.com/e2b-dev/infra/issues/2992)) ([badc8ad](https://github.com/e2b-dev/infra/commit/badc8ad8db13f405efc0dab44725ceed8c2e3971))
* **orchestrator:** harden Firecracker process shutdown ([#2996](https://github.com/e2b-dev/infra/issues/2996)) ([df662e7](https://github.com/e2b-dev/infra/commit/df662e7fd223d0b761893e3cd372df0cc79240a5))
* **orchestrator:** harden shutdown network cleanup ([#3000](https://github.com/e2b-dev/infra/issues/3000)) ([de2f391](https://github.com/e2b-dev/infra/commit/de2f39199436238aa7a75da7748d5a91e101b7ae))
* **orchestrator:** implement Docker COPY merge semantics in template builds ([#3283](https://github.com/e2b-dev/infra/issues/3283)) ([9174104](https://github.com/e2b-dev/infra/commit/9174104159839e79851c823c76ec176f860e9b7c))
* **orchestrator:** keep dedup empty-pages telemetry scan-only ([#2991](https://github.com/e2b-dev/infra/issues/2991)) ([35d0832](https://github.com/e2b-dev/infra/commit/35d083296f03496c5802a54beab90321b29fd6ed))
* **orchestrator:** let build-cache threshold flag raise above its fal… ([#3175](https://github.com/e2b-dev/infra/issues/3175)) ([06393c3](https://github.com/e2b-dev/infra/commit/06393c3407fdbb61f05eb14e054f810fbcbb90bf))
* **orchestrator:** log missing egress proxy in startup reclaim instead of defaulting silently ([#3116](https://github.com/e2b-dev/infra/issues/3116)) ([6ca3163](https://github.com/e2b-dev/infra/commit/6ca31638fad1c5fbebad8e80bca8a966d813e081))
* **orchestrator:** make copy-build handle filesystem-only snapshots ([#3299](https://github.com/e2b-dev/infra/issues/3299)) ([62add04](https://github.com/e2b-dev/infra/commit/62add0400fd6b60924deca71f30b448a994b2c73))
* **orchestrator:** measure ext4 free space from block groups ([#3282](https://github.com/e2b-dev/infra/issues/3282)) ([f18f05f](https://github.com/e2b-dev/infra/commit/f18f05f02b9d55ca0376130e5ecc2e64a3498644))
* **orchestrator:** normalize upload metric file labels ([#2767](https://github.com/e2b-dev/infra/issues/2767)) ([6dec8b3](https://github.com/e2b-dev/infra/commit/6dec8b32370d1281b065fc3bb5481581f0d94479))
* **orchestrator:** order egress config/firewall updates to close BYOP enable race ([#3313](https://github.com/e2b-dev/infra/issues/3313)) ([7faa59e](https://github.com/e2b-dev/infra/commit/7faa59e64472435c686456d82e54be1d98ad7e4a))
* **orchestrator:** order envd.service after local-fs.target ([#3043](https://github.com/e2b-dev/infra/issues/3043)) ([ea2663e](https://github.com/e2b-dev/infra/commit/ea2663e1bb8fba667bd8308db79d5f0f8c6ff9c8))
* **orchestrator:** order envd.service after systemd-tmpfiles-setup ([#3130](https://github.com/e2b-dev/infra/issues/3130)) ([9481811](https://github.com/e2b-dev/infra/commit/94818117c2376018519c08af8c2308cc756b7a80))
* **orchestrator:** pause upload retain retry ([#2993](https://github.com/e2b-dev/infra/issues/2993)) ([4f81799](https://github.com/e2b-dev/infra/commit/4f817994bb67217bafa71ebfb5c67e7a7b51ba1d))
* **orchestrator:** pin tap device host-side MAC address ([#3271](https://github.com/e2b-dev/infra/issues/3271)) ([3c786ba](https://github.com/e2b-dev/infra/commit/3c786ba3bb1ee9c4f421672084edd112e27408b3))
* **orchestrator:** pin UFFD copy source buffers ([#2745](https://github.com/e2b-dev/infra/issues/2745)) ([837fa91](https://github.com/e2b-dev/infra/commit/837fa91ceb40dcde7711c6f472a79671af539590))
* **orchestrator:** preserve full ENV value across stdout chunks ([#2740](https://github.com/e2b-dev/infra/issues/2740)) ([4822e6d](https://github.com/e2b-dev/infra/commit/4822e6dee026a69bcca58b4b812660a28f18334d))
* **orchestrator:** read V3 ancestors as uncompressed instead of failing ([#2994](https://github.com/e2b-dev/infra/issues/2994)) ([c479dd3](https://github.com/e2b-dev/infra/commit/c479dd390964bdfd408998e6f4e55b6d3cbbc266))
* **orchestrator:** reject standby while draining ([#3325](https://github.com/e2b-dev/infra/issues/3325)) ([475a7ee](https://github.com/e2b-dev/infra/commit/475a7eee7c94f35a834b1978b189b75890c2bbe7))
* **orchestrator:** report real V4 header compression ratio ([#2771](https://github.com/e2b-dev/infra/issues/2771)) ([ecd344e](https://github.com/e2b-dev/infra/commit/ecd344e3581e5e5ff51fcd397a4cc87ccdae2e40))
* **orchestrator:** resolve remaining P2P/compression/V5 issues ([#3015](https://github.com/e2b-dev/infra/issues/3015)) ([1e4379e](https://github.com/e2b-dev/infra/commit/1e4379e8993e458858047fa26ec32763b61374c3))
* **orchestrator:** sanitize OCI pull errors ([#3096](https://github.com/e2b-dev/infra/issues/3096)) ([a3af6c0](https://github.com/e2b-dev/infra/commit/a3af6c0ae71678a26b3ca6b2c77100849c4fb4a9))
* **orchestrator:** scope rootfs hash to provision default ([#3129](https://github.com/e2b-dev/infra/issues/3129)) ([475f955](https://github.com/e2b-dev/infra/commit/475f955cf3dfb87622c2e19a98b7e8ff558d257e))
* **orchestrator:** stop Checks health-loop leaking ([#2739](https://github.com/e2b-dev/infra/issues/2739)) ([17e6e60](https://github.com/e2b-dev/infra/commit/17e6e606e3737d6997c9fbe4863ab020460ce541))
* **orchestrator:** survive SIGBUS from failing disks under mmap'd caches ([#3385](https://github.com/e2b-dev/infra/issues/3385)) ([728bba3](https://github.com/e2b-dev/infra/commit/728bba3457d8a1ee41d5b5f4b02aa938a1831808))
* **orchestrator:** tolerate missing header for legacy templates ([#3026](https://github.com/e2b-dev/infra/issues/3026)) ([8a44bfe](https://github.com/e2b-dev/infra/commit/8a44bfe1a4984e1f0a30627c1694591a4c3bd4d5))
* **orch:** fall back to ID_LIKE with a warning instead of rejecting ([#3459](https://github.com/e2b-dev/infra/issues/3459)) ([7167818](https://github.com/e2b-dev/infra/commit/7167818772f497a02e2e183bc0437b4eb5b25b6e))
* **orch:** prevent NBD dispatch read-loop stall on WRITE_ZEROES (behind flag) ([#3048](https://github.com/e2b-dev/infra/issues/3048)) ([efd3d4d](https://github.com/e2b-dev/infra/commit/efd3d4ddb5540fb2dc867a3f6a341b29b9d68289))
* **orch:** split scheduling base build id per artifact ([#2920](https://github.com/e2b-dev/infra/issues/2920)) ([3e35a2a](https://github.com/e2b-dev/infra/commit/3e35a2ab81f28f71af9d3afb8a889fe6a9f6c566))
* **orch:** validate copy-build -gdb buckets before the snapshot copy ([#3446](https://github.com/e2b-dev/infra/issues/3446)) ([586ad74](https://github.com/e2b-dev/infra/commit/586ad7428874bfd42756b0530c04f79ad4166226))
* **shared:** never report a failed envd command stream as success ([#3281](https://github.com/e2b-dev/infra/issues/3281)) ([69c06b6](https://github.com/e2b-dev/infra/commit/69c06b6f95fac2b007c784d96309bc97d3a33c39))
* **storage:** compression upload & cache correctness fixes ([#3231](https://github.com/e2b-dev/infra/issues/3231)) ([980748f](https://github.com/e2b-dev/infra/commit/980748fb1549fc7a1cf17e6bea616417d5089009))
* **storage:** don't assume V4+ ancestor gaps are uncompressed ([#3447](https://github.com/e2b-dev/infra/issues/3447)) ([bfdbb24](https://github.com/e2b-dev/infra/commit/bfdbb24130ede13b449331acab40d39981f9b329))
* **uffd:** dedupe deferred page faults ([#2864](https://github.com/e2b-dev/infra/issues/2864)) ([9680a41](https://github.com/e2b-dev/infra/commit/9680a414ad02ff83e348240b2c844a4270397602))
* WrapContextAsUserError should not misclassify internal timeouts as user cancellations ([#3155](https://github.com/e2b-dev/infra/issues/3155)) ([8f83959](https://github.com/e2b-dev/infra/commit/8f83959e3a05e9c4b8486c517229b1e7158b1dfd))


### Performance Improvements

* **build:** cache resolved Diff per BuildId within File.ReadAt ([#2838](https://github.com/e2b-dev/infra/issues/2838)) ([53de07f](https://github.com/e2b-dev/infra/commit/53de07f15d082c59134a70fdacb54db768176413))
* **build:** parallelize fragmented backing reads ([#2872](https://github.com/e2b-dev/infra/issues/2872)) ([c7655a7](https://github.com/e2b-dev/infra/commit/c7655a7cd4777a299426ad89fc53a9267f800801))
* **clean-nfs-cache:** restore dirfd-relative statx ([#2766](https://github.com/e2b-dev/infra/issues/2766)) ([6bdbedb](https://github.com/e2b-dev/infra/commit/6bdbedb9d744e6476ceb41bf34b25b5ddafa1a10))
* **header:** add V5 columnar varint header format ([#2847](https://github.com/e2b-dev/infra/issues/2847)) ([9dd931b](https://github.com/e2b-dev/infra/commit/9dd931b8dec8b23c99c842d2cbf73460fb2c0aa3))
* **header:** pack cached Header.Mapping into a compact form ([#2844](https://github.com/e2b-dev/infra/issues/2844)) ([7f0b13c](https://github.com/e2b-dev/infra/commit/7f0b13c8ac5c83ac15b67d262c98320b3dac22b6))
* **orchestrator:** add memfile dedup density threshold ([#2862](https://github.com/e2b-dev/infra/issues/2862)) ([7ccfa02](https://github.com/e2b-dev/infra/commit/7ccfa028cd5d5ad43ceacec05753ecaa650087db))
* **orchestrator:** avoid V3-ancestor header refresh ([#2999](https://github.com/e2b-dev/infra/issues/2999)) ([cb6aa0b](https://github.com/e2b-dev/infra/commit/cb6aa0bbe31655dbfd8b90897c08f91adf170a9c))
* **orch:** metrics for dirty page throttling ([#2858](https://github.com/e2b-dev/infra/issues/2858)) ([d2aa554](https://github.com/e2b-dev/infra/commit/d2aa554af6c8243241c2b50045cc113111e12a76))
