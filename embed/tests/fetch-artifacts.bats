#!/usr/bin/env bats

# shellcheck disable=SC2034  # SHA256 is the sourced script's checksum table
# shellcheck disable=SC2030,SC2031  # bats runs each @test body in its own
# shell, so the SHA256 entries a test overrides are read back in that same
# shell; shellcheck reads an @test block as a subshell whose changes are lost.

# The bucket is a file:// tree and HOST_ROOT a tmpdir, so the download and
# verification logic runs unprivileged and offline.
setup() {
  export FA_NO_MAIN=1
  export E2B_ORCHESTRATOR_VERSION=v0.11.0 E2B_ENVD_VERSION=v0.7.0 E2B_KERNEL_VERSION=vmlinux-6.1.177_5008931 \
         E2B_FIRECRACKER_VERSION=v1.14-0.2.0 E2B_BUSYBOX_VERSION=1.36.1
  # shellcheck disable=SC1091
  source "$BATS_TEST_DIRNAME/../compose/scripts/fetch-artifacts.sh"
  export BUCKET="file://$BATS_TEST_TMPDIR/bucket"
  export HOST_ROOT="$BATS_TEST_TMPDIR/host"
  mkdir -p "$BATS_TEST_TMPDIR/bucket/t" "$HOST_ROOT"
  printf 'hello\n' > "$BATS_TEST_TMPDIR/bucket/t/obj"
  SHA256["t/obj"]="$(sha256_of "$BATS_TEST_TMPDIR/bucket/t/obj")"
}

# Reads one pin out of the committed .env the way an operator's shell would:
# the value up to the first space, which drops the release marker comment the
# platform-versioned lines carry.
env_pin() { sed -n "s/^$1=\([^ #]*\).*/\1/p" "$BATS_TEST_DIRNAME/../compose/.env" | tr -d '\r'; }

# Octal mode of a file. `stat` takes different flags in its GNU and BSD builds
# and these tests run on both (Linux CI, a macOS workstation), so the GNU form
# is tried first and the BSD one is the fallback.
mode_of() { stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1"; }

@test "goarch maps x86_64 and amd64 to amd64" {
  [ "$(goarch x86_64)" = amd64 ]
  [ "$(goarch amd64)" = amd64 ]
}

@test "goarch maps aarch64 to arm64 and rejects unknown" {
  [ "$(goarch aarch64)" = arm64 ]
  run goarch mips
  [ "$status" -ne 0 ]
}

@test "fetch downloads, verifies and sets the mode" {
  fetch "t/obj" "$HOST_ROOT/x/obj" 0755
  [ -x "$HOST_ROOT/x/obj" ]
  [ "$(cat "$HOST_ROOT/x/obj")" = hello ]
}

@test "fetch skips a file whose checksum already matches" {
  fetch "t/obj" "$HOST_ROOT/x/obj" 0644
  run fetch "t/obj" "$HOST_ROOT/x/obj" 0644
  [ "$status" -eq 0 ]
  [[ "$output" == *"already present"* ]]
}

# A cache hit has to leave the mode the caller asked for, not just the right
# bytes: an artifact that landed non-executable (a run interrupted between mv
# and chmod, a hand-copied file, a restored backup) would otherwise stay that
# way for good, because the checksum test short-circuits every later run.
@test "fetch re-applies the mode on a cache hit, without downloading" {
  local stub="$BATS_TEST_TMPDIR/stub"
  mkdir -p "$stub" "$HOST_ROOT/x"
  export CURL_LOG="$BATS_TEST_TMPDIR/curl-called"
  cat > "$stub/curl" <<'EOF2'
#!/usr/bin/env bash
echo called >> "$CURL_LOG"
exit 99
EOF2
  chmod +x "$stub/curl"
  cp "$BATS_TEST_TMPDIR/bucket/t/obj" "$HOST_ROOT/x/obj"
  chmod 0644 "$HOST_ROOT/x/obj"
  PATH="$stub:$PATH" run fetch "t/obj" "$HOST_ROOT/x/obj" 0755
  [ "$status" -eq 0 ]
  [[ "$output" == *"already present"* ]]
  [ "$(mode_of "$HOST_ROOT/x/obj")" = 755 ]
  [ ! -e "$CURL_LOG" ]
}

@test "fetch fails with FIX on a checksum mismatch and leaves no file" {
  SHA256["t/obj"]="0000000000000000000000000000000000000000000000000000000000000000"
  run fetch "t/obj" "$HOST_ROOT/y/obj" 0644
  [ "$status" -ne 0 ]
  [[ "$output" == *"FIX:"* ]]
  [ ! -e "$HOST_ROOT/y/obj" ]
  [ ! -e "$HOST_ROOT/y/obj.tmp" ]
}

@test "fetch fails with FIX when no checksum is pinned and no .sha256 is published" {
  run fetch "t/unknown" "$HOST_ROOT/z/obj" 0644
  [ "$status" -ne 0 ]
  [[ "$output" == *"FIX:"* ]]
  [[ "$output" == *"t/unknown.sha256"* ]]
}

# The release workflow writes <key>.sha256 beside every binary it publishes.
# That is the checksum for a version the table cannot know: the orchestrator
# pin moves with the platform release, and the tools image is built before
# the binary it would have to pin.
@test "fetch verifies against the published .sha256 when the table has no row" {
  printf 'released\n' > "$BATS_TEST_TMPDIR/bucket/t/side"
  (cd "$BATS_TEST_TMPDIR/bucket/t" && sha256sum side > side.sha256)
  fetch "t/side" "$HOST_ROOT/s/side" 0755
  [ -x "$HOST_ROOT/s/side" ]
  [ "$(cat "$HOST_ROOT/s/side")" = released ]
}

@test "a pinned row wins over a published .sha256" {
  printf 'released\n' > "$BATS_TEST_TMPDIR/bucket/t/side"
  printf '%s  side\n' "0000000000000000000000000000000000000000000000000000000000000000" > "$BATS_TEST_TMPDIR/bucket/t/side.sha256"
  SHA256["t/side"]="$(sha256_of "$BATS_TEST_TMPDIR/bucket/t/side")"
  fetch "t/side" "$HOST_ROOT/s/side" 0644
  [ "$(cat "$HOST_ROOT/s/side")" = released ]
}

@test "a .sha256 that is not a sha256 is refused with FIX" {
  printf 'released\n' > "$BATS_TEST_TMPDIR/bucket/t/side"
  printf 'not a checksum\n' > "$BATS_TEST_TMPDIR/bucket/t/side.sha256"
  run fetch "t/side" "$HOST_ROOT/s/side" 0644
  [ "$status" -ne 0 ]
  [[ "$output" == *"FIX:"* ]]
  [ ! -e "$HOST_ROOT/s/side" ]
}

@test "a binary that disagrees with its published .sha256 fails and leaves no file" {
  printf 'released\n' > "$BATS_TEST_TMPDIR/bucket/t/side"
  printf '%s  side\n' "0000000000000000000000000000000000000000000000000000000000000000" > "$BATS_TEST_TMPDIR/bucket/t/side.sha256"
  run fetch "t/side" "$HOST_ROOT/s/side" 0644
  [ "$status" -ne 0 ]
  [[ "$output" == *"FIX:"* ]]
  [ ! -e "$HOST_ROOT/s/side" ]
  [ ! -e "$HOST_ROOT/s/side.tmp" ]
}

@test "fetch fails with FIX when the download itself fails" {
  export BUCKET="file://$BATS_TEST_TMPDIR/nonexistent-bucket"
  run fetch "t/obj" "$HOST_ROOT/w/obj" 0644
  [ "$status" -ne 0 ]
  [[ "$output" == *"FIX:"* ]]
  [ ! -e "$HOST_ROOT/w/obj" ]
  [ ! -e "$HOST_ROOT/w/obj.tmp" ]
}

# curl never creates the output file for a missing file:// source, so the test
# above cannot see the partial-cleanup contract. A dropped connection can, and
# that is the case the `rm -f "$dest.tmp"` exists for: a stub curl writes some
# bytes and exits 18 (CURLE_PARTIAL_FILE).
@test "fetch removes the partial when a transfer breaks off part-way" {
  local stub="$BATS_TEST_TMPDIR/stub"
  mkdir -p "$stub"
  cat > "$stub/curl" <<'EOF'
#!/usr/bin/env bash
dest=""
while [ "$#" -gt 0 ]; do
  case "$1" in -o) dest="$2"; shift 2 ;; *) shift ;; esac
done
printf 'partial' > "$dest"
exit 18
EOF
  chmod +x "$stub/curl"
  PATH="$stub:$PATH" run fetch "t/obj" "$HOST_ROOT/v/obj" 0644
  [ "$status" -ne 0 ]
  [[ "$output" == *"FIX:"* ]]
  [ ! -e "$HOST_ROOT/v/obj" ]
  [ ! -e "$HOST_ROOT/v/obj.tmp" ]
}

# The two halves of a hand-pinned version live in different artifacts: the
# version in .env on the host, the checksum table inside the tools image. A
# version bumped in one without the other is otherwise only caught at runtime,
# by which point host-setup has already mutated the host. The orchestrator is
# not hand-pinned: the release moves it and publishes the .sha256 it is
# verified against, so no row can exist for it ahead of time.
@test "every hand-pinned artifact version in .env has a checksum in the table" {
  local envd kernel fc bb key
  envd="$(env_pin E2B_ENVD_VERSION)"
  kernel="$(env_pin E2B_KERNEL_VERSION)"
  fc="$(env_pin E2B_FIRECRACKER_VERSION)"
  bb="$(env_pin E2B_BUSYBOX_VERSION)"
  [ -n "$envd" ] && [ -n "$kernel" ] && [ -n "$fc" ] && [ -n "$bb" ]
  # The three Firecracker artifacts are published for both architectures, so
  # both rows are required. The orchestrator is not hand-pinned: the release
  # moves it and publishes the .sha256 it is verified against.
  for key in "envd/$envd/envd" \
             "firecrackers/$fc/amd64/firecracker" \
             "kernels/$kernel/amd64/vmlinux.bin" \
             "busybox/$bb/amd64/busybox" \
             "firecrackers/$fc/arm64/firecracker" \
             "kernels/$kernel/arm64/vmlinux.bin" \
             "busybox/$bb/arm64/busybox"; do
    if [ -z "${SHA256[$key]:-}" ]; then
      echo "no sha256 pinned for $key (composed from .env)"
      return 1
    fi
  done
}

# Lays out a fake bucket holding every object main() fetches for one
# architecture, pins their checksums, and returns the keys' arch segment.
# amd64 keeps the bare orchestrator and envd keys every release has published;
# arm64 gets an arm64/ segment, the layout the Firecracker artifacts use.
stage_bucket() {
  local ga="$1" seg="" key
  [ "$ga" = amd64 ] || seg="$ga/"
  for key in "orchestrator/$E2B_ORCHESTRATOR_VERSION/${seg}orchestrator" \
             "envd/$E2B_ENVD_VERSION/${seg}envd" \
             "firecrackers/$E2B_FIRECRACKER_VERSION/$ga/firecracker" \
             "kernels/$E2B_KERNEL_VERSION/$ga/vmlinux.bin" \
             "busybox/$E2B_BUSYBOX_VERSION/$ga/busybox"; do
    mkdir -p "$(dirname "$BATS_TEST_TMPDIR/bucket/$key")"
    printf '%s\n' "$key" > "$BATS_TEST_TMPDIR/bucket/$key"
    SHA256["$key"]="$(sha256_of "$BATS_TEST_TMPDIR/bucket/$key")"
  done
}

# The release writes <key>.sha256 as a bare 64-hex line, which is what
# sidecar_sha256 accepts.
write_sidecar() {
  local key="$1"
  sha256_of "$BATS_TEST_TMPDIR/bucket/$key" > "$BATS_TEST_TMPDIR/bucket/$key.sha256"
}

@test "main on x86_64 fetches the bare orchestrator and envd keys and the amd64 Firecracker artifacts" {
  stage_bucket amd64
  FA_ARCH=x86_64 run main
  [ "$status" -eq 0 ]
  [[ "$output" == *"downloading orchestrator/$E2B_ORCHESTRATOR_VERSION/orchestrator"* ]]
  [[ "$output" == *"downloading envd/$E2B_ENVD_VERSION/envd"* ]]
  [[ "$output" == *"fetch-artifacts: ok"* ]]
  [ "$(cat "$HOST_ROOT/var/lib/e2b/bin/orchestrator")" = "orchestrator/$E2B_ORCHESTRATOR_VERSION/orchestrator" ]
  [ "$(cat "$HOST_ROOT/fc-envd/envd")" = "envd/$E2B_ENVD_VERSION/envd" ]
  [ -f "$HOST_ROOT/fc-versions/$E2B_FIRECRACKER_VERSION/amd64/firecracker" ]
  [ -f "$HOST_ROOT/fc-kernels/$E2B_KERNEL_VERSION/amd64/vmlinux.bin" ]
  [ -f "$HOST_ROOT/fc-busybox/$E2B_BUSYBOX_VERSION/amd64/busybox" ]
}

# The host paths are the same on both architectures — the orchestrator reads
# envd from /fc-envd/envd — only the bucket keys differ.
@test "main on aarch64 fetches the arm64-keyed orchestrator and envd once their rows are pinned" {
  stage_bucket arm64
  FA_ARCH=aarch64 run main
  [ "$status" -eq 0 ]
  [[ "$output" == *"downloading orchestrator/$E2B_ORCHESTRATOR_VERSION/arm64/orchestrator"* ]]
  [[ "$output" == *"downloading envd/$E2B_ENVD_VERSION/arm64/envd"* ]]
  [[ "$output" == *"fetch-artifacts: ok"* ]]
  [ "$(cat "$HOST_ROOT/var/lib/e2b/bin/orchestrator")" = "orchestrator/$E2B_ORCHESTRATOR_VERSION/arm64/orchestrator" ]
  [ "$(cat "$HOST_ROOT/fc-envd/envd")" = "envd/$E2B_ENVD_VERSION/arm64/envd" ]
  [ -f "$HOST_ROOT/fc-versions/$E2B_FIRECRACKER_VERSION/arm64/firecracker" ]
  [ -f "$HOST_ROOT/fc-kernels/$E2B_KERNEL_VERSION/arm64/vmlinux.bin" ]
  [ -f "$HOST_ROOT/fc-busybox/$E2B_BUSYBOX_VERSION/arm64/busybox" ]
}

# After the first arm64 publish, the table has no row for those versions;
# fetch() has to take the sidecar or an aarch64 host can never install them.
@test "main on aarch64 fetches the arm64 orchestrator via its published .sha256 when the table has no row" {
  stage_bucket arm64
  unset 'SHA256[orchestrator/'"$E2B_ORCHESTRATOR_VERSION"'/arm64/orchestrator]'
  write_sidecar "orchestrator/$E2B_ORCHESTRATOR_VERSION/arm64/orchestrator"
  FA_ARCH=aarch64 run main
  [ "$status" -eq 0 ]
  [[ "$output" == *"downloading orchestrator/$E2B_ORCHESTRATOR_VERSION/arm64/orchestrator"* ]]
  [[ "$output" == *"fetch-artifacts: ok"* ]]
  [ "$(cat "$HOST_ROOT/var/lib/e2b/bin/orchestrator")" = "orchestrator/$E2B_ORCHESTRATOR_VERSION/arm64/orchestrator" ]
}

@test "main on aarch64 fetches the arm64 envd via its published .sha256 when the table has no row" {
  stage_bucket arm64
  unset 'SHA256[envd/'"$E2B_ENVD_VERSION"'/arm64/envd]'
  write_sidecar "envd/$E2B_ENVD_VERSION/arm64/envd"
  FA_ARCH=aarch64 run main
  [ "$status" -eq 0 ]
  [[ "$output" == *"downloading envd/$E2B_ENVD_VERSION/arm64/envd"* ]]
  [[ "$output" == *"fetch-artifacts: ok"* ]]
  [ "$(cat "$HOST_ROOT/fc-envd/envd")" = "envd/$E2B_ENVD_VERSION/arm64/envd" ]
}

@test "main on aarch64 stops with FIX when the arm64 orchestrator has no row and no sidecar" {
  stage_bucket arm64
  unset 'SHA256[orchestrator/'"$E2B_ORCHESTRATOR_VERSION"'/arm64/orchestrator]'
  FA_ARCH=aarch64 run main
  [ "$status" -eq 1 ]
  [[ "$output" == *"no sha256 pinned for orchestrator/$E2B_ORCHESTRATOR_VERSION/arm64/orchestrator"* ]]
  [[ "$output" == *"orchestrator/$E2B_ORCHESTRATOR_VERSION/arm64/orchestrator.sha256"* ]]
  [[ "$output" != *"no arm64 build of orchestrator is pinned"* ]]
  [[ "$output" != *"downloading"* ]]
  [ ! -e "$HOST_ROOT/var/lib/e2b/bin/orchestrator" ]
  [ ! -e "$HOST_ROOT/fc-envd/envd" ]
}

@test "main fails with FIX naming the variable when a required version is unset" {
  run env -u FA_NO_MAIN -u E2B_ORCHESTRATOR_VERSION HOST_ROOT="$HOST_ROOT" bash "$BATS_TEST_DIRNAME/../compose/scripts/fetch-artifacts.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"FIX:"* ]]
  [[ "$output" == *"E2B_ORCHESTRATOR_VERSION"* ]]
}

# The committed table has no arm64 orchestrator or envd row. An aarch64 host
# at the .env pins must go through the sidecar path, not the old in-image
# gate. An empty file:// bucket keeps the test offline.
@test "the shipped script on aarch64 at the .env pins looks for the published sidecar" {
  run env -u FA_NO_MAIN FA_ARCH=aarch64 HOST_ROOT="$HOST_ROOT" \
      BUCKET="file://$BATS_TEST_TMPDIR/empty-bucket" \
      E2B_ORCHESTRATOR_VERSION="$(env_pin E2B_ORCHESTRATOR_VERSION)" E2B_ENVD_VERSION="$(env_pin E2B_ENVD_VERSION)" \
      E2B_KERNEL_VERSION="$(env_pin E2B_KERNEL_VERSION)" E2B_FIRECRACKER_VERSION="$(env_pin E2B_FIRECRACKER_VERSION)" \
      E2B_BUSYBOX_VERSION="$(env_pin E2B_BUSYBOX_VERSION)" \
      bash "$BATS_TEST_DIRNAME/../compose/scripts/fetch-artifacts.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"no sha256 pinned for orchestrator/$(env_pin E2B_ORCHESTRATOR_VERSION)/arm64/orchestrator"* ]]
  [[ "$output" == *".sha256 beside it"* ]]
  [[ "$output" != *"no arm64 build of orchestrator is pinned"* ]]
  [[ "$output" != *"downloading"* ]]
}

@test "main fails with the exact FIX line for an unrecognized architecture" {
  run env -u FA_NO_MAIN FA_ARCH=mips HOST_ROOT="$HOST_ROOT" bash "$BATS_TEST_DIRNAME/../compose/scripts/fetch-artifacts.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"FIX: only x86_64 and aarch64 hosts are supported; the released orchestrator and envd have no mips build"* ]]
}
