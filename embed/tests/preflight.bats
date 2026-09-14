#!/usr/bin/env bats

# preflight.sh is sourced with PF_NO_MAIN=1 so each check function can be
# called on its own with the paths and commands it reads overridden.
#
# shellcheck disable=SC2030,SC2031  # bats runs each @test body in its own
# shell, and FAILURES is reset, appended to and read there; shellcheck reads
# the @test block as a subshell whose changes are lost, which they are not.

setup() {
  export PF_NO_MAIN=1
  # shellcheck disable=SC1091
  source "$BATS_TEST_DIRNAME/../compose/scripts/preflight.sh"
  FAILURES=()
}

@test "check_arch rejects an unsupported host with a FIX line naming it" {
  PF_ARCH=mips check_arch
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"FIX: only x86_64 and aarch64 hosts are supported"* ]]
  [[ "${FAILURES[0]}" == *"no mips build"* ]]
}

@test "check_arch accepts x86_64 and aarch64" {
  PF_ARCH=x86_64 check_arch
  PF_ARCH=aarch64 check_arch
  [ "${#FAILURES[@]}" -eq 0 ]
}

@test "check_page_size accepts 4096 and rejects a 64 KiB-page kernel with a FIX line" {
  PF_PAGE_SIZE=4096 check_page_size
  [ "${#FAILURES[@]}" -eq 0 ]
  PF_PAGE_SIZE=65536 check_page_size
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"65536"* ]]
  [[ "${FAILURES[0]}" == *"FIX: boot a 4 KiB-page kernel"* ]]
}

# One FIX per architecture: the x86 remedy names firmware switches and modules
# that do not exist on arm64, and both must keep the phrase the guides quote.
@test "check_kvm's FIX line is written for the host's architecture" {
  PF_DEV="$BATS_TEST_TMPDIR/dev"; mkdir -p "$PF_DEV"
  PF_ARCH=x86_64 check_kvm
  [[ "${FAILURES[0]}" == *"kvm_intel or kvm_amd"* ]]
  [[ "${FAILURES[0]}" == *"nested virtualization"* ]]
  FAILURES=()
  PF_ARCH=aarch64 check_kvm
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"EL2"* ]]
  [[ "${FAILURES[0]}" == *"nested virtualization"* ]]
  [[ "${FAILURES[0]}" != *"kvm_intel"* ]]
}

@test "check_kernel accepts 6.8.0-45-generic" {
  PF_UNAME_R=6.8.0-45-generic check_kernel
  [ "${#FAILURES[@]}" -eq 0 ]
}

@test "check_kernel rejects 6.6.12" {
  PF_UNAME_R=6.6.12 check_kernel
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"6.8"* ]]
}

@test "check_kvm fails when /dev/kvm is missing" {
  PF_DEV="$BATS_TEST_TMPDIR/dev"; mkdir -p "$PF_DEV"
  check_kvm
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"nested virtualization"* ]]
}

@test "check_tun fails when /dev/net/tun is missing" {
  PF_DEV="$BATS_TEST_TMPDIR/dev"; mkdir -p "$PF_DEV/net"
  check_tun
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"tun"* ]]
}

@test "check_cgroup2 fails when cgroup.controllers is missing" {
  PF_SYS="$BATS_TEST_TMPDIR/sys"; mkdir -p "$PF_SYS/fs/cgroup"
  check_cgroup2
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"cgroup"* ]]
}

@test "check_glibc rejects 2.31 and accepts 2.39" {
  PF_GLIBC=2.31 check_glibc
  [ "${#FAILURES[@]}" -eq 1 ]
  FAILURES=()
  PF_GLIBC=2.39 check_glibc
  [ "${#FAILURES[@]}" -eq 0 ]
}

@test "PF_GLIBC parses a single token even when ldd's output exceeds the pipe buffer" {
  bindir="$BATS_TEST_TMPDIR/bin"; mkdir -p "$bindir"
  cat > "$bindir/ldd" <<'FAKE_LDD'
#!/usr/bin/env bash
printf 'ldd (Ubuntu GLIBC 2.39-0ubuntu8.4) 2.39\n'
# ~20000 extra lines: comfortably bigger than any OS pipe buffer, so a
# `head -n1` consumer on the real ldd --version pipeline would close its
# read end before this finishes writing, sending ldd a SIGPIPE.
yes 'padding padding padding padding padding padding padding padding padding padding' | head -n 20000
FAKE_LDD
  chmod +x "$bindir/ldd"
  d="$BATS_TEST_TMPDIR/root"; mkdir -p "$d/dev/net" "$d/sys/fs/cgroup"
  : > "$d/sys/fs/cgroup/cgroup.controllers"
  # PF_GLIBC is deliberately NOT preset: the script must derive it itself,
  # under set -o pipefail, from the fake ldd on PATH.
  run env -u PF_NO_MAIN PATH="$bindir:$PATH" PF_ARCH=x86_64 PF_PAGE_SIZE=4096 PF_UNAME_R=6.8.0-45-generic PF_FREE_GIB=50 \
      PF_SYS="$d/sys" PF_MODPROBE=true PF_SKIP_DEVICES=1 PF_TOOLS=bash bash "$BATS_TEST_DIRNAME/../compose/scripts/preflight.sh"
  [ "$status" -eq 0 ]
  [[ "$output" == *"glibc=2.39"* ]]
}

@test "check_nbd fails when the kernel has no nbd module" {
  PF_MODPROBE=false check_nbd
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"nbd"* ]]
  [[ "${FAILURES[0]}" == *"FIX:"* ]]
}

@test "check_disk honours PF_MIN_FREE_GIB" {
  PF_FREE_GIB=15 PF_MIN_FREE_GIB=20 check_disk
  [ "${#FAILURES[@]}" -eq 1 ]
  FAILURES=()
  PF_FREE_GIB=15 PF_MIN_FREE_GIB=10 check_disk
  [ "${#FAILURES[@]}" -eq 0 ]
}

@test "check_tools passes when every required binary is on PATH" {
  PF_TOOLS="bash sh" check_tools
  [ "${#FAILURES[@]}" -eq 0 ]
}

@test "check_tools fails with a FIX line naming the missing binaries" {
  PF_TOOLS="bash definitely-not-installed-xyz" check_tools
  [ "${#FAILURES[@]}" -eq 1 ]
  [[ "${FAILURES[0]}" == *"definitely-not-installed-xyz"* ]]
  [[ "${FAILURES[0]}" == *"FIX: apt-get install iptables rsync e2fsprogs iproute2"* ]]
}

@test "main exits 1 and prints a FIX line when a check fails" {
  d="$BATS_TEST_TMPDIR/root"; mkdir -p "$d/dev/net" "$d/sys/fs/cgroup"
  run env -u PF_NO_MAIN PF_ARCH=mips PF_PAGE_SIZE=4096 PF_UNAME_R=6.8.0 PF_GLIBC=2.39 PF_FREE_GIB=50 \
      PF_DEV="$d/dev" PF_SYS="$d/sys" PF_MODPROBE=true PF_TOOLS=bash bash "$BATS_TEST_DIRNAME/../compose/scripts/preflight.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"FIX:"* ]]
}

@test "main exits 0 when every check passes" {
  d="$BATS_TEST_TMPDIR/root"; mkdir -p "$d/dev/net" "$d/sys/fs/cgroup"
  : > "$d/sys/fs/cgroup/cgroup.controllers"
  # character devices cannot be created without root; PF_SKIP_DEVICES bypasses the two device checks
  run env -u PF_NO_MAIN PF_ARCH=x86_64 PF_PAGE_SIZE=4096 PF_UNAME_R=6.8.0-45-generic PF_GLIBC=2.39 PF_FREE_GIB=50 \
      PF_SYS="$d/sys" PF_MODPROBE=true PF_SKIP_DEVICES=1 PF_TOOLS=bash bash "$BATS_TEST_DIRNAME/../compose/scripts/preflight.sh"
  [ "$status" -eq 0 ]
  [[ "$output" == *"preflight: ok (arch=x86_64 page=4096"* ]]
}

@test "main passes on an aarch64 host with a 4 KiB-page kernel" {
  d="$BATS_TEST_TMPDIR/root"; mkdir -p "$d/dev/net" "$d/sys/fs/cgroup"
  : > "$d/sys/fs/cgroup/cgroup.controllers"
  run env -u PF_NO_MAIN PF_ARCH=aarch64 PF_PAGE_SIZE=4096 PF_UNAME_R=6.8.0-45-generic PF_GLIBC=2.39 PF_FREE_GIB=50 \
      PF_SYS="$d/sys" PF_MODPROBE=true PF_SKIP_DEVICES=1 PF_TOOLS=bash bash "$BATS_TEST_DIRNAME/../compose/scripts/preflight.sh"
  [ "$status" -eq 0 ]
  [[ "$output" == *"preflight: ok (arch=aarch64 page=4096"* ]]
}

@test "main fails on an aarch64 host with a 64 KiB-page kernel" {
  d="$BATS_TEST_TMPDIR/root"; mkdir -p "$d/dev/net" "$d/sys/fs/cgroup"
  : > "$d/sys/fs/cgroup/cgroup.controllers"
  run env -u PF_NO_MAIN PF_ARCH=aarch64 PF_PAGE_SIZE=65536 PF_UNAME_R=6.8.0-45-generic PF_GLIBC=2.39 PF_FREE_GIB=50 \
      PF_SYS="$d/sys" PF_MODPROBE=true PF_SKIP_DEVICES=1 PF_TOOLS=bash bash "$BATS_TEST_DIRNAME/../compose/scripts/preflight.sh"
  [ "$status" -eq 1 ]
  [[ "$output" == *"page size is 65536"* ]]
  [[ "$output" == *"FIX: boot a 4 KiB-page kernel"* ]]
}
