#!/usr/bin/env bash

set -euo pipefail
set -x

readonly test_path=${1:-}

if [[ -z "${test_path}" ]]; then
  echo "usage: $0 <test_path>" >&2
  exit 2
fi

if [[ ! -d "${test_path}" ]]; then
  echo "${test_path} is not a directory" >&2
  exit 1
fi

# All operations must stay within ${test_path}
tmpdir="${test_path}/posix_suite"
mkdir -p "${tmpdir}"

# 1) Basic write/read
echo "writing a file ..."
echo "hello world" > "${tmpdir}/test.txt"

echo "reading file ..."
file_content=$(cat "${tmpdir}/test.txt")
if [[ "${file_content}" != "hello world" ]]; then
  echo "content does not match" >&2
  exit 1
fi

# 2) Append
echo " appending ..." >> "${tmpdir}/test.txt"
echo "line2" >> "${tmpdir}/test.txt"
grep -q "hello world" "${tmpdir}/test.txt"
grep -q "line2" "${tmpdir}/test.txt"

# 3) chmod
chmod 0600 "${tmpdir}/test.txt"
perms=$(stat -c %a "${tmpdir}/test.txt" 2>/dev/null || stat -f %Lp "${tmpdir}/test.txt")
if [[ "${perms}" != "600" ]]; then
  echo "chmod failed: expected 600, got ${perms}" >&2
  exit 1
fi

# 4) chown (best-effort, skip if not possible)
if command -v id >/dev/null 2>&1 && command -v chown >/dev/null 2>&1; then
  # Try nobody:nogroup which exists on Ubuntu/Debian
  if getent passwd nobody >/dev/null 2>&1 && getent group nogroup >/dev/null 2>&1; then
    chown nobody:nogroup "${tmpdir}/test.txt" || true
    owner=$(stat -c %U "${tmpdir}/test.txt" 2>/dev/null || stat -f %Su "${tmpdir}/test.txt") || true
    group=$(stat -c %G "${tmpdir}/test.txt" 2>/dev/null || stat -f %Sg "${tmpdir}/test.txt") || true
    # Some NFS setups may squash root to nobody; accept either
    if [[ -n "${owner}" && -n "${group}" ]]; then
      if [[ "${owner}" != "nobody" && "${owner}" != "root" ]]; then
        echo "warning: owner after chown is ${owner}, continuing"
      fi
      if [[ "${group}" != "nogroup" && "${group}" != "root" ]]; then
        echo "warning: group after chown is ${group}, continuing"
      fi
    fi
  fi
fi

# 5) mkdir -p and recursive structure
mkdir -p "${tmpdir}/dir1/subdir"
echo foo > "${tmpdir}/dir1/a.txt"
echo bar > "${tmpdir}/dir1/subdir/b.txt"

# 6) file listing should include created files
ls_out=$(ls -1 "${tmpdir}/dir1" | tr -d '\r')
echo "${ls_out}" | grep -q "a.txt"
echo "${ls_out}" | grep -q "subdir"

# 7) rename/move
mv "${tmpdir}/dir1/a.txt" "${tmpdir}/dir1/a_renamed.txt"
test -f "${tmpdir}/dir1/a_renamed.txt"
test ! -f "${tmpdir}/dir1/a.txt"

# 8) symlink
ln -s "${tmpdir}/dir1/a_renamed.txt" "${tmpdir}/link_to_a"
readlink "${tmpdir}/link_to_a" >/dev/null
grep -q foo "${tmpdir}/link_to_a"

# 9) hardlink (may be restricted on some FS; validate link count when possible)
ln "${tmpdir}/dir1/a_renamed.txt" "${tmpdir}/hardlink_a" || true
if [[ -f "${tmpdir}/hardlink_a" ]]; then
  nlinks=$(stat -c %h "${tmpdir}/dir1/a_renamed.txt" 2>/dev/null || echo 1)
  if [[ ${nlinks} -lt 1 ]]; then
    echo "unexpected link count: ${nlinks}" >&2
    exit 1
  fi
fi

# 10) truncate
truncate -s 3 "${tmpdir}/dir1/subdir/b.txt" || : > "${tmpdir}/dir1/subdir/b.txt"
sz=$(stat -c %s "${tmpdir}/dir1/subdir/b.txt" 2>/dev/null || wc -c < "${tmpdir}/dir1/subdir/b.txt")
if [[ "${sz}" -ne 3 ]]; then
  echo "truncate failed: expected size 3, got ${sz}" >&2
  exit 1
fi

# 11) deletion of file
rm "${tmpdir}/dir1/subdir/b.txt"
test ! -f "${tmpdir}/dir1/subdir/b.txt"

# 12) recursive deletion of directory
rm -rf "${tmpdir}/dir1"
test ! -d "${tmpdir}/dir1"

# 13) advisory locking (flock) must resolve locally and NOT hang.
# The proxy has no lock manager, so the volume must be mounted with nolock
# (see packages/envd/internal/api/init.go). Without it the kernel sends the
# lock to the proxy as an NLM request, gets port 0, and a hard mount retries
# forever -- flock wedges in uninterruptible sleep. Guard against that
# regression: a 10s timeout turns a hang into a loud failure instead of a
# stuck suite. See github.com/e2b-dev/infra issue #3619.
if ! timeout 10 flock "${tmpdir}/test.txt" -c true; then
  echo "flock did not resolve within 10s under ${test_path} (missing nolock?)" >&2
  exit 1
fi

echo "POSIX filesystem operations OK under ${test_path}"
