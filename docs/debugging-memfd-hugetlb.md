# Reproducing memfd observer reservation failures

With hugetlb-backed guest memory, snapshot restore can fail before the guest runs:

```text
failed to wrap memfd: mmap memfd: cannot allocate memory
```

Firecracker maps its guest memfd with `MAP_SHARED | MAP_NORESERVE`. A second,
read-only mapping without `MAP_NORESERVE` establishes reservations for the
unreserved part of the entire file. This can exhaust the hugepage pool even
though the observer has not read any pages and ordinary RAM remains available.
`block.NewFromFd` must preserve the owner's no-reservation policy.

## Deterministic regression

Run from the repository root on Linux with the toolchain required by `go.work`:

```sh
go test -v -count=1 ./packages/orchestrator/pkg/sandbox/block \
  -run '^TestNewFromFd_HugetlbNoReservation$'
```

The test requires a supported 1 GiB hugepage size whose `nr_hugepages` and
`nr_overcommit_hugepages` are both zero. It also requires permission to create a
hugetlb memfd (root, `CAP_IPC_LOCK`, or the configured hugetlb group). It reports
a skip when those prerequisites are unavailable; a skipped run is not a
successful reproduction.

The fixture creates a sparse 1 GiB memfd and an owner mapping with
`MAP_NORESERVE`. It first verifies that a reserving observer fails with
`ENOMEM`, then calls `NewFromFd` on a duplicate descriptor. Without the fix,
that call fails with the same error. With the fix, it succeeds and both mappings
are closed normally.

No pages are accessed, no physical huge pages are consumed, and no host sysctl
is changed. Do not populate or read these deliberately unbacked mappings: doing
so can raise `SIGBUS`.

Existing memfd data/export tests and the ENOMEM classification test can be run
together with the regression:

```sh
go test -race -v -count=1 ./packages/orchestrator/pkg/sandbox/block \
  -run 'TestNewFromFd|TestNewCacheFromMemfd|TestDedupedMemfdCache|TestMemfdIdentitySource'
```

This change avoids an observer-induced reservation; it does not make physical
memory unlimited or guarantee that future guest faults can succeed under actual
hugepage exhaustion. Capacity planning and admission checks remain necessary.
See the [Linux hugetlb reservation documentation](https://docs.kernel.org/mm/hugetlbfs_reserv.html).
