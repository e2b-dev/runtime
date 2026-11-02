# Sandbox store key-form protocol, model-checked

`SandboxStore.tla` is a TLA+ model of the sandbox store's lock and ordering
protocol while per-sandbox keys move from the team-tagged key form ("old") to
the sandbox-tagged form ("new"). `check.sh` runs TLC on it. The model exists so
the protocol's safety claims are checked by a machine before the code that
implements them is written; the Go code is the authority on everything the
model leaves out.

## What is modelled

Two API pods act on two sandbox IDs of one team: `p1` runs release R1, whose
write form is the flag as it last read it; `p2` runs `P2Release`, R1 or R3.
The store holds, per form and sandbox, the record, the lock holder and the
transition marker with its TTL; shared, the team index, the global expiration
set and the flag. Add, StartRemoving, Remove, Update and the healer run a step
per command, so a crash or the other pod can land between any two. Each fault
has a budget of one per run: a pod crash, a lost or failed command reply, a
lost reply on Add's cleanup, a clock tick, a flag flip and a replication loss
(budget 0 by default, which excludes it). The spec's header and the comment
above each section (`Add`, `StartRemoving`, `Remove`, `Update / keepalive`,
`The healer`, `Faults and the environment`) carry the per-command detail.

## Invariants

| Operator | Meaning |
| --- | --- |
| `OneRecordPerSandbox` | A sandbox has at most one record, under one form. |
| `IndexHasRecord` | Every index member has a record, except in the states the protocol admits: after replication loss (not excused: enabling that action violates the invariant by design) and the Add window described below, transient or lasting. |
| `RecordIndexedOrTransient` | Every record is indexed, except inside Add's write-to-SADD (or write-to-cleanup) and Remove's SREM-to-delete windows, and after a crash or a failed SADD interrupted one of them, until a repair. |
| `LockExclusion` | A pod inside a locked section touches only records whose form lock it holds. Hence two pods never act on one sandbox's record under different lock keys. |
| `NoSuccessorDeleted` | A removal expecting execution `e`, or Add's cleanup of execution `e`, never deletes another execution's record. |
| `NoLiveMembershipRemoved` | No SREM, by a removal's retry or by the healer, drops a running sandbox's index member. Add's cleanup of its own failed commit is the one legitimate case. |
| `NoDeadReAdd` | No SADD indexes a sandbox whose record is not running. |
| `NoSuccessorReportedGone` | A removal never concludes its sandbox gone, the verdict on which the store releases the reservation, while another execution's record exists: a form found empty under its lock is checked against the other form before the lock is released, where a record is a completed resume's. |

`IndexHasRecordStrict` is the same as `IndexHasRecord` without the lasting
exemption; it is not in the configuration and exists to reproduce that state.

Two formulations differ from a literal reading of the protocol on purpose, and
the invariant comments in the spec hold the reasoning. `LockExclusion` checks
that nobody reads or writes a record under the other form's lock, not that
two pods never hold different lock keys for one sandbox, which two pods that
resolve "no record, write form by flag" with different flag views do.
`IndexHasRecord` excuses exactly the member with no record an Add leaves
between its DEL of the other form's record and the end of its cleanup, which
lasts only when that Add crashes before its write landed; the protocol admits
that state alongside replication loss.

## Running it

From this directory:

```sh
./check.sh
```

The script runs the TLC 2.19 `tla2tools.jar` that `TLA2TOOLS_JAR` names when
it is set (with a JRE on `PATH`), else `tlc` on `PATH`, else TLC through nix,
as its header says, writes TLC's state files to a temporary directory and
leaves nothing here. The exit status
is TLC's: 0 for `Model checking completed. No error has been found.`,
otherwise non-zero with the counterexample on stdout. TLC runs with
`-deadlock`, which turns its deadlock check off: every run of this model ends
in a state with no enabled action once the fault budgets are spent and the
pods are idle, and that end is not a bug.

The committed configuration (`SandboxStore.cfg`) explores about 53.6 million
distinct states (173.0 million generated), 3 min 46 s on an 18-core laptop
with TLC's default heap. `NAME=VALUE` arguments override its constants and
`INVARIANTS="A B"` replaces its invariant list, for example:

```sh
./check.sh P2Release='"R1"'                     # both pods on R1
./check.sh MaxCrashes=2                         # two crashes, every invariant
INVARIANTS="TypeOK OneRecordPerSandbox RecordIndexedOrTransient LockExclusion \
  NoSuccessorDeleted NoLiveMembershipRemoved NoDeadReAdd" \
  ./check.sh MaxReplicationLosses=1             # a failover loses one record
```

All three pass; the second and third explore several times more states than
the default.

## Mutations and reproductions

Nine constants, `FALSE` in the committed configuration, each break the
protocol in one way. TLC must report the violation each row names, two of them
only in the combination the row gives; the committed form must pass.

| Run | Expected result |
| --- | --- |
| `./check.sh RemoveDeletesBeforeSrem=TRUE` | `IndexHasRecord` violated: the new-form Remove deletes the record while its index member still exists. |
| `./check.sh UpdateProbesOtherForm=TRUE` | `LockExclusion` violated: a pod that resolved "no record, write form by flag" finds the record the other pod has since added under the other form and acts on it under the wrong lock. |
| `./check.sh AddLocksNewFormOnly=TRUE` | `LockExclusion` violated: an R1 pod whose flag still says old writes an old-form record while holding only the new-form lock. |
| `./check.sh UnguardedSREM=TRUE` | `NoLiveMembershipRemoved` violated: a crashed Add leaves a running record unindexed, the evictor starts removing it, a resume of the same ID is admitted and installs execution 2, and the Remove expecting execution 1 reads 2 and SREMs its live member. |
| `INVARIANTS="TypeOK NoLiveMembershipRemoved" ./check.sh UnguardedRemoveScripts=TRUE AddLocksNewFormOnly=TRUE` | `NoLiveMembershipRemoved` violated through a lost reply: the old-form remove script runs, its reply is lost, a resume is admitted and writes execution 2 under only the new-form lock, and the retried script, without its execution guard, deletes execution 2 and drops its member. `INVARIANTS="TypeOK NoSuccessorDeleted"` on the same run reports the deletion. |
| `./check.sh UnguardedCleanup=TRUE` | `NoSuccessorDeleted` violated: a resume's write does not land, and its cleanup, without the execution guard, deletes the stranded predecessor's running record under the same form. |
| `./check.sh RemoveSkipsRecheck=TRUE` | `NoSuccessorReportedGone` violated: a record its Add left unindexed is being removed by one pod while a resume of the same ID, admitted meanwhile, takes both locks on the other pod, deletes that record as the other form's and writes execution 2 under its own write form; the Remove finds its form empty and, without looking at the other, concludes the sandbox gone while execution 2's record exists. |
| `./check.sh CleanupSkipsSrem=TRUE` | `IndexHasRecord` violated with no crash, healer or earlier execution: a cleanup that drops the record but never the index entry leaves the entry for good. |
| `INVARIANTS="TypeOK IndexHasRecord" ./check.sh CleanupDeleteFirst=TRUE MaxCrashes=2` | `IndexHasRecord` violated: with the new-form cleanup's delete before its SREM, a healer re-index of a crashed predecessor's record, a write that landed over it with its reply lost, the cleanup's delete, and a crash before its SREM leave an entry with no record for good. In the protocol's order the same crash leaves a record without an entry, which the healer or the evictor clears. |

`UnguardedRemoveScripts` and `UnguardedCleanup` are separate constants so each
check is tested on its own; in the code the cleanup runs Remove's guarded
scripts and, in the new form, the same comparison of the record's execution
before its SREM, so the guards are shared.

Mutation (c) on its own breaks no invariant but `LockExclusion`: the old-form
remove script's executionID guard makes a re-executed script a no-op, and the
new-form Remove holds the new-form lock that the mutated Add still takes.
`./check.sh UnguardedRemoveScripts=TRUE` alone, with the two-lock Add kept,
passes. Either mechanism closes the lost-reply race; the protocol keeps both.

The admitted lasting state is reproduced with the strict invariant, which has
no exemption for it. Both fail with the double fault described above, the
first with the predecessor stranded by a crash, the second with the
predecessor stranded by a failed SADD:

```sh
INVARIANTS="TypeOK IndexHasRecordStrict" ./check.sh MaxCrashes=2 MaxFailedReplies=0
INVARIANTS="TypeOK IndexHasRecordStrict" ./check.sh
```

## What the model leaves out

- Locks expire only when their holder crashes. A removal stalled past the lock
  TTL while alive is a pre-existing hazard the protocol does not claim to
  close.
- Add's cleanup commands can lose a reply (re-executed, guarded) but not fail
  outright. The protocol treats the double failure as a residual; the model does
  not explore it. The expiration member Add added before a failed write is
  left behind, as it is in the code; the evictor sweeps it.
- The keepalive's write is a no-op and the healer's re-read and repair are one
  step. Both are sound under the locked-section rule, which `LockExclusion`
  checks; neither is sound once that rule is broken, so a mutation that breaks
  it is reported by `LockExclusion` and not by a downstream symptom.
- The healer's grace periods, the reservation's stale TTL, transition result
  keys, the global teams set and the sandbox's VM are not modelled. A wait
  reads its result at the `(form, tx)` it carries and then retries; nothing in
  the store depends on the result's value.
- One team, two sandboxes, two executions and two transitions per sandbox, and
  one of each fault per run; the command-level faults outside the cleanup
  share one budget, so a lost reply on a Remove and a failed Add write do not
  occur in the same run at the default bounds. Larger bounds are a matter of
  editing the configuration and waiting.
