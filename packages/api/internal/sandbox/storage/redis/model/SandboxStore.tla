---------------------------- MODULE SandboxStore ----------------------------
(* The sandbox store's lock and ordering protocol while per-sandbox keys    *)
(* move from the team-tagged form ("old") to the sandbox-tagged form ("new").*)
(*                                                                          *)
(* Two API pods act on two sandbox IDs. Pod p1 runs R1 (its write form is   *)
(* the flag as it last read it); pod p2 runs P2Release, R1 or R3 (R3 writes *)
(* the new form only). Every operation resolves a sandbox's form from the   *)
(* record that exists, else from the pod's write form, before it locks.     *)
(* Add is the exception: it locks both forms, old then new.                 *)
(*                                                                          *)
(* The model is small on purpose: one team, no time beyond a TTL counter on *)
(* transition markers, locks that expire only when their holder crashes,    *)
(* budgets of one for each fault. README.md explains how to run it.         *)
EXTENDS Naturals, TLC

CONSTANTS
  Sandboxes,              \* sandbox IDs; interchangeable, so declared symmetric
  P2Release,              \* "R1" or "R3": the release pod p2 runs (p1 runs R1)
  MaxExec,                \* executions one sandbox ID gets in a run (create + resumes)
  MaxTx,                  \* transitions one sandbox ID gets in a run
  TransitionTTL,          \* ticks a transition marker survives
  MaxCrashes,             \* pod crashes per run
  MaxFailedReplies,       \* command replies lost (the command is re-executed) or returned
                          \* as errors (Add's record write or SADD, after retries), per run
  MaxCleanupLostReplies,  \* replies lost on Add's cleanup commands, per run (its own budget,
                          \* since a cleanup only runs after a failed reply was spent)
  MaxTicks,               \* clock ticks per run (each ages every marker by one)
  MaxFlips,               \* flag flips per run
  MaxReplicationLosses,   \* records lost on a failover per run; 0 excludes the action
  RemoveDeletesBeforeSrem,  \* mutation (a): the new-form Remove deletes before it SREMs
  UpdateProbesOtherForm,    \* mutation (b): Update reads the other form inside its lock
  AddLocksNewFormOnly,      \* mutation (c): Add takes the new-form lock only
  UnguardedSREM,            \* mutation: the new-form Remove SREMs without comparing the execution
  UnguardedRemoveScripts,   \* mutation: Remove's scripts act on any execution (today's remove_sandbox)
  UnguardedCleanup,         \* mutation: Add's cleanup, its GET gate and its scripts, acts on any execution
  CleanupSkipsSrem,         \* mutation: Add's cleanup never removes the index entry
  CleanupDeleteFirst,       \* mutation: the new-form cleanup deletes the record before its SREM
  RemoveSkipsRecheck        \* mutation: Remove takes a resolved form found empty for a sandbox gone
                            \* without looking at the other form

ASSUME P2Release \in {"R1", "R3"}

Pods == {"p1", "p2"}
Forms == {"old", "new"}
Release == [p \in Pods |-> IF p = "p1" THEN "R1" ELSE P2Release]
NoPod == "none"
Exec == 1..MaxExec
Tx == 1..MaxTx

Other(f) == IF f = "old" THEN "new" ELSE "old"

Absent == [st |-> "absent"]
Running(e) == [st |-> "running", exec |-> e]
Removing(e) == [st |-> "removing", exec |-> e]
Present(r) == r.st # "absent"

\* The execution guard of the remove scripts: act only on the expected execution's record.
\* unguarded is the mutation constant that switches the guard off at the call site.
Guarded(unguarded, r, e) == Present(r) /\ (unguarded \/ r.exec = e)

(* Transition markers: the transition key with its id and remaining TTL *)
NoMarker == [tx |-> 0, ttl |-> 0]
Marker(t) == [tx |-> t, ttl |-> TransitionTTL]

(* In-flight operations, one per pod *)
Idle == [kind |-> "idle"]
\* sbx and exec: the sandbox and the execution an operation acts for; wf: the pod's write
\* form as the Add read it; pc: the program counter, the command the operation runs next.
\* delOrphaned: this Add's DEL of the other form removed a present record whose ID was in
\* the index (the healer's re-index of a predecessor); landed: this Add's record write
\* took effect; deleted: the execution the cleanup script deleted, 0 for none.
AddOp(s, e, wf, pc) ==
  [kind |-> "add", sbx |-> s, exec |-> e, wf |-> wf, pc |-> pc,
   delOrphaned |-> FALSE, landed |-> FALSE, deleted |-> 0]
StartOp(s, pin, f, pc) == [kind |-> "start", sbx |-> s, pin |-> pin, form |-> f, retried |-> FALSE, pc |-> pc]
WaitOp(s, pin, f, t) == [kind |-> "wait", sbx |-> s, pin |-> pin, form |-> f, tx |-> t]
\* tform and tx: the form and id of the marker the removal started under; form: the form it
\* acts on, which RemoveRecheck re-resolves. verdict: what the removal concluded, "removed",
\* "mismatch" (another execution's record, left whole) or "absent" (a sandbox gone).
RemoveOp(s, e, tf, t, pc) ==
  [kind |-> "remove", sbx |-> s, exec |-> e, tform |-> tf, tx |-> t, form |-> tf, deleted |-> 0,
   verdict |-> "none", pc |-> pc]
UpdateOp(s, f, pc) == [kind |-> "update", sbx |-> s, form |-> f, retried |-> FALSE, pc |-> pc]
HealOp(s, rule, f, pc) == [kind |-> "heal", sbx |-> s, rule |-> rule, form |-> f, deleted |-> 0, pc |-> pc]

VARIABLES
  rec,          \* [Forms -> [Sandboxes -> record]]: the record key of each form
  lock,         \* [Forms -> [Sandboxes -> Pods \cup {NoPod}]]: the lock key of each form
  index,        \* SUBSET Sandboxes: the team index set
  expiration,   \* SUBSET (Sandboxes \X Exec): the global expiration set, one member per execution
  trans,        \* [Forms -> [Sandboxes -> marker]]: the transition key of each form
  flag,         \* the flag's current value, a form
  podFlag,      \* [Pods -> Forms]: the flag as each pod last read it (R3 ignores it)
  op,           \* [Pods -> operation]
  acted,        \* [Pods -> SUBSET Forms]: forms whose record the pod touched inside its locked section
  nextExec,     \* [Sandboxes -> Nat]: the next execution id
  nextTx,       \* [Sandboxes -> Nat]: the next transition id
  budget,       \* remaining faults: crash, failedReply, cleanupReply, tick, flip, replicationLoss
  stranded,     \* history: sandboxes whose Add or Remove was interrupted in its window, until repaired
  orphanMember, \* history: index members an interrupted Add left without a record, until the healer removes them
  bad           \* history: action-level violations seen so far

vars == <<rec, lock, index, expiration, trans, flag, podFlag, op, acted, nextExec, nextTx, budget,
          stranded, orphanMember, bad>>

(* ------------------------------------------------------------------------ *)
(* Helpers                                                                   *)

WriteForm(p) == IF Release[p] = "R1" THEN podFlag[p] ELSE "new"

HasRecord(s) == \E f \in Forms : Present(rec[f][s])

\* Form resolution: what exists wins, else the pod's write form.
Resolve(p, s) ==
  IF Present(rec["new"][s]) THEN "new"
  ELSE IF Present(rec["old"][s]) THEN "old"
  ELSE WriteForm(p)

IsIdle(p) == op[p].kind = "idle"
SetOp(p, o) == op' = [op EXCEPT ![p] = o]
Advance(p, pc) == op' = [op EXCEPT ![p].pc = pc]

Acquire(p, f, s) == lock[f][s] = NoPod /\ lock' = [lock EXCEPT ![f][s] = p]
ReleaseAll(p) == lock' = [f \in Forms |-> [s \in Sandboxes |-> IF lock[f][s] = p THEN NoPod ELSE lock[f][s]]]

Touch(p, f) == acted' = [acted EXCEPT ![p] = @ \cup {f}]
Untouch(p) == acted' = [acted EXCEPT ![p] = {}]

Spend(k) == budget' = [budget EXCEPT ![k] = @ - 1]

\* The step's reply arrives, or is lost and the command is re-executed from the same program point.
Reply(p, o) ==
  \/ /\ op' = [op EXCEPT ![p] = o]
     /\ UNCHANGED budget
  \/ /\ budget.failedReply > 0
     /\ Spend("failedReply")
     /\ op' = [op EXCEPT ![p] = [o EXCEPT !.pc = op[p].pc]]

\* The same for Add's cleanup commands, on their own budget.
CleanupReply(p, o) ==
  \/ /\ op' = [op EXCEPT ![p] = o]
     /\ UNCHANGED budget
  \/ /\ budget.cleanupReply > 0
     /\ Spend("cleanupReply")
     /\ op' = [op EXCEPT ![p] = [o EXCEPT !.pc = op[p].pc]]

(* ------------------------------------------------------------------------ *)
(* Add: reserve, lock old then new, ZADD expiration, DEL the                  *)
(* other form, write the record, SADD the index, ZADD teams, unlock.         *)
(* A create and a resume are the same path; the reserve admits a sandbox ID  *)
(* only while it is not in the index.                                        *)

AddStart(p, s) ==
  /\ IsIdle(p)
  /\ s \notin index
  /\ ~\E q \in Pods : op[q].kind = "add" /\ op[q].sbx = s   \* the reservation's pending entry
  /\ nextExec[s] <= MaxExec
  /\ SetOp(p, AddOp(s, nextExec[s], WriteForm(p), IF AddLocksNewFormOnly THEN "lockNew" ELSE "lockOld"))
  /\ nextExec' = [nextExec EXCEPT ![s] = @ + 1]
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, acted, nextTx, budget>>

AddLockOld(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "lockOld"
  /\ Acquire(p, "old", o.sbx)
  /\ Advance(p, "lockNew")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

AddLockNew(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "lockNew"
  /\ Acquire(p, "new", o.sbx)
  /\ Untouch(p)
  /\ Advance(p, "zadd")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

AddZadd(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "zadd"
  /\ expiration' = expiration \cup {<<o.sbx, o.exec>>}
  /\ Advance(p, "delOther")
  /\ UNCHANGED <<rec, lock, index, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

\* The DEL may remove a predecessor's record the healer has re-indexed: the member then
\* has no record until this Add's write lands or its cleanup removes the member.
AddDelOther(p) == LET o == op[p] g == Other(o.wf) IN
  /\ o.kind = "add" /\ o.pc = "delOther"
  /\ rec' = [rec EXCEPT ![g][o.sbx] = Absent]
  /\ acted' = [acted EXCEPT ![p] = IF Present(rec[g][o.sbx]) THEN @ \cup {g} ELSE @]
  /\ op' = [op EXCEPT ![p].delOrphaned = Present(rec[g][o.sbx]) /\ o.sbx \in index,
                      ![p].pc = "write"]
  /\ UNCHANGED <<lock, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

\* The commit point: the record write. Old form: the add_sandbox script, SET and SADD in
\* one command. New form: SET, then SADD. The write can fail after the client's retries
\* with its effect landed (the reply was lost) or not; Add then cleans up its own
\* execution and returns the error (the create path kills the VM; not modelled).
WriteLands(p, o) ==
  /\ rec' = [rec EXCEPT ![o.wf][o.sbx] = Running(o.exec)]
  /\ index' = IF o.wf = "old" THEN index \cup {o.sbx} ELSE index
  /\ Touch(p, o.wf)

AddWrite(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "write"
  /\ \/ /\ WriteLands(p, o)
        /\ op' = [op EXCEPT ![p].landed = TRUE, ![p].pc = IF o.wf = "old" THEN "zaddTeams" ELSE "sadd"]
        /\ UNCHANGED budget
     \/ /\ budget.failedReply > 0
        /\ Spend("failedReply")
        /\ \/ WriteLands(p, o) /\ op' = [op EXCEPT ![p].landed = TRUE, ![p].pc = "cleanup"]
           \/ UNCHANGED <<rec, index, acted>> /\ Advance(p, "cleanup")
  /\ UNCHANGED <<lock, expiration, trans, flag, podFlag, nextExec, nextTx>>

\* The cleanup after a failed commit point, still under both locks, in the order every
\* removal uses: the index entry first. Old form: the guarded remove_sandbox script,
\* record and entry in one command. New form: GET the record and, only if it carries this
\* execution's ID, SREM the entry (AddCleanupIndex) and then the guarded remove_record
\* (AddCleanupDelete); an interruption between them leaves a record without an entry,
\* never the reverse. The guard matters here: a write that did not land may have left a
\* stranded predecessor's record under this form, which is not Add's to delete. A lost
\* reply on the GET is just another GET, so the gate has no budget.
AddCleanup(p) == LET o == op[p] r == rec[o.wf][o.sbx] hit == Guarded(UnguardedCleanup, r, o.exec) IN
  /\ o.kind = "add" /\ o.pc = "cleanup"
  /\ IF o.wf = "old"
       THEN /\ rec' = IF hit THEN [rec EXCEPT !["old"][o.sbx] = Absent] ELSE rec
            /\ index' = IF hit /\ ~CleanupSkipsSrem THEN index \ {o.sbx} ELSE index
            /\ CleanupReply(p, [o EXCEPT !.deleted = IF hit THEN r.exec ELSE 0, !.pc = "cleanupSrem"])
       ELSE /\ UNCHANGED <<rec, index, budget>>
            /\ Advance(p, IF ~hit THEN "cleanupSrem"
                          ELSE IF CleanupDeleteFirst THEN "cleanupDelete" ELSE "cleanupIndex")
  /\ UNCHANGED <<lock, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

\* New form: SREM the index entry. CleanupDeleteFirst, a mutation, runs it after the delete.
AddCleanupIndex(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "cleanupIndex"
  /\ index' = IF CleanupSkipsSrem THEN index ELSE index \ {o.sbx}
  /\ CleanupReply(p, [o EXCEPT !.pc = IF CleanupDeleteFirst THEN "cleanupSrem" ELSE "cleanupDelete"])
  /\ UNCHANGED <<rec, lock, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

\* New form: the guarded remove_record. A retry that finds nothing keeps what the first
\* execution deleted, for the record.
AddCleanupDelete(p) == LET o == op[p] r == rec[o.wf][o.sbx] hit == Guarded(UnguardedCleanup, r, o.exec) IN
  /\ o.kind = "add" /\ o.pc = "cleanupDelete"
  /\ rec' = IF hit THEN [rec EXCEPT ![o.wf][o.sbx] = Absent] ELSE rec
  /\ CleanupReply(p, [o EXCEPT !.deleted = IF hit THEN r.exec ELSE o.deleted,
                        !.pc = IF CleanupDeleteFirst THEN "cleanupIndex" ELSE "cleanupSrem"])
  /\ UNCHANGED <<lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

\* Then, for either form, SREM the index entry if neither form holds a record for the ID:
\* the healer may have re-indexed a crashed predecessor's record that this Add's DEL of
\* the other form removed. Add then returns the error.
AddCleanupSrem(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "cleanupSrem"
  /\ index' = IF ~HasRecord(o.sbx) /\ ~CleanupSkipsSrem THEN index \ {o.sbx} ELSE index
  /\ CleanupReply(p, [o EXCEPT !.pc = "unlock"])
  /\ UNCHANGED <<rec, lock, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

AddSadd(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "sadd"
  /\ \/ /\ index' = index \cup {o.sbx}
        /\ UNCHANGED budget
     \/ \* the SADD fails after the inline retries: Add returns success and the record
        \* stays unindexed until the healer re-adds it
        /\ budget.failedReply > 0
        /\ Spend("failedReply")
        /\ UNCHANGED index
  /\ Advance(p, "zaddTeams")
  /\ UNCHANGED <<rec, lock, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

\* The global teams set is not modelled; the step stays so a crash can land after the SADD.
AddZaddTeams(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "zaddTeams"
  /\ Advance(p, "unlock")
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

AddUnlock(p) == LET o == op[p] IN
  /\ o.kind = "add" /\ o.pc = "unlock"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ SetOp(p, Idle)
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

(* ------------------------------------------------------------------------ *)
(* StartRemoving: resolve, lock that form, GET it, check the                 *)
(* pin, then the start_transition script: record to removing, marker set.   *)
(* A transition already in flight is waited out, carrying (form, tx), and   *)
(* the removal is then retried in full.                                      *)
(* pin = 0 is a kill or pause by ID; a pinned start is the evictor acting on *)
(* an expiration member, which names the execution it saw.                   *)

RemoveStart(p, s, pin) ==
  /\ IsIdle(p)
  /\ pin = 0 \/ <<s, pin>> \in expiration
  /\ nextTx[s] <= MaxTx
  /\ SetOp(p, StartOp(s, pin, Resolve(p, s), "lock"))
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

StartLock(p) == LET o == op[p] IN
  /\ o.kind = "start" /\ o.pc = "lock"
  /\ Acquire(p, o.form, o.sbx)
  /\ Untouch(p)
  /\ Advance(p, "get")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

StartGet(p) == LET o == op[p] r == rec[o.form][o.sbx] m == trans[o.form][o.sbx] IN
  /\ o.kind = "start" /\ o.pc = "get"
  /\ IF ~Present(r)
       THEN \* the form resolved is empty: looked at once more in the other form (StartRecheck)
            /\ Advance(p, IF o.retried THEN "unlock" ELSE "recheck")
            /\ UNCHANGED <<lock, acted>>
       ELSE IF o.pin # 0 /\ r.exec # o.pin
       THEN \* the pin no longer holds: nothing to do
            /\ Advance(p, "unlock")
            /\ UNCHANGED <<lock, acted>>
       ELSE IF m # NoMarker
         THEN \* a transition is in flight: release the lock and wait for that (form, tx)
              /\ ReleaseAll(p)
              /\ Untouch(p)
              /\ SetOp(p, WaitOp(o.sbx, o.pin, o.form, m.tx))
         ELSE /\ Touch(p, o.form)
              /\ Advance(p, "begin")
              /\ UNCHANGED lock
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

\* The start_transition script: its own pin check, then the record and the marker in one command.
\* A sandbox out of transition ids (the MaxTx bound, nothing in the protocol) exits as a NotFound would.
StartBegin(p) == LET o == op[p] r == rec[o.form][o.sbx] t == nextTx[o.sbx] IN
  /\ o.kind = "start" /\ o.pc = "begin"
  /\ IF ~Present(r) \/ (o.pin # 0 /\ r.exec # o.pin) \/ t > MaxTx
       THEN /\ Advance(p, "unlock")
            /\ UNCHANGED <<rec, trans, nextTx>>
       ELSE /\ rec' = [rec EXCEPT ![o.form][o.sbx] = Removing(r.exec)]
            /\ trans' = [trans EXCEPT ![o.form][o.sbx] = Marker(t)]
            /\ nextTx' = [nextTx EXCEPT ![o.sbx] = @ + 1]
            /\ SetOp(p, RemoveOp(o.sbx, r.exec, o.form, t, "unlockStart"))
  /\ UNCHANGED <<lock, index, expiration, flag, podFlag, acted, nextExec, budget>>

\* The lock of the empty form is released, then the other form is read without a lock:
\* a record there, a resume's successor, is handled under that form's lock; none is ErrNotFound.
StartRecheck(p) == LET o == op[p] other == Other(o.form) IN
  /\ o.kind = "start" /\ o.pc = "recheck"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ op' = IF Present(rec[other][o.sbx])
             THEN [op EXCEPT ![p].form = other, ![p].retried = TRUE, ![p].pc = "lock"]
             ELSE [op EXCEPT ![p] = Idle]
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

StartUnlock(p) == LET o == op[p] IN
  /\ o.kind = "start" /\ o.pc = "unlock"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ SetOp(p, Idle)
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

\* The wait returns once the marker it carries is gone or replaced; it reads the result
\* at that same (form, tx) and retries the removal in full, resolving afresh.
WaitDone(p) == LET o == op[p] m == trans[o.form][o.sbx] IN
  /\ o.kind = "wait"
  /\ m = NoMarker \/ m.tx # o.tx
  /\ SetOp(p, StartOp(o.sbx, o.pin, Resolve(p, o.sbx), "lock"))
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

(* ------------------------------------------------------------------------ *)
(* Remove, after the transition started and the node's work is               *)
(* done: resolve, lock, GET and compare the execution, then                   *)
(*   old form: the remove_sandbox script (GET, DEL, SREM when the execution  *)
(*             matches), then ZREM;                                          *)
(*   new form: SREM, the guarded delete, then ZREM.                          *)
(* Then unlock, and finish_transition drops the marker if it is still ours.  *)

RemoveUnlockStart(p) == LET o == op[p] IN
  /\ o.kind = "remove" /\ o.pc = "unlockStart"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ Advance(p, "resolve")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

RemoveResolve(p) == LET o == op[p] IN
  /\ o.kind = "remove" /\ o.pc = "resolve"
  /\ op' = [op EXCEPT ![p].form = Resolve(p, o.sbx), ![p].pc = "lock"]
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

RemoveLock(p) == LET o == op[p] IN
  /\ o.kind = "remove" /\ o.pc = "lock"
  /\ Acquire(p, o.form, o.sbx)
  /\ Untouch(p)
  /\ Advance(p, "get")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

\* The record belongs to another execution: this removal has nothing left to do. The
\* comparison before the SREM is required protocol, not defence: a Remove that waited
\* for its lock behind a resume of the same ID (admitted while a crashed Add's record was
\* unindexed) would otherwise SREM the successor's live member (mutation UnguardedSREM
\* reproduces it). On the old form it is redundant with the script's own guard.
\* The record is gone from the form resolved: the other form is read while this lock is
\* still held. An Add holds both form locks while it writes, so a record there is a
\* completed resume's, with a new execution, and the verdict is a mismatch; both forms
\* empty under the lock is a sandbox gone (mutation RemoveSkipsRecheck skips the look and
\* reproduces the misjudgement). The two reads of the code are one step here: under the
\* lock no Add can land between them.
RemoveGet(p) == LET o == op[p] r == rec[o.form][o.sbx]
                    successor == ~RemoveSkipsRecheck /\ Present(rec[Other(o.form)][o.sbx]) IN
  /\ o.kind = "remove" /\ o.pc = "get"
  /\ IF ~Present(r)
       THEN /\ op' = [op EXCEPT ![p].pc = "unlock", ![p].verdict = IF successor THEN "mismatch" ELSE "absent"]
            /\ UNCHANGED acted
       ELSE IF ~UnguardedSREM /\ r.exec # o.exec
       THEN /\ op' = [op EXCEPT ![p].pc = "unlock", ![p].verdict = "mismatch"]
            /\ UNCHANGED acted
       ELSE /\ Touch(p, o.form)
            /\ Advance(p, IF o.form = "old" THEN "script"
                          ELSE IF RemoveDeletesBeforeSrem THEN "delete" ELSE "srem")
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

RemoveScript(p) == LET o == op[p] r == rec["old"][o.sbx] hit == Guarded(UnguardedRemoveScripts, r, o.exec) IN
  /\ o.kind = "remove" /\ o.pc = "script"
  /\ rec' = IF hit THEN [rec EXCEPT !["old"][o.sbx] = Absent] ELSE rec
  /\ index' = IF hit THEN index \ {o.sbx} ELSE index
  /\ Reply(p, [o EXCEPT !.deleted = IF hit THEN r.exec ELSE 0, !.pc = "zrem",
                        !.verdict = IF hit THEN "removed" ELSE IF Present(r) THEN "mismatch" ELSE "absent"])
  /\ UNCHANGED <<lock, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

RemoveSrem(p) == LET o == op[p] IN
  /\ o.kind = "remove" /\ o.pc = "srem"
  /\ index' = index \ {o.sbx}
  /\ Reply(p, [o EXCEPT !.pc = IF RemoveDeletesBeforeSrem THEN "zrem" ELSE "delete"])
  /\ UNCHANGED <<rec, lock, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

\* The remove_record script: delete only the expected execution's record, return what it deleted.
RemoveDelete(p) == LET o == op[p] r == rec[o.form][o.sbx] hit == Guarded(UnguardedRemoveScripts, r, o.exec) IN
  /\ o.kind = "remove" /\ o.pc = "delete"
  /\ rec' = IF hit THEN [rec EXCEPT ![o.form][o.sbx] = Absent] ELSE rec
  /\ Reply(p, [o EXCEPT !.deleted = IF hit THEN r.exec ELSE 0,
                        !.verdict = IF hit THEN "removed" ELSE IF Present(r) THEN "mismatch" ELSE "absent",
                        !.pc = IF RemoveDeletesBeforeSrem THEN "srem" ELSE "zrem"])
  /\ UNCHANGED <<lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx>>

\* ZREM the member of the execution actually deleted; a retry that deleted nothing drops no member.
RemoveZrem(p) == LET o == op[p] IN
  /\ o.kind = "remove" /\ o.pc = "zrem"
  /\ expiration' = IF o.deleted # 0 THEN expiration \ {<<o.sbx, o.deleted>>} ELSE expiration
  /\ Advance(p, "unlock")
  /\ UNCHANGED <<rec, lock, index, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

RemoveUnlock(p) == LET o == op[p] IN
  /\ o.kind = "remove" /\ o.pc = "unlock"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ Advance(p, "finish")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

\* finish_transition, keyed by the form the transition started under: drop the marker if it is still this transition's.
RemoveFinish(p) == LET o == op[p] m == trans[o.tform][o.sbx] IN
  /\ o.kind = "remove" /\ o.pc = "finish"
  /\ trans' = [trans EXCEPT ![o.tform][o.sbx] = IF m.tx = o.tx THEN NoMarker ELSE m]
  /\ SetOp(p, Idle)
  /\ UNCHANGED <<rec, lock, index, expiration, flag, podFlag, acted, nextExec, nextTx, budget>>

(* ------------------------------------------------------------------------ *)
(* Update / keepalive: resolve, lock that form, GET that form                *)
(* only, write it back. A nil inside the lock is ErrNotFound.                *)

UpdateStart(p, s) ==
  /\ IsIdle(p)
  /\ SetOp(p, UpdateOp(s, Resolve(p, s), "lock"))
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

UpdateLock(p) == LET o == op[p] IN
  /\ o.kind = "update" /\ o.pc = "lock"
  /\ Acquire(p, o.form, o.sbx)
  /\ Untouch(p)
  /\ Advance(p, "get")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

UpdateGet(p) == LET o == op[p] g == Other(o.form) IN
  /\ o.kind = "update" /\ o.pc = "get"
  /\ IF Present(rec[o.form][o.sbx])
       THEN /\ Touch(p, o.form)
            /\ Advance(p, "write")
       ELSE IF UpdateProbesOtherForm /\ Present(rec[g][o.sbx])
         THEN \* mutation (b): a nil taken as a cue to probe, and then act on, the other form
              /\ Touch(p, g)
              /\ op' = [op EXCEPT ![p].form = g, ![p].pc = "write"]
         ELSE \* the form resolved is empty: looked at once more in the other form (UpdateRecheck)
              /\ UNCHANGED acted
              /\ Advance(p, IF o.retried THEN "unlock" ELSE "recheck")
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

\* As StartRecheck: release, read the other form without a lock, continue under its lock
\* when a record is there, otherwise ErrNotFound.
UpdateRecheck(p) == LET o == op[p] other == Other(o.form) IN
  /\ o.kind = "update" /\ o.pc = "recheck"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ op' = IF Present(rec[other][o.sbx])
             THEN [op EXCEPT ![p].form = other, ![p].retried = TRUE, ![p].pc = "lock"]
             ELSE [op EXCEPT ![p] = Idle]
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

\* SET KEEPTTL of the record read above; no modelled field changes, so the record is unchanged.
UpdateWrite(p) == LET o == op[p] IN
  /\ o.kind = "update" /\ o.pc = "write"
  /\ Touch(p, o.form)
  /\ Advance(p, "unlock")
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

UpdateUnlock(p) == LET o == op[p] IN
  /\ o.kind = "update" /\ o.pc = "unlock"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ SetOp(p, Idle)
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

(* ------------------------------------------------------------------------ *)
(* The healer. The lock-free walk nominates a candidate; every               *)
(* repair re-reads under the sandbox's lock and acts only if the condition   *)
(* still holds:                                                              *)
(*   readd:       a running record with no index member -> SADD;             *)
(*   deleteStale: a non-running record with no index member -> delete it and *)
(*                its expiration member;                                     *)
(*   remove:      an index member with no record in either form -> SREM.     *)
(* The grace periods are not modelled: the healer may act at any time.       *)

HealStart(p, s, rule) ==
  /\ IsIdle(p)
  /\ CASE rule = "readd" ->
            \E f \in Forms : rec[f][s].st = "running" /\ s \notin index /\ SetOp(p, HealOp(s, rule, f, "lock"))
       [] rule = "deleteStale" ->
            \E f \in Forms : rec[f][s].st = "removing" /\ s \notin index /\ SetOp(p, HealOp(s, rule, f, "lock"))
       [] rule = "remove" ->
            s \in index /\ ~HasRecord(s) /\ SetOp(p, HealOp(s, rule, WriteForm(p), "lock"))
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

HealLock(p) == LET o == op[p] IN
  /\ o.kind = "heal" /\ o.pc = "lock"
  /\ Acquire(p, o.form, o.sbx)
  /\ Untouch(p)
  /\ Advance(p, "check")
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

HealCheck(p) == LET o == op[p] r == rec[o.form][o.sbx] IN
  /\ o.kind = "heal" /\ o.pc = "check"
  /\ CASE o.rule = "readd" ->
            IF r.st = "running" /\ o.sbx \notin index
              THEN /\ index' = index \cup {o.sbx}
                   /\ Touch(p, o.form)
                   /\ Advance(p, "unlock")
                   /\ UNCHANGED rec
              ELSE /\ Advance(p, "unlock")
                   /\ UNCHANGED <<rec, index, acted>>
       [] o.rule = "deleteStale" ->
            IF r.st = "removing" /\ o.sbx \notin index
              THEN /\ rec' = [rec EXCEPT ![o.form][o.sbx] = Absent]
                   /\ Touch(p, o.form)
                   /\ op' = [op EXCEPT ![p].deleted = r.exec, ![p].pc = "zrem"]
                   /\ UNCHANGED index
              ELSE /\ Advance(p, "unlock")
                   /\ UNCHANGED <<rec, index, acted>>
       [] o.rule = "remove" ->
            \* both forms are read under the one lock; an absent record is not touched
            IF ~HasRecord(o.sbx) /\ o.sbx \in index
              THEN /\ index' = index \ {o.sbx}
                   /\ Advance(p, "unlock")
                   /\ UNCHANGED <<rec, acted>>
              ELSE /\ Advance(p, "unlock")
                   /\ UNCHANGED <<rec, index, acted>>
  /\ UNCHANGED <<lock, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

HealZrem(p) == LET o == op[p] IN
  /\ o.kind = "heal" /\ o.pc = "zrem"
  /\ expiration' = expiration \ {<<o.sbx, o.deleted>>}
  /\ Advance(p, "unlock")
  /\ UNCHANGED <<rec, lock, index, trans, flag, podFlag, acted, nextExec, nextTx, budget>>

HealUnlock(p) == LET o == op[p] IN
  /\ o.kind = "heal" /\ o.pc = "unlock"
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ SetOp(p, Idle)
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx, budget>>

(* ------------------------------------------------------------------------ *)
(* Faults and the environment                                                *)

\* The pod dies between any two steps; its locks expire (modelled as released at once).
Crash(p) ==
  /\ ~IsIdle(p)
  /\ budget.crash > 0
  /\ Spend("crash")
  /\ ReleaseAll(p)
  /\ Untouch(p)
  /\ SetOp(p, Idle)
  /\ UNCHANGED <<rec, index, expiration, trans, flag, podFlag, nextExec, nextTx>>

\* The clock ticks: every marker ages by one, and those at the end of their TTL expire.
Tick ==
  /\ budget.tick > 0
  /\ \E f \in Forms, s \in Sandboxes : trans[f][s] # NoMarker
  /\ Spend("tick")
  /\ trans' = [f \in Forms |-> [s \in Sandboxes |->
                 IF trans[f][s] = NoMarker \/ trans[f][s].ttl <= 1 THEN NoMarker
                 ELSE [trans[f][s] EXCEPT !.ttl = @ - 1]]]
  /\ UNCHANGED <<rec, lock, index, expiration, flag, podFlag, op, acted, nextExec, nextTx>>

FlagFlip ==
  /\ budget.flip > 0
  /\ Spend("flip")
  /\ flag' = Other(flag)
  /\ UNCHANGED <<rec, lock, index, expiration, trans, podFlag, op, acted, nextExec, nextTx>>

\* An R1 pod re-reads the flag on its own schedule.
ReadFlag(p) ==
  /\ Release[p] = "R1"
  /\ podFlag[p] # flag
  /\ podFlag' = [podFlag EXCEPT ![p] = flag]
  /\ UNCHANGED <<rec, lock, index, expiration, trans, flag, op, acted, nextExec, nextTx, budget>>

\* A failover loses a record's write on its primary; the index, on another primary, keeps the member.
ReplicationLoss(f, s) ==
  /\ budget.replicationLoss > 0
  /\ Present(rec[f][s])
  /\ Spend("replicationLoss")
  /\ rec' = [rec EXCEPT ![f][s] = Absent]
  /\ UNCHANGED <<lock, index, expiration, trans, flag, podFlag, op, acted, nextExec, nextTx>>

(* ------------------------------------------------------------------------ *)
(* History, kept by every step                                               *)

\* The transient windows the protocol admits: Add between the record write and the SADD (or
\* the cleanup of a failed write), Remove between the SREM and the delete. A Remove
\* whose SREM reply was lost is still at "srem" after the SREM executed, so that step
\* is in the window too.
CleanupPcs == {"cleanup", "cleanupIndex", "cleanupDelete", "cleanupSrem"}

InWindow(p, s) ==
  \/ op[p].kind = "add" /\ op[p].sbx = s /\ op[p].pc \in {"sadd"} \cup CleanupPcs
  \/ op[p].kind = "remove" /\ op[p].sbx = s /\ op[p].pc \in {"srem", "delete"}

\* An operation interrupted in its window: a crash there, or Add's SADD failing
\* (the Add moves on without the member).
Interrupted(s) ==
  \E p \in Pods : /\ InWindow(p, s)
                  /\ \/ op'[p].kind = "idle" /\ budget'.crash < budget.crash
                     \/ op[p].pc = "sadd" /\ op'[p].pc = "zaddTeams" /\ s \notin index'

\* The one admitted lasting producer of an index member with no record, the protocol's
\* double fault: this Add's DEL of the other form removed a present record whose ID
\* the healer had re-indexed (a crashed predecessor's), and this Add then crashed
\* before its own write landed. Nothing else is excused.
AdmittedCrash(s) ==
  \E p \in Pods : /\ op[p].kind = "add" /\ op[p].sbx = s
                  /\ op[p].delOrphaned /\ ~op[p].landed
                  /\ op[p].pc \in {"write"} \cup CleanupPcs
                  /\ op'[p].kind = "idle" /\ budget'.crash < budget.crash

\* An index member removed while a running record exists. Add's cleanup of its own
\* failed commit (the old-form script, or the new form's SREM before its delete) is the
\* one legitimate case and is excluded, and only for Add's own execution.
LiveMembershipRemovedNow ==
  \E s \in Sandboxes :
    /\ s \in index /\ s \notin index'
    /\ \E f \in Forms : rec[f][s].st = "running"
    /\ ~\E p \in Pods : /\ op[p].kind = "add" /\ op[p].sbx = s /\ op[p].pc \in {"cleanup", "cleanupIndex"}
                        /\ Present(rec[op[p].wf][s]) /\ rec[op[p].wf][s].exec = op[p].exec

\* An index member added for a sandbox whose record is not running (a dead record;
\* a member with no record at all is IndexHasRecord's concern).
DeadReAddNow ==
  \E s \in Sandboxes : s \notin index /\ s \in index' /\ \E f \in Forms : rec'[f][s].st = "removing"

\* A removal, or Add's cleanup, deleted a record of an execution other than its own.
SuccessorDeletedNow ==
  \E p \in Pods : op'[p].kind \in {"remove", "add"} /\ op'[p].deleted \notin {0, op'[p].exec}

\* A removal concluded its sandbox gone while a record of another execution exists: the
\* store would release that execution's reservation, dropping a start still pending.
SuccessorReportedGoneNow ==
  \E p \in Pods : /\ op[p].kind = "remove" /\ op'[p].kind = "remove"
                  /\ op[p].verdict # "absent" /\ op'[p].verdict = "absent"
                  /\ \E f \in Forms : Present(rec'[f][op[p].sbx]) /\ rec'[f][op[p].sbx].exec # op[p].exec

History ==
  /\ bad' = bad \cup {k \in {"liveMembershipRemoved", "deadReAdd", "successorDeleted", "successorReportedGone"} :
                        \/ k = "liveMembershipRemoved" /\ LiveMembershipRemovedNow
                        \/ k = "deadReAdd" /\ DeadReAddNow
                        \/ k = "successorDeleted" /\ SuccessorDeletedNow
                        \/ k = "successorReportedGone" /\ SuccessorReportedGoneNow}
  /\ stranded' = {s \in Sandboxes :
                    /\ s \in stranded \/ Interrupted(s)
                    /\ \E f \in Forms : Present(rec'[f][s])
                    /\ s \notin index'}
  /\ orphanMember' = {s \in Sandboxes :
                        /\ s \in orphanMember \/ AdmittedCrash(s)
                        /\ s \in index'
                        /\ ~\E f \in Forms : Present(rec'[f][s])}

(* ------------------------------------------------------------------------ *)

Init ==
  /\ rec = [f \in Forms |-> [s \in Sandboxes |-> Absent]]
  /\ lock = [f \in Forms |-> [s \in Sandboxes |-> NoPod]]
  /\ index = {}
  /\ expiration = {}
  /\ trans = [f \in Forms |-> [s \in Sandboxes |-> NoMarker]]
  /\ flag = "old"
  /\ podFlag = [p \in Pods |-> "old"]
  /\ op = [p \in Pods |-> Idle]
  /\ acted = [p \in Pods |-> {}]
  /\ nextExec = [s \in Sandboxes |-> 1]
  /\ nextTx = [s \in Sandboxes |-> 1]
  /\ budget = [crash |-> MaxCrashes, failedReply |-> MaxFailedReplies,
               cleanupReply |-> MaxCleanupLostReplies, tick |-> MaxTicks,
               flip |-> MaxFlips, replicationLoss |-> MaxReplicationLosses]
  /\ stranded = {}
  /\ orphanMember = {}
  /\ bad = {}

Step ==
  \/ \E p \in Pods :
       \/ \E s \in Sandboxes : AddStart(p, s)
       \/ AddLockOld(p) \/ AddLockNew(p) \/ AddZadd(p) \/ AddDelOther(p) \/ AddWrite(p)
       \/ AddCleanup(p) \/ AddCleanupIndex(p) \/ AddCleanupDelete(p) \/ AddCleanupSrem(p)
       \/ AddSadd(p) \/ AddZaddTeams(p) \/ AddUnlock(p)
       \/ \E s \in Sandboxes, pin \in {0} \cup Exec : RemoveStart(p, s, pin)
       \/ StartLock(p) \/ StartGet(p) \/ StartRecheck(p) \/ StartBegin(p) \/ StartUnlock(p) \/ WaitDone(p)
       \/ RemoveUnlockStart(p) \/ RemoveResolve(p) \/ RemoveLock(p) \/ RemoveGet(p)
       \/ RemoveScript(p) \/ RemoveSrem(p) \/ RemoveDelete(p) \/ RemoveZrem(p)
       \/ RemoveUnlock(p) \/ RemoveFinish(p)
       \/ \E s \in Sandboxes : UpdateStart(p, s)
       \/ UpdateLock(p) \/ UpdateGet(p) \/ UpdateRecheck(p) \/ UpdateWrite(p) \/ UpdateUnlock(p)
       \/ \E s \in Sandboxes, rule \in {"readd", "deleteStale", "remove"} : HealStart(p, s, rule)
       \/ HealLock(p) \/ HealCheck(p) \/ HealZrem(p) \/ HealUnlock(p)
       \/ Crash(p) \/ ReadFlag(p)
  \/ Tick \/ FlagFlip
  \/ \E f \in Forms, s \in Sandboxes : ReplicationLoss(f, s)

Next == Step /\ History

Spec == Init /\ [][Next]_vars

SandboxPerms == Permutations(Sandboxes)

(* ------------------------------------------------------------------------ *)
(* Invariants                                                                *)

TypeOK ==
  /\ \A f \in Forms, s \in Sandboxes :
       /\ rec[f][s].st \in {"absent", "running", "removing"}
       /\ lock[f][s] \in Pods \cup {NoPod}
       /\ trans[f][s].tx \in {0} \cup Tx
  /\ index \subseteq Sandboxes
  /\ expiration \subseteq Sandboxes \X Exec
  /\ flag \in Forms
  /\ \A p \in Pods : podFlag[p] \in Forms /\ acted[p] \subseteq Forms
       /\ op[p].kind \in {"idle", "add", "start", "wait", "remove", "update", "heal"}
  /\ stranded \subseteq Sandboxes /\ orphanMember \subseteq Sandboxes

\* 1. A sandbox has at most one record, under one form.
OneRecordPerSandbox == \A s \in Sandboxes : ~(Present(rec["old"][s]) /\ Present(rec["new"][s]))

\* 2. Every index member has a record, with the exceptions the protocol admits.
\*    ReplicationLoss breaks it, and the configuration gives that action no budget
\*    by default; enabling it violates this invariant by design. The other exception
\*    was found by this model: a resume reserved while a crashed Add's running
\*    record was unindexed, after which the healer re-indexed that record; the
\*    resume's Add then deletes the other form's record before writing its own.
\*    While that Add holds both locks, between its DEL and the end of its cleanup,
\*    the member may have no record (AddWindow): the write that follows, or the
\*    cleanup's SREM, ends it. It lasts only if the Add crashes before its write
\*    landed (orphanMember, until the healer's removed rule drops the member). The
\*    strict form below has no lasting exemption and reproduces that state (README).
AddWindow(p, s) == op[p].kind = "add" /\ op[p].sbx = s /\ op[p].pc \in {"write"} \cup CleanupPcs

IndexHasRecordStrict == \A s \in index : HasRecord(s) \/ \E p \in Pods : AddWindow(p, s)

IndexHasRecord == \A s \in index : HasRecord(s) \/ (\E p \in Pods : AddWindow(p, s)) \/ s \in orphanMember

\* 3. Every record has an index member, except inside Add's and Remove's transient
\*    windows and after a crash interrupted one of them, until a repair.
RecordIndexedOrTransient ==
  \A s \in Sandboxes : HasRecord(s) => s \in index \/ (\E p \in Pods : InWindow(p, s)) \/ s \in stranded

\* 4. A pod inside a locked section touches only records whose form lock it holds,
\*    so two pods never act on one sandbox's record under different lock keys.
LockExclusion == \A p \in Pods : \A f \in acted[p] : lock[f][op[p].sbx] = p

\* A removal, or Add's cleanup, expecting execution e never deletes a record of
\* another execution.
NoSuccessorDeleted == "successorDeleted" \notin bad

\* No SREM, by a removal's retry or by the healer, ever drops a running sandbox's member.
NoLiveMembershipRemoved == "liveMembershipRemoved" \notin bad

\* No SADD, by the healer in particular, ever indexes a sandbox whose record is not running.
NoDeadReAdd == "deadReAdd" \notin bad

\* A removal never concludes its sandbox gone while another execution's record exists: the
\* verdict is taken under the lock, where the other form is read too.
NoSuccessorReportedGone == "successorReportedGone" \notin bad

=============================================================================
