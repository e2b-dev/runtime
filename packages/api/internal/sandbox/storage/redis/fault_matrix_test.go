package redis

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// A flip row has one instance writing the sandbox form while another still
// writes the team form, as replicas mid-rollout do, and every row's repair
// checks run only after the crashed locks have expired and one healer sweep,
// the point at which the design promises the store is whole again.

var errInjected = errors.New("injected failure")

// matrixForm is the key-form column of a row.
type matrixForm string

const (
	matrixTeam    matrixForm = "team"
	matrixSandbox matrixForm = "sandbox"
	matrixFlip    matrixForm = "flip"
)

// matrixKind is the failure a row injects.
type matrixKind string

const (
	matrixError matrixKind = "error"
	matrixCrash matrixKind = "crash"
)

type matrixRow struct {
	point faultPoint
	kind  matrixKind
	form  matrixForm
}

func (r matrixRow) String() string {
	return fmt.Sprintf("%s/%s/%s", r.point, r.kind, r.form)
}

// injected returns the error the row's hook injects.
func (r matrixRow) injected() error {
	if r.kind == matrixCrash {
		return errSimulatedCrash
	}

	return errInjected
}

func (r matrixRow) crash() bool {
	return r.kind == matrixCrash
}

// matrixFamily is the operation a fault point sits in.
type matrixFamily int

const (
	familyAdd matrixFamily = iota + 1
	familyRemove
	familyUpdate
	familyReaders
	familyHealer
)

func familyOf(p faultPoint) matrixFamily {
	switch p {
	case faultAddAfterLocks, faultAddBeforeOtherDel, faultAddAfterOtherDel, faultAddRecordWrite, faultAddAfterRecordWrite, faultAddAfterIndex:
		return familyAdd
	case faultRemoveAfterSREM, faultRemoveAfterDelete:
		return familyRemove
	case faultUpdateAfterResolve:
		return familyUpdate
	case faultReadersSecondPipeline:
		return familyReaders
	default:
		return familyHealer
	}
}

// skipReason says why a point cannot fire in a column. The team form's
// scripts write the record with its index entry and remove the two
// together, so nothing sits between them to fault.
func skipReason(p faultPoint, form matrixForm) string {
	if form != matrixTeam {
		return ""
	}
	switch p {
	case faultAddAfterRecordWrite:
		return "the team form's add script writes the index entry with the record, so the point is never consulted"
	case faultRemoveAfterSREM:
		return "the team form's remove script removes the index entry with the record, so the point is never consulted"
	}

	return ""
}

// repairsOf returns the repairs a sweep made, without its census.
func repairsOf(report healReport) healReport {
	return healReport{Readded: report.Readded, Removed: report.Removed, DeletedStale: report.DeletedStale, TwinsDeleted: report.TwinsDeleted}
}

// matrixEnv is one row's world: a team with live sandboxes that are never
// faulted, an undecodable record the readers must skip, the sandbox the
// fault hits, and the instance that wrote the existing records beside the
// one that runs the faulted operation. In the flip column the writer holds
// the sandbox form and the actor the team form, except for an Add, whose
// predecessor the team-form instance wrote and which the sandbox-form
// instance resumes.
type matrixEnv struct {
	row    matrixRow
	client redis.UniversalClient
	writer *Storage
	actor  *Storage
	team   uuid.UUID
	future time.Time
	past   time.Time

	// refs are the sandboxes the row touched, for the invariants.
	refs []recordRef
	// lives are the sandboxes never faulted; alive are the IDs of every
	// sandbox whose VM exists after the fault and which the store must
	// serve: the live ones, the healer rows' other nominees, and the victim
	// once it is.
	lives []sandboxtypes.Sandbox
	alive []string
	// victim is the sandbox the fault hits, as the row wants it after the
	// repair; original is the execution the fault hit.
	victim   sandboxtypes.Sandbox
	original sandboxtypes.Sandbox
	// twinOlder and twinNewer are the healer rows' twin in the flip column.
	twinOlder sandboxtypes.Sandbox
	twinNewer sandboxtypes.Sandbox
	// garbageMember is the undecodable record's expiration member.
	garbageMember string
	// lockIDs are the sandboxes whose locks the row's operation may take;
	// heldOnCrash are the lock keys a crash leaves held, which the row
	// expires before the sweep.
	lockIDs     []string
	heldOnCrash []string
	// expectRepairs is what the sweep after the fault must repair.
	expectRepairs healReport
	fired         int
}

func newMatrixEnv(t *testing.T, row matrixRow) *matrixEnv {
	t.Helper()

	env := &matrixEnv{row: row, client: redis_utils.SetupInstance(t), team: uuid.New(), future: time.Now().Add(time.Hour), past: time.Now().Add(-time.Hour)}
	instance := func(sandboxForm bool) *Storage {
		s := newTestStorage(t, env.client)
		s.SetPerSandboxWrites(sandboxForm)

		return s
	}
	switch row.form {
	case matrixTeam:
		env.writer = instance(false)
		env.actor = env.writer
	case matrixSandbox:
		env.writer = instance(true)
		env.actor = env.writer
	case matrixFlip:
		env.writer, env.actor = instance(true), instance(false)
		if familyOf(row.point) == familyAdd {
			env.writer, env.actor = env.actor, env.writer
		}
	}

	env.garbageMember = writeGarbageRecord(t, env.client, env.garbage())
	env.lockIDs = []string{garbageSandboxID}

	// Alternating instances put live records in both forms in the flip column.
	for i := range 3 {
		sbx := env.record(fmt.Sprintf("sbx-live-%d", i), env.future)
		s := env.writer
		if i%2 == 1 {
			s = env.actor
		}
		require.NoError(t, s.Add(t.Context(), sbx))
		env.lives = append(env.lives, sbx)
		env.alive = append(env.alive, sbx.SandboxID)
	}

	return env
}

func (env *matrixEnv) garbage() recordRef {
	return recordRef{TeamID: env.team.String(), SandboxID: garbageSandboxID}
}

// record builds a Running record that started a grace period ago, so the
// one sweep after the fault stands for the first sweep past the grace
// period, and tracks it for the invariants.
func (env *matrixEnv) record(id string, end time.Time) sandboxtypes.Sandbox {
	env.track(id)

	return makeIndexedSandbox(env.team, id, uuid.NewString(), time.Now().Add(-2*healGracePeriod), end)
}

func (env *matrixEnv) track(id string) {
	ref := recordRef{TeamID: env.team.String(), SandboxID: id}
	if !slices.Contains(env.refs, ref) {
		env.refs = append(env.refs, ref)
	}
}

func (env *matrixEnv) index() string {
	return GetSandboxStorageTeamIndexKey(env.team.String())
}

// arm installs the row's hook on the actor; it counts the firings.
func (env *matrixEnv) arm() {
	env.actor.SetFaultHook(func(p faultPoint) error {
		if p != env.row.point {
			return nil
		}
		env.fired++

		return env.row.injected()
	})
}

func (env *matrixEnv) disarm() {
	env.actor.SetFaultHook(nil)
}

// report is a node's report of the given sandboxes and of the undecodable
// record, all old enough to be judged.
func (env *matrixEnv) report(ids ...string) []sandboxtypes.NodeSandbox {
	nodes := make([]sandboxtypes.NodeSandbox, 0, len(ids)+1)
	for _, id := range slices.Concat(ids, []string{garbageSandboxID}) {
		nodes = append(nodes, sandboxtypes.NodeSandbox{SandboxID: id, TeamID: env.team, StartTime: time.Now().Add(-2 * orphanGracePeriod)})
	}

	return nodes
}

// resolvedLock returns the lock key an operation on the sandbox takes: the
// form its record is in, or the actor's write form when it has none.
func (env *matrixEnv) resolvedLock(t *testing.T, id string) string {
	t.Helper()

	form, ok, err := env.actor.formOfExisting(t.Context(), env.team.String(), id)
	require.NoError(t, err)
	if !ok {
		form = env.actor.writeForm()
	}

	return redis_utils.GetLockKey(recordKey(form, env.team.String(), id))
}

// pauseOut completes a pause on s as the removal flow does: the transition,
// the removal, the callback.
func (env *matrixEnv) pauseOut(t *testing.T, s *Storage, sbx sandboxtypes.Sandbox) {
	t.Helper()

	transition, alreadyDone, done, err := s.StartRemoving(t.Context(), env.team, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	require.False(t, alreadyDone)
	require.NoError(t, s.Remove(t.Context(), env.team, sbx.SandboxID, transition.Sandbox.ExecutionID))
	done(t.Context(), nil)
}

// inject runs the row's operation under its hook and checks what the
// operation itself reports, the state it leaves, and what the sweep must
// then repair.
func (env *matrixEnv) inject(t *testing.T) {
	t.Helper()

	switch familyOf(env.row.point) {
	case familyAdd:
		env.injectAdd(t)
	case familyRemove:
		env.injectRemove(t)
	case familyUpdate:
		env.injectUpdate(t)
	case familyReaders:
		env.injectReaders(t)
	case familyHealer:
		env.injectHealer(t)
	}
}

// injectAdd resumes, under the fault, a sandbox the writer created and
// paused out, which removed its record, entry and member: a resume is the
// Add this project changes, and it writes the ID afresh in the actor's
// form. An error at the index entry or at the team's global entry is a
// success the healer completes; every other error fails the Add; a crash
// ends it where it stands, both locks held.
func (env *matrixEnv) injectAdd(t *testing.T) {
	t.Helper()

	predecessor := env.record("sbx-victim", env.future)
	require.NoError(t, env.writer.Add(t.Context(), predecessor))
	env.pauseOut(t, env.writer, predecessor)
	env.victim = predecessor
	env.victim.ExecutionID = uuid.NewString()
	env.original = env.victim
	env.lockIDs = append(env.lockIDs, env.victim.SandboxID)
	if env.row.crash() {
		teamLock, sandboxLock := lockKeys(env.team, env.victim.SandboxID)
		env.heldOnCrash = []string{teamLock, sandboxLock}
	}

	env.arm()
	err := env.actor.Add(t.Context(), env.victim)
	switch {
	case env.row.crash():
		require.ErrorIs(t, err, errSimulatedCrash)
	case env.row.point == faultAddAfterRecordWrite, env.row.point == faultAddAfterIndex:
		require.NoError(t, err)
	default:
		require.ErrorIs(t, err, errInjected)
	}
	env.assertAddState(t)
}

// addOutcome is the state the design admits after the row's fault: whether
// the resumed record exists, whether it is indexed, and whether its
// expiration member was written. Before the record write nothing but the
// member exists, and an error there undoes the write; the team form's
// script indexes with the write, the sandbox form's entry follows it.
func (env *matrixEnv) addOutcome() (record, indexed, member bool) {
	crash := env.row.crash()
	switch env.row.point {
	case faultAddAfterLocks:
		return false, false, false
	case faultAddBeforeOtherDel, faultAddAfterOtherDel:
		return false, false, true
	case faultAddRecordWrite:
		return crash, crash && env.actor.writeForm() == formTeam, true
	case faultAddAfterRecordWrite:
		return true, false, true
	default:
		return true, true, true
	}
}

func (env *matrixEnv) assertAddState(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	record, indexed, member := env.addOutcome()
	form := env.actor.writeForm()
	key := recordKey(form, env.team.String(), env.victim.SandboxID)
	if record {
		requireRecord(t, env.client, key, mustJSON(t, env.victim))
		sbx, err := env.actor.Get(ctx, env.team, env.victim.SandboxID)
		require.NoError(t, err)
		require.Equal(t, env.victim.ExecutionID, sbx.ExecutionID)
	} else {
		requireNoRecord(t, env.client, key)
		requireNoRecord(t, env.client, recordKey(form.other(), env.team.String(), env.victim.SandboxID))
		_, err := env.actor.Get(ctx, env.team, env.victim.SandboxID)
		require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	}
	requireIndexed(t, env.client, env.index(), env.victim.SandboxID, indexed)
	if member {
		requireMemberScore(t, env.client, sandboxExpirationMember(env.victim), float64(env.victim.EndTime.UnixMilli()))
	} else {
		requireMemberAbsent(t, env.client, sandboxExpirationMember(env.victim))
	}
	if record && !indexed {
		env.expectRepairs.Readded = 1
	}
}

// injectRemove evicts an expired sandbox the writer holds, as the removal
// flow orders it: the kill transition, the removal under the fault, and the
// callback, which a crashed replica never makes. A failed member removal is
// a success that leaves the member; a failed record delete leaves the
// record stranded without its entry, fails, and is the healer's to delete
// once stale; a crash leaves the record's lock held.
func (env *matrixEnv) injectRemove(t *testing.T) {
	t.Helper()

	env.victim = env.record("sbx-victim", env.past)
	require.NoError(t, env.writer.Add(t.Context(), env.victim))
	env.original = env.victim
	env.lockIDs = append(env.lockIDs, env.victim.SandboxID)
	key := recordKey(env.writer.writeForm(), env.team.String(), env.victim.SandboxID)
	if env.row.crash() {
		env.heldOnCrash = []string{redis_utils.GetLockKey(key)}
	}
	transition, alreadyDone, done, err := env.actor.StartRemoving(t.Context(), env.team, env.victim.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill, Eviction: true})
	require.NoError(t, err)
	require.False(t, alreadyDone)

	env.arm()
	err = env.actor.Remove(t.Context(), env.team, env.victim.SandboxID, transition.Sandbox.ExecutionID)
	switch {
	case env.row.crash():
		require.ErrorIs(t, err, errSimulatedCrash)
	case env.row.point == faultRemoveAfterDelete:
		require.NoError(t, err)
	default:
		require.ErrorIs(t, err, errInjected)
	}
	if !env.row.crash() {
		done(t.Context(), nil)
	}

	requireIndexed(t, env.client, env.index(), env.victim.SandboxID, false)
	requireMemberScore(t, env.client, sandboxExpirationMember(env.victim), float64(env.victim.EndTime.UnixMilli()))
	if env.row.point == faultRemoveAfterSREM {
		requireRecord(t, env.client, key, mustJSON(t, transition.Sandbox))
		env.expectRepairs.DeletedStale = 1
	} else {
		requireNoRecord(t, env.client, key)
	}
}

// injectUpdate fails a keepalive of a sandbox the writer holds before its
// lock: nothing is written, no lock is taken and the error returned.
func (env *matrixEnv) injectUpdate(t *testing.T) {
	t.Helper()

	env.victim = env.record("sbx-victim", env.future)
	require.NoError(t, env.writer.Add(t.Context(), env.victim))
	env.original = env.victim
	env.lockIDs = append(env.lockIDs, env.victim.SandboxID)

	env.arm()
	_, err := env.keepalive(t, env.actor)
	require.ErrorIs(t, err, env.row.injected())
	requireRecord(t, env.client, recordKey(env.writer.writeForm(), env.team.String(), env.victim.SandboxID), mustJSON(t, env.victim))
}

func (env *matrixEnv) keepalive(t *testing.T, s *Storage) (sandboxtypes.Sandbox, error) {
	t.Helper()

	return s.Update(t.Context(), env.team, env.victim.SandboxID, func(sbx sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		sbx.EndTime = env.future.Add(time.Hour)

		return sbx, nil
	})
}

// injectReaders fails the readers' second pipeline, the team form's, under
// a reconcile and an expiration sweep. The population gives both a reason
// to run it: a sandbox the node reports that was never stored, and a member
// of an execution whose record is gone. The reconcile must report no orphan
// and the sweep must fail and sweep nothing.
func (env *matrixEnv) injectReaders(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	expired := env.lives[0]
	_, err := env.writer.Update(ctx, env.team, expired.SandboxID, func(sbx sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		sbx.EndTime = env.past

		return sbx, nil
	})
	require.NoError(t, err)
	gone := expirationMember(env.team.String(), "sbx-gone", uuid.NewString())
	require.NoError(t, env.client.ZAdd(ctx, globalExpirationSet, redis.Z{Score: float64(env.past.UnixMilli()), Member: gone}).Err())
	env.track("sbx-gone")
	env.victim = env.record("sbx-absent", env.future)
	env.original = env.victim

	env.arm()
	require.Empty(t, env.actor.Reconcile(ctx, env.report(slices.Concat(env.alive, []string{env.victim.SandboxID})...), "node"), "a failed second pipeline must abort the node, never report orphans")
	items, err := env.actor.ExpiredItems(ctx)
	require.ErrorIs(t, err, env.row.injected())
	require.Empty(t, items, "an aborted expired read returns no items beside its error")
	requireMemberScore(t, env.client, gone, float64(env.past.UnixMilli()))
	requireMemberScore(t, env.client, expirationMember(env.team.String(), expired.SandboxID, expired.ExecutionID), float64(env.past.UnixMilli()))
}

// injectHealer runs a sweep whose repairs the fault interrupts, over a
// nominee of every kind the walks find: a Running record older than the
// grace period without its index entry, in the column's form and in the
// flip column in both; an index entry that has dangled past the grace
// period; in the flip column a twin, one record per form; and the
// undecodable record. The interrupted sweep must repair nothing by halves;
// an error releases every lock, and a crash under the locks leaves them
// held.
func (env *matrixEnv) injectHealer(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	env.victim = env.record("sbx-unindexed", env.future)
	env.original = env.victim
	require.NoError(t, env.writer.Add(ctx, env.victim))
	require.NoError(t, env.client.SRem(ctx, env.index(), env.victim.SandboxID).Err())
	env.track("sbx-dangling")
	require.NoError(t, env.client.SAdd(ctx, env.index(), "sbx-dangling").Err())
	env.lockIDs = append(env.lockIDs, env.victim.SandboxID, "sbx-dangling")
	env.expectRepairs = healReport{Readded: 1, Removed: 1}
	nominees := []string{env.victim.SandboxID}
	if env.row.form == matrixFlip {
		other := env.record("sbx-unindexed-other", env.future)
		require.NoError(t, env.actor.Add(ctx, other))
		require.NoError(t, env.client.SRem(ctx, env.index(), other.SandboxID).Err())
		env.alive = append(env.alive, other.SandboxID)
		nominees = append(nominees, other.SandboxID)

		env.track("sbx-twin")
		env.twinOlder = makeIndexedSandbox(env.team, "sbx-twin", uuid.NewString(), time.Now().Add(-2*time.Hour), env.future)
		env.twinNewer = makeIndexedSandbox(env.team, "sbx-twin", uuid.NewString(), time.Now().Add(-time.Hour), env.future)
		writeRecordWithMember(t, env.client, formTeam, env.twinOlder)
		writeRecordWithMember(t, env.client, formSandbox, env.twinNewer)
		require.NoError(t, env.client.SAdd(ctx, env.index(), "sbx-twin").Err())
		env.alive = append(env.alive, "sbx-twin")
		nominees = append(nominees, "sbx-twin")
		env.lockIDs = append(env.lockIDs, other.SandboxID, "sbx-twin")
		env.expectRepairs = healReport{Readded: 2, Removed: 1, TwinsDeleted: 1}
	}

	env.ageDangling()
	env.arm()
	healed, report := runSweep(t, env.actor)
	require.Zero(t, healed)
	require.Zero(t, report.repairs(), "the interrupted sweep made a repair")
	requireIndexed(t, env.client, env.index(), env.victim.SandboxID, false)
	requireIndexed(t, env.client, env.index(), "sbx-dangling", true)
	for _, id := range nominees {
		_, exists, err := env.actor.formOfExisting(ctx, env.team.String(), id)
		require.NoError(t, err)
		require.Truef(t, exists, "the interrupted sweep deleted %s", id)
	}
	if env.row.form == matrixFlip {
		requireRecord(t, env.client, recordKey(formTeam, env.team.String(), "sbx-twin"), mustJSON(t, env.twinOlder))
		requireRecord(t, env.client, recordKey(formSandbox, env.team.String(), "sbx-twin"), mustJSON(t, env.twinNewer))
	}

	// A crash under the locks leaves every nominee's lock held: the form
	// its record is in, the write form for the dangling entry, both for
	// the twin.
	if env.row.crash() && env.row.point == faultHealerUnderLock {
		for _, id := range []string{env.victim.SandboxID, "sbx-dangling", garbageSandboxID} {
			env.heldOnCrash = append(env.heldOnCrash, env.resolvedLock(t, id))
		}
		if env.row.form == matrixFlip {
			teamLock, sandboxLock := lockKeys(env.team, "sbx-twin")
			env.heldOnCrash = append(env.heldOnCrash, env.resolvedLock(t, "sbx-unindexed-other"), teamLock, sandboxLock)
		}
	}
}

// ageDangling has the actor remember every tracked index entry as dangling
// since before the grace period, so the next sweep removes one that still
// has no record, as the first sweep past the grace period would.
func (env *matrixEnv) ageDangling() {
	since := make(map[recordRef]time.Time, len(env.refs))
	for _, ref := range env.refs {
		since[ref] = time.Now().Add(-2 * healGracePeriod)
	}
	env.actor.danglingSince = since
}

// checkLocks asserts that the fault left exactly the locks a crash leaves
// held, in both forms of every sandbox the operation may lock, and expires
// the held ones as their TTL would once the crashed replica is gone.
func (env *matrixEnv) checkLocks(t *testing.T) {
	t.Helper()

	for _, id := range env.lockIDs {
		teamLock, sandboxLock := lockKeys(env.team, id)
		for _, key := range []string{teamLock, sandboxLock} {
			if !slices.Contains(env.heldOnCrash, key) {
				require.Falsef(t, lockHeld(t, env.actor.locker, key), "lock %s is held after the fault", key)

				continue
			}
			require.Truef(t, lockHeld(t, env.actor.locker, key), "lock %s is not held after the crash", key)
			require.NoError(t, env.client.Del(t.Context(), key).Err())
		}
	}
}

// assertSweep checks that the sweep after the fault repaired exactly what
// the family's remains call for, re-added no expiration member, and took
// a complete census.
func (env *matrixEnv) assertSweep(t *testing.T, healed int, report healReport) {
	t.Helper()

	require.Zero(t, healed, "the sweep re-added an expiration member")
	require.Equal(t, env.expectRepairs, repairsOf(report))
	require.True(t, report.Complete)
}

// assertHealed checks the row's properties after the sweep: what the family
// left is as the design says; no sandbox a node reports is an orphan; a
// resume of an ID whose record is gone is admitted and whole; a removal
// naming a stale execution is refused and touches nothing; every sandbox
// whose VM exists is served, listed and counted, nothing dead is listed and
// nothing non-Running is listed as Running; the undecodable record is never
// swept; the invariants hold; and no lock is left held.
func (env *matrixEnv) assertHealed(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	env.assertFamilyHealed(t)

	_, alive, err := env.actor.formOfExisting(ctx, env.team.String(), env.victim.SandboxID)
	require.NoError(t, err)
	reported := env.alive
	if alive {
		reported = slices.Concat(env.alive, []string{env.victim.SandboxID})
	}
	require.Empty(t, env.actor.Reconcile(ctx, env.report(reported...), "node"), "a sandbox whose record exists was reported as an orphan")

	if !alive {
		env.victim.ExecutionID = uuid.NewString()
		env.victim.EndTime = env.future
		require.NoError(t, env.actor.Add(ctx, env.victim), "a resume of an ID whose record is gone was refused")
	}
	env.alive = append(env.alive, env.victim.SandboxID)

	stale := env.original.ExecutionID
	if stale == env.victim.ExecutionID {
		stale = uuid.NewString()
	}
	require.ErrorIs(t, env.actor.Remove(ctx, env.team, env.victim.SandboxID, stale), sandboxtypes.ErrExecutionMismatch)

	sbx, err := env.actor.Get(ctx, env.team, env.victim.SandboxID)
	require.NoError(t, err)
	require.Equal(t, sandboxtypes.StateRunning, sbx.State)
	require.Equal(t, env.victim.ExecutionID, sbx.ExecutionID)
	for _, s := range []*Storage{env.writer, env.actor} {
		running, err := s.TeamItems(ctx, env.team, []sandboxtypes.State{sandboxtypes.StateRunning})
		require.NoError(t, err)
		for _, item := range running {
			require.Equalf(t, sandboxtypes.StateRunning, item.State, "%s listed as Running", item.SandboxID)
		}
		require.ElementsMatch(t, env.alive, sandboxIDsOf(running), "the Running listing")
		all, err := s.TeamItems(ctx, env.team, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, env.alive, sandboxIDsOf(all), "the unfiltered listing")
	}
	counts, err := env.actor.TeamsWithSandboxCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len(env.alive)), counts[env.team])
	expired, err := env.actor.ExpiredItems(ctx)
	require.NoError(t, err)
	for _, item := range expired {
		require.Truef(t, item.IsExpired(time.Now()), "%s returned as expired", item.SandboxID)
	}
	require.NoError(t, env.client.ZScore(ctx, globalExpirationSet, env.garbageMember).Err(), "the undecodable record's member was swept")
	if env.original.ExecutionID != env.victim.ExecutionID {
		requireMemberScore(t, env.client, sandboxExpirationMember(env.victim), float64(env.victim.EndTime.UnixMilli()))
	}

	assertInvariants(t, env.actor, env.refs)
	for _, id := range env.lockIDs {
		requireLocksFree(t, env.actor, env.team, id)
	}
}

// assertFamilyHealed checks what the sweep made of the family's remains.
func (env *matrixEnv) assertFamilyHealed(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	switch familyOf(env.row.point) {
	case familyRemove:
		// The stranded record of a dead execution is gone with its entry;
		// its member goes with the record, or with the evictor's sweep.
		_, exists, err := env.actor.formOfExisting(ctx, env.team.String(), env.victim.SandboxID)
		require.NoError(t, err)
		require.False(t, exists, "the dead execution's record remains")
		requireIndexed(t, env.client, env.index(), env.victim.SandboxID, false)
		_, err = env.actor.ExpiredItems(ctx)
		require.NoError(t, err)
		requireMemberAbsent(t, env.client, sandboxExpirationMember(env.victim))
	case familyUpdate:
		_, err := env.keepalive(t, env.actor)
		require.NoError(t, err)
	case familyReaders:
		// With the readers whole again the reconcile reports exactly the
		// sandbox that was never stored, and the sweep returns the expired
		// sandbox and removes the member whose record is gone.
		orphans := env.actor.Reconcile(ctx, env.report(slices.Concat(env.alive, []string{env.victim.SandboxID})...), "node")
		require.Equal(t, []string{env.victim.SandboxID}, sandboxIDsOfNodes(orphans))
		expired, err := env.actor.ExpiredItems(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{env.alive[0]}, sandboxIDsOf(expired))
		members, err := env.client.ZRangeByScore(ctx, globalExpirationSet, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
		require.NoError(t, err)
		for _, member := range members {
			require.NotContains(t, member, "sbx-gone", "the member whose record is gone was not swept")
		}
	case familyHealer:
		requireIndexed(t, env.client, env.index(), env.victim.SandboxID, true)
		requireIndexed(t, env.client, env.index(), "sbx-dangling", false)
		if env.row.form == matrixFlip {
			requireIndexed(t, env.client, env.index(), "sbx-unindexed-other", true)
			requireNoRecord(t, env.client, recordKey(formTeam, env.team.String(), "sbx-twin"))
			requireRecord(t, env.client, recordKey(formSandbox, env.team.String(), "sbx-twin"), mustJSON(t, env.twinNewer))
			requireIndexed(t, env.client, env.index(), "sbx-twin", true)
		}
	}
}

func sandboxIDsOfNodes(nodes []sandboxtypes.NodeSandbox) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.SandboxID)
	}

	return ids
}

// Every fault point, as a returned error and as a crash, in the team form,
// the sandbox form and the middle of a flip: the fault leaves the state and
// the locks the design admits; after the crashed locks expire, one healer
// sweep repairs exactly what the design says, no sandbox a node reports is
// an orphan, a resume of an ID whose record is gone is admitted, a stale
// removal is refused, nothing non-Running or dead is listed, and the
// invariants hold.
// The suite runs before the package's parallel tests: its load on the shared
// server starves their pub/sub and lock timing windows on a loaded runner.
//
//nolint:paralleltest,tparallel // see above; its rows still run in parallel with each other
func TestFaultMatrix(t *testing.T) {
	points := []faultPoint{
		faultAddAfterLocks, faultAddBeforeOtherDel, faultAddAfterOtherDel, faultAddRecordWrite, faultAddAfterRecordWrite, faultAddAfterIndex,
		faultRemoveAfterSREM, faultRemoveAfterDelete,
		faultUpdateAfterResolve,
		faultReadersSecondPipeline,
		faultHealerAfterNominate, faultHealerUnderLock,
	}
	for _, point := range points {
		for _, kind := range []matrixKind{matrixError, matrixCrash} {
			for _, form := range []matrixForm{matrixTeam, matrixSandbox, matrixFlip} {
				row := matrixRow{point: point, kind: kind, form: form}
				t.Run(row.String(), func(t *testing.T) {
					t.Parallel()
					if reason := skipReason(row.point, row.form); reason != "" {
						t.Skip(reason)
					}

					env := newMatrixEnv(t, row)
					env.inject(t)
					env.disarm()
					require.Positive(t, env.fired, "the fault point never fired")
					env.checkLocks(t)

					env.ageDangling()
					healed, report := runSweep(t, env.actor)
					env.assertSweep(t, healed, report)

					env.assertHealed(t)
				})
			}
		}
	}
}
