package redis

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// The flip store runs as two instances on one server whose flips are a few
// operations apart and which take the operations in turn, as replicas whose
// flag reads lag do. Every operation is checked against the in-memory model
// as it runs, so a divergence names the seed, the operation and the store.

const (
	differentialOperations = 500
	differentialTeams      = 2
	differentialSandboxes  = 20

	// differentialFlipAt is the operation before which the flip store's
	// first instance flips to the sandbox form; its second instance flips
	// differentialFlipLag operations later.
	differentialFlipAt  = 250
	differentialFlipLag = 10

	// differentialHold keeps a refused pause's sandbox out of the expiration
	// sweep past the end of the run, so no operation sees the hold pass.
	differentialHold = time.Hour

	// differentialFar is how far from the run every EndTime lies, so that
	// the wall clock never crosses one while the three stores replay.
	differentialFar = time.Hour

	// garbageSandboxID names a sandbox-form record that does not decode,
	// seeded with an expiration member and no index entry in every store:
	// the readers skip it, so it is never listed, never an orphan and never
	// swept, and the healer nominates it every sweep and repairs nothing.
	garbageSandboxID = "sbx-garbage"
)

// differentialSeeds seed the generator; a failure names the seed.
var differentialSeeds = []uint64{1, 2, 3}

var errNodeFailed = errors.New("node failed")

type differentialKind string

const (
	kindAdd         differentialKind = "add"       // a create, or a resume after a pause completed
	kindKeepalive   differentialKind = "keepalive" // EndTime moved into the future
	kindTimeout     differentialKind = "timeout"   // EndTime set into the past or the future
	kindPause       differentialKind = "pause"     // with every ending the removal flow has
	kindKill        differentialKind = "kill"
	kindSnapshot    differentialKind = "snapshot"     // a transient transition, restored to Running by its callback
	kindStaleRemove differentialKind = "stale remove" // Remove naming an execution that was removed before
	kindEvict       differentialKind = "evict"        // what ExpiredItems returns is evicted
	kindList        differentialKind = "list"
	kindReconcile   differentialKind = "reconcile"
	kindCount       differentialKind = "count"
	kindGet         differentialKind = "get"
	kindHeal        differentialKind = "heal"    // one healer sweep, which has nothing to repair
	kindForeign     differentialKind = "foreign" // a keepalive naming another team's ID, which finds nothing
)

// transitionEnd is how a transition ends, as the removal flow ends it: the
// node completed it and the record is removed; the node refused a pause and
// the record went back to Running, with or without a retry hold; the node
// failed it, which removes the record like a kill; or, for a snapshot, the
// callback put the record back to Running.
type transitionEnd string

const (
	endCompleted   transitionEnd = "completed"
	endRefusedHeld transitionEnd = "refused and held"
	endRefused     transitionEnd = "refused"
	endFailed      transitionEnd = "failed"
	endRestored    transitionEnd = "restored"
)

// waitTrace is what a waiter on the transition reports once it ends.
func (e transitionEnd) waitTrace() string {
	switch e {
	case endRefusedHeld, endRefused:
		return (&sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.PauseQueueExhaustedError{}}).Error()
	case endFailed:
		return "transition failed: " + errNodeFailed.Error()
	default:
		return "ok"
	}
}

// nodeReport is one sandbox a node reports to a reconcile; a fresh one is
// younger than the orphan grace period and never judged.
type nodeReport struct {
	ref   recordRef
	fresh bool
}

// differentialOp is one generated operation, with everything it writes and
// the trace the model expects it to produce.
type differentialOp struct {
	index  int
	kind   differentialKind
	ref    recordRef
	exec   string
	end    time.Time
	finish transitionEnd
	report []nodeReport
	expect string
}

func (op differentialOp) String() string {
	switch op.kind {
	case kindAdd, kindStaleRemove:
		return fmt.Sprintf("%s %s, execution %s", op.kind, op.ref.SandboxID, op.exec)
	case kindKeepalive, kindTimeout:
		return fmt.Sprintf("%s %s, end %s", op.kind, op.ref.SandboxID, op.end.Format(time.RFC3339))
	case kindPause:
		return fmt.Sprintf("pause %s, %s", op.ref.SandboxID, op.finish)
	case kindKill, kindSnapshot, kindGet, kindForeign:
		return fmt.Sprintf("%s %s", op.kind, op.ref.SandboxID)
	case kindReconcile:
		return fmt.Sprintf("reconcile, %d reported", len(op.report))
	default:
		return string(op.kind)
	}
}

// listFilter is one state filter a listing is traced with.
type listFilter struct {
	name   string
	states []sandboxtypes.State
}

var (
	listFilters = []listFilter{
		{"all", nil},
		{"running", []sandboxtypes.State{sandboxtypes.StateRunning}},
		{"pausing", []sandboxtypes.State{sandboxtypes.StatePausing}},
		{"killing", []sandboxtypes.State{sandboxtypes.StateKilling}},
		{"snapshotting", []sandboxtypes.State{sandboxtypes.StateSnapshotting}},
		{"running+pausing", []sandboxtypes.State{sandboxtypes.StateRunning, sandboxtypes.StatePausing}},
	}
	transitionFilters = []listFilter{
		{"all", nil},
		{"running", []sandboxtypes.State{sandboxtypes.StateRunning}},
		{"transitional", []sandboxtypes.State{sandboxtypes.StatePausing, sandboxtypes.StateKilling, sandboxtypes.StateSnapshotting}},
	}
	staleRemoveFilters = listFilters[:2]
)

// listed is one sandbox a listing shows.
type listed struct {
	id    string
	state sandboxtypes.State
}

func listedOf(items []sandboxtypes.Sandbox) []listed {
	out := make([]listed, 0, len(items))
	for _, sbx := range items {
		out = append(out, listed{id: sbx.SandboxID, state: sbx.State})
	}

	return out
}

func formatListed(items []listed) string {
	slices.SortFunc(items, func(a, b listed) int { return strings.Compare(a.id, b.id) })
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, item.id+":"+string(item.state))
	}

	return "[" + strings.Join(parts, " ") + "]"
}

func formatIDs(ids []string) string {
	slices.Sort(ids)

	return "[" + strings.Join(ids, " ") + "]"
}

// modelSandbox is what the model holds for a sandbox the store has a record
// of.
type modelSandbox struct {
	exec  string
	end   time.Time
	state sandboxtypes.State
	// refusedUntil is the retry hold the record keeps from its last refused
	// and held pause; held is set while a hold has moved the expiration
	// member past the run. An EndTime change scores the member again, and
	// the restore of a later refused pause applies the kept hold again when
	// it still lies past the EndTime, as the store's does.
	refusedUntil time.Time
	held         bool
}

// differentialModel is the in-memory oracle the stores are checked against.
type differentialModel struct {
	base  time.Time
	teams []uuid.UUID
	// ids[team] are the team's sandbox IDs, unique across teams: the sandbox
	// form's key carries no team.
	ids       [][]string
	sandboxes map[recordRef]*modelSandbox
	// removed is the execution each sandbox's record was last removed with.
	removed map[recordRef]string
	// garbage is the undecodable record every store is seeded with.
	garbage recordRef
}

func newDifferentialModel(base time.Time, teams []uuid.UUID, ids [][]string) *differentialModel {
	return &differentialModel{
		base:      base,
		teams:     teams,
		ids:       ids,
		sandboxes: make(map[recordRef]*modelSandbox),
		removed:   make(map[recordRef]string),
		garbage:   recordRef{TeamID: teams[0].String(), SandboxID: garbageSandboxID},
	}
}

// refs returns every sandbox the run can touch, in a fixed order.
func (m *differentialModel) refs() []recordRef {
	refs := make([]recordRef, 0, len(m.teams)*len(m.ids[0]))
	for team, ids := range m.ids {
		for _, id := range ids {
			refs = append(refs, recordRef{TeamID: m.teams[team].String(), SandboxID: id})
		}
	}

	return refs
}

func (m *differentialModel) teamIndex(teamID string) int {
	for i, team := range m.teams {
		if team.String() == teamID {
			return i
		}
	}

	panic("unknown team " + teamID)
}

func (m *differentialModel) present(ref recordRef) bool {
	_, ok := m.sandboxes[ref]

	return ok
}

// pickRef returns a random sandbox for which pick holds, and false when
// none does.
func (m *differentialModel) pickRef(rng *rand.Rand, pick func(recordRef) bool) (recordRef, bool) {
	var candidates []recordRef
	for _, ref := range m.refs() {
		if pick(ref) {
			candidates = append(candidates, ref)
		}
	}
	if len(candidates) == 0 {
		return recordRef{}, false
	}

	return candidates[rng.IntN(len(candidates))], true
}

func (m *differentialModel) absent(ref recordRef) bool {
	return !m.present(ref)
}

func (m *differentialModel) removedBefore(ref recordRef) bool {
	return m.removed[ref] != ""
}

// pickKind draws an operation kind by weight, falling back to one whose
// target exists.
func (m *differentialModel) pickKind(rng *rand.Rand) differentialKind {
	var kind differentialKind
	switch draw := rng.IntN(100); {
	case draw < 24:
		kind = kindAdd
	case draw < 34:
		kind = kindKeepalive
	case draw < 46:
		kind = kindTimeout
	case draw < 58:
		kind = kindPause
	case draw < 64:
		kind = kindKill
	case draw < 70:
		kind = kindEvict
	case draw < 74:
		kind = kindSnapshot
	case draw < 78:
		kind = kindStaleRemove
	case draw < 84:
		kind = kindList
	case draw < 86:
		kind = kindForeign
	case draw < 92:
		kind = kindReconcile
	case draw < 95:
		kind = kindCount
	case draw < 98:
		kind = kindGet
	default:
		kind = kindHeal
	}

	_, anyAbsent := m.pickRef(rng, m.absent)
	_, anyPresent := m.pickRef(rng, m.present)
	_, anyRemoved := m.pickRef(rng, m.removedBefore)
	if kind == kindStaleRemove && !anyRemoved {
		kind = kindAdd
	}
	switch {
	case kind == kindAdd && !anyAbsent:
		return kindKill
	case (kind == kindKeepalive || kind == kindTimeout || kind == kindPause || kind == kindKill || kind == kindSnapshot || kind == kindForeign) && !anyPresent:
		return kindAdd
	}

	return kind
}

func (m *differentialModel) futureEnd(rng *rand.Rand) time.Time {
	return m.base.Add(differentialFar + time.Duration(rng.IntN(60))*time.Minute)
}

func (m *differentialModel) pastEnd(rng *rand.Rand) time.Time {
	return m.base.Add(-differentialFar - time.Duration(rng.IntN(60))*time.Minute)
}

// generate draws the next operation, applies it to the model and records
// the trace the model expects.
func (m *differentialModel) generate(rng *rand.Rand, index int) differentialOp {
	op := differentialOp{index: index, kind: m.pickKind(rng)}
	switch op.kind {
	case kindAdd:
		op.ref, _ = m.pickRef(rng, m.absent)
		op.exec = randomUUID(rng).String()
		op.end = m.futureEnd(rng)
		m.sandboxes[op.ref] = &modelSandbox{exec: op.exec, end: op.end, state: sandboxtypes.StateRunning}
		op.expect = "added"
	case kindKeepalive:
		op.ref, _ = m.pickRef(rng, m.present)
		op.end = m.futureEnd(rng)
		m.update(op.ref, op.end)
		op.expect = "updated"
	case kindTimeout:
		op.ref, _ = m.pickRef(rng, m.present)
		op.end = m.futureEnd(rng)
		if rng.IntN(2) == 0 {
			op.end = m.pastEnd(rng)
		}
		m.update(op.ref, op.end)
		op.expect = "updated"
	case kindPause:
		op.ref, _ = m.pickRef(rng, m.present)
		op.finish = []transitionEnd{endCompleted, endRefusedHeld, endRefused, endFailed}[rng.IntN(4)]
		op.expect = m.transition(op.ref, sandboxtypes.StatePausing, op.finish)
	case kindKill:
		op.ref, _ = m.pickRef(rng, m.present)
		op.finish = endCompleted
		op.expect = m.transition(op.ref, sandboxtypes.StateKilling, endCompleted)
	case kindSnapshot:
		op.ref, _ = m.pickRef(rng, m.present)
		op.finish = endRestored
		op.expect = m.transition(op.ref, sandboxtypes.StateSnapshotting, endRestored)
	case kindStaleRemove:
		op.ref, _ = m.pickRef(rng, m.removedBefore)
		op.exec = m.removed[op.ref]
		op.expect = m.staleRemoveTrace(op.ref)
	case kindEvict:
		op.expect = "expired=" + formatIDs(m.expiredIDs()) + " evicted"
		for _, ref := range m.refs() {
			if m.expired(ref) {
				m.remove(ref)
			}
		}
	case kindList:
		op.expect = m.listTrace()
	case kindReconcile:
		op.report = m.randomReport(rng)
		op.expect = "orphans=" + m.orphanIDs(op.report)
	case kindCount:
		op.expect = m.countTrace()
	case kindGet:
		refs := m.refs()
		op.ref = refs[rng.IntN(len(refs))]
		op.expect = m.getTrace(op.ref)
	case kindHeal:
		op.expect = healTrace(0, healReport{}, m.records(), true)
	case kindForeign:
		op.ref, _ = m.pickRef(rng, m.present)
		op.expect = "foreign=notfound " + m.listTrace()
	}

	return op
}

// records is how many records carry an expiration member: every sandbox
// the store holds, and the undecodable one.
func (m *differentialModel) records() int {
	return len(m.sandboxes) + 1
}

func (m *differentialModel) update(ref recordRef, end time.Time) {
	sbx := m.sandboxes[ref]
	if sbx.end.Equal(end) {
		return
	}
	sbx.end = end
	sbx.held = false
}

// remove forgets the sandbox's record and remembers the execution removed.
func (m *differentialModel) remove(ref recordRef) {
	m.removed[ref] = m.sandboxes[ref].exec
	delete(m.sandboxes, ref)
}

// transition traces the sandbox while its transition is open, then ends it
// as finish says.
func (m *differentialModel) transition(ref recordRef, target sandboxtypes.State, finish transitionEnd) string {
	sbx := m.sandboxes[ref]
	sbx.state = target
	trace := fmt.Sprintf("%s get=%s %s orphans=[]", target, m.getTrace(ref), m.teamListTrace(m.teamIndex(ref.TeamID), transitionFilters))
	switch finish {
	case endCompleted, endFailed:
		m.remove(ref)
	case endRefusedHeld:
		sbx.state = sandboxtypes.StateRunning
		sbx.refusedUntil = m.base.Add(differentialHold)
		sbx.held = true
	case endRefused:
		sbx.state = sandboxtypes.StateRunning
		sbx.held = sbx.refusedUntil.After(sbx.end)
	case endRestored:
		sbx.state = sandboxtypes.StateRunning
	}

	return trace + " wait=" + finish.waitTrace()
}

// staleRemoveTrace is what a Remove naming a removed execution shows: a
// mismatch while a later execution's record exists, a success when none
// does, and in both cases nothing changed.
func (m *differentialModel) staleRemoveTrace(ref recordRef) string {
	result := "ok"
	if m.present(ref) {
		result = "mismatch"
	}

	return fmt.Sprintf("stale remove=%s %s get=%s", result, m.teamListTrace(m.teamIndex(ref.TeamID), staleRemoveFilters), m.getTrace(ref))
}

// expired reports whether the expiration sweep returns the sandbox: its
// member's score has passed and no hold moved it.
func (m *differentialModel) expired(ref recordRef) bool {
	sbx, ok := m.sandboxes[ref]

	return ok && !sbx.held && sbx.end.Before(m.base)
}

func (m *differentialModel) expiredIDs() []string {
	var ids []string
	for _, ref := range m.refs() {
		if m.expired(ref) {
			ids = append(ids, ref.SandboxID)
		}
	}

	return ids
}

func (m *differentialModel) teamListTrace(team int, filters []listFilter) string {
	parts := make([]string, 0, len(filters))
	for _, filter := range filters {
		var items []listed
		for _, id := range m.ids[team] {
			sbx, ok := m.sandboxes[recordRef{TeamID: m.teams[team].String(), SandboxID: id}]
			if !ok || (len(filter.states) > 0 && !slices.Contains(filter.states, sbx.state)) {
				continue
			}
			items = append(items, listed{id: id, state: sbx.state})
		}
		parts = append(parts, filter.name+"="+formatListed(items))
	}

	return strings.Join(parts, " ")
}

func (m *differentialModel) listTrace() string {
	parts := make([]string, 0, len(m.teams))
	for team := range m.teams {
		parts = append(parts, fmt.Sprintf("t%d: %s", team, m.teamListTrace(team, listFilters)))
	}

	return strings.Join(parts, " | ")
}

func (m *differentialModel) countTrace() string {
	var parts []string
	for team, ids := range m.ids {
		count := 0
		for _, id := range ids {
			if m.present(recordRef{TeamID: m.teams[team].String(), SandboxID: id}) {
				count++
			}
		}
		if count > 0 {
			parts = append(parts, fmt.Sprintf("t%d=%d", team, count))
		}
	}

	return "counts=[" + strings.Join(parts, " ") + "]"
}

// orphanIDs returns the reported sandboxes a reconcile reports as orphans:
// the judged ones without a record. The undecodable record is skipped, not
// absent.
func (m *differentialModel) orphanIDs(report []nodeReport) string {
	var ids []string
	for _, entry := range report {
		if !entry.fresh && !m.present(entry.ref) && entry.ref != m.garbage {
			ids = append(ids, entry.ref.SandboxID)
		}
	}

	return formatIDs(ids)
}

func (m *differentialModel) getTrace(ref recordRef) string {
	sbx, ok := m.sandboxes[ref]
	if !ok {
		return "notfound"
	}

	return string(sbx.state) + "/" + sbx.exec
}

// randomReport reports every other sandbox, present or not, one in eight of
// them too fresh to be judged, and the undecodable record.
func (m *differentialModel) randomReport(rng *rand.Rand) []nodeReport {
	var report []nodeReport
	for _, ref := range m.refs() {
		if rng.IntN(2) == 0 {
			report = append(report, nodeReport{ref: ref, fresh: rng.IntN(8) == 0})
		}
	}

	return append(report, nodeReport{ref: m.garbage})
}

// fixedReport reports every sandbox but each third, old enough to be
// judged, and the undecodable record.
func (m *differentialModel) fixedReport() []nodeReport {
	var report []nodeReport
	for i, ref := range m.refs() {
		if i%3 != 0 {
			report = append(report, nodeReport{ref: ref})
		}
	}

	return append(report, nodeReport{ref: m.garbage})
}

// projectionTrace is the observable projection of the model: every listing,
// the counts, the expired sandboxes and the orphans of the fixed report.
func (m *differentialModel) projectionTrace() string {
	return strings.Join([]string{m.listTrace(), m.countTrace(), "expired=" + formatIDs(m.expiredIDs()), "orphans=" + m.orphanIDs(m.fixedReport())}, " | ")
}

// healTrace is what one healer sweep shows: the expiration members it
// re-added, its repairs, how many records its census found, and whether it
// was complete.
func healTrace(healed int, repairs healReport, records int, complete bool) string {
	return fmt.Sprintf("healed=%d readded=%d removed=%d stale=%d twins=%d records=%d complete=%t",
		healed, repairs.Readded, repairs.Removed, repairs.DeletedStale, repairs.TwinsDeleted, records, complete)
}

// randomUUID draws a UUID from rng, so every store of a seed sees the same
// IDs.
func randomUUID(rng *rand.Rand) uuid.UUID {
	var id uuid.UUID
	binary.BigEndian.PutUint64(id[:8], rng.Uint64())
	binary.BigEndian.PutUint64(id[8:], rng.Uint64())
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80

	return id
}

// differentialCase is one seed's generated run: its teams and sandboxes,
// the undecodable record every store is seeded with, the operations with
// the traces the model expects, and the projection the model expects at
// the end.
type differentialCase struct {
	seed       uint64
	teams      []uuid.UUID
	refs       []recordRef
	garbage    recordRef
	ops        []differentialOp
	report     []nodeReport
	projection string
}

func generateDifferential(seed uint64) differentialCase {
	rng := rand.New(rand.NewPCG(seed, 0))
	teams := make([]uuid.UUID, differentialTeams)
	ids := make([][]string, differentialTeams)
	for team := range teams {
		teams[team] = randomUUID(rng)
		for i := range differentialSandboxes {
			ids[team] = append(ids[team], fmt.Sprintf("sbx-t%d-%02d", team, i))
		}
	}
	model := newDifferentialModel(time.Now(), teams, ids)
	ops := make([]differentialOp, 0, differentialOperations)
	for i := range differentialOperations {
		ops = append(ops, model.generate(rng, i))
	}

	return differentialCase{
		seed:       seed,
		teams:      teams,
		refs:       model.refs(),
		garbage:    model.garbage,
		ops:        ops,
		report:     model.fixedReport(),
		projection: model.projectionTrace(),
	}
}

// differentialRun is one store under the differential: one instance, or
// two on one server that take the operations in turn and flip at different
// operations.
type differentialRun struct {
	name      string
	instances []*Storage
	// flipAt is the operation before which each instance flips to the
	// sandbox form; -1 never.
	flipAt []int
}

func newDifferentialRun(t *testing.T, name string, garbage recordRef) differentialRun {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	writeGarbageRecord(t, client, garbage)
	instance := func() *Storage {
		s := newTestStorage(t, client)
		startNotifications(t, s)

		return s
	}
	switch name {
	case "team":
		return differentialRun{name: name, instances: []*Storage{instance()}, flipAt: []int{-1}}
	case "sandbox":
		s := instance()
		s.SetPerSandboxWrites(true)

		return differentialRun{name: name, instances: []*Storage{s}, flipAt: []int{-1}}
	default:
		return differentialRun{name: name, instances: []*Storage{instance(), instance()}, flipAt: []int{differentialFlipAt, differentialFlipAt + differentialFlipLag}}
	}
}

// writeGarbageRecord stores a value that does not decode under the
// sandbox-form key of ref, with an expiration member whose score has
// passed and no index entry, and returns the member.
func writeGarbageRecord(t *testing.T, client redis.UniversalClient, ref recordRef) string {
	t.Helper()

	require.NoError(t, client.Set(t.Context(), recordKey(formSandbox, ref.TeamID, ref.SandboxID), "not json", 0).Err())
	member := expirationMember(ref.TeamID, ref.SandboxID, uuid.NewString())
	require.NoError(t, client.ZAdd(t.Context(), globalExpirationSet, redis.Z{Score: float64(time.Now().Add(-differentialFar).UnixMilli()), Member: member}).Err())

	return member
}

// actors returns the instance that runs an operation and the one that
// observes it meanwhile; one instance plays both.
func (r differentialRun) actors(index int) (actor, other *Storage) {
	return r.instances[index%len(r.instances)], r.instances[(index+1)%len(r.instances)]
}

func (r differentialRun) flip(index int) {
	for i, at := range r.flipAt {
		if at == index {
			r.instances[i].SetPerSandboxWrites(true)
		}
	}
}

// Three stores that differ only in their write forms, and a model, show the
// same sandboxes to every reader through a generated sequence of creates,
// keepalives, timeout changes, pauses with every ending, kills, snapshots,
// removes naming a removed execution, evictions, listings, keepalives
// naming another team, reconciles, counts, reads and healer sweeps, waiters
// included; the flip store's two instances flip ten operations apart.
// The suite runs before the package's parallel tests: its load on the shared
// server starves their pub/sub and lock timing windows on a loaded runner.
//
//nolint:paralleltest,tparallel // see above; its seeds still run in parallel with each other
func TestDifferentialOldNewAndFlip(t *testing.T) {
	for _, seed := range differentialSeeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()

			c := generateDifferential(seed)
			projections := make(map[string]string)
			for _, name := range []string{"team", "sandbox", "flip"} {
				run := newDifferentialRun(t, name, c.garbage)
				projections[name] = runDifferential(t, c, run)
			}

			require.Equalf(t, projections["team"], projections["sandbox"], "seed %d: the team-form and sandbox-form stores differ", seed)
			require.Equalf(t, projections["team"], projections["flip"], "seed %d: the team-form and flipped stores differ", seed)
			require.Equalf(t, c.projection, projections["team"], "seed %d: the stores differ from the model", seed)
		})
	}
}

// runDifferential replays the case's operations on the run, checking every
// operation's trace against the model's, and returns the run's final
// projection, which every instance of the run must show alike.
func runDifferential(t *testing.T, c differentialCase, run differentialRun) string {
	t.Helper()

	for _, op := range c.ops {
		run.flip(op.index)
		actor, other := run.actors(op.index)
		got := applyDifferentialOp(t, actor, other, c.teams, op)
		require.Equalf(t, op.expect, got, "seed %d, operation %d (%s) on the %s store", c.seed, op.index, op, run.name)
	}

	var projection string
	for i, instance := range run.instances {
		assertInvariants(t, instance, c.refs)
		seen := projectionTrace(t, instance, c.teams, c.report)
		if i > 0 {
			require.Equalf(t, projection, seen, "seed %d: the %s store's instances differ", c.seed, run.name)
		}
		projection = seen
	}

	return projection
}

func applyDifferentialOp(t *testing.T, actor, other *Storage, teams []uuid.UUID, op differentialOp) string {
	t.Helper()

	ctx := t.Context()
	switch op.kind {
	case kindAdd:
		sbx := makeIndexedSandbox(uuid.MustParse(op.ref.TeamID), op.ref.SandboxID, op.exec, time.Now().Add(-2*healGracePeriod), op.end)
		if err := actor.Add(ctx, sbx); err != nil {
			return "add error: " + err.Error()
		}

		return "added"
	case kindKeepalive, kindTimeout:
		_, err := actor.Update(ctx, uuid.MustParse(op.ref.TeamID), op.ref.SandboxID, func(sbx sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
			sbx.EndTime = op.end

			return sbx, nil
		})
		if err != nil {
			return "update error: " + err.Error()
		}

		return "updated"
	case kindPause:
		return runTransition(t, actor, other, op.ref, sandboxtypes.StateActionPause, op.finish)
	case kindKill:
		return runTransition(t, actor, other, op.ref, sandboxtypes.StateActionKill, endCompleted)
	case kindSnapshot:
		return runTransition(t, actor, other, op.ref, sandboxtypes.StateActionSnapshot, endRestored)
	case kindStaleRemove:
		return staleRemoveTrace(t, actor, op.ref, op.exec)
	case kindEvict:
		return evictTrace(t, actor)
	case kindList:
		return listTrace(t, actor, teams)
	case kindReconcile:
		return "orphans=" + orphanIDs(t, actor, op.report)
	case kindCount:
		return countTrace(t, actor, teams)
	case kindGet:
		return getTrace(t, actor, op.ref)
	case kindHeal:
		healed, walk, err := actor.healExpirationIndex(ctx)
		if err != nil {
			return "heal error: " + err.Error()
		}
		report, err := actor.healIndexConsistency(ctx, walk)
		if err != nil {
			return "heal error: " + err.Error()
		}

		return healTrace(healed, report, report.TeamForm+report.SandboxForm, report.Complete)
	case kindForeign:
		return foreignTrace(t, actor, teams, op.ref)
	}

	return "unknown operation"
}

// foreignTrace addresses the sandbox with the other team's ID, as a
// keepalive would, and shows the result and every listing after: the
// sandbox form's key carries no team, so only the record's team can refuse
// the caller.
func foreignTrace(t *testing.T, s *Storage, teams []uuid.UUID, ref recordRef) string {
	t.Helper()

	foreign := teams[0]
	if foreign.String() == ref.TeamID {
		foreign = teams[1]
	}
	result := "updated"
	switch _, err := s.Update(t.Context(), foreign, ref.SandboxID, func(sbx sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		sbx.EndTime = sbx.EndTime.Add(time.Hour)

		return sbx, nil
	}); {
	case errors.Is(err, sandboxtypes.ErrNotFound):
		result = "notfound"
	case err != nil:
		result = "error: " + err.Error()
	}

	return "foreign=" + result + " " + listTrace(t, s, teams)
}

// runTransition starts a transition on actor and, while it is open, reads,
// lists and reconciles the sandbox through other, whose waiter is registered
// before the transition ends the way finish says. The trace carries all of
// it and the waiter's result.
func runTransition(t *testing.T, actor, other *Storage, ref recordRef, action sandboxtypes.StateAction, finish transitionEnd) string {
	t.Helper()

	ctx := t.Context()
	team := uuid.MustParse(ref.TeamID)
	transition, alreadyDone, done, err := actor.StartRemoving(ctx, team, ref.SandboxID, sandboxtypes.RemoveOpts{Action: action})
	if err != nil {
		return "start error: " + err.Error()
	}
	if alreadyDone {
		return "start: already done"
	}

	waiter := make(chan error, 1)
	go func() { waiter <- other.WaitForStateChange(ctx, team, ref.SandboxID) }()
	registerTransitionWaiter(t, other, ref)

	trace := fmt.Sprintf("%s get=%s %s orphans=%s", action.TargetState, getTrace(t, other, ref), teamListTrace(t, other, team, transitionFilters), orphanIDs(t, other, []nodeReport{{ref: ref}}))

	switch finish {
	case endCompleted:
		err = actor.Remove(ctx, team, ref.SandboxID, transition.Sandbox.ExecutionID)
		done(ctx, nil)
	case endFailed:
		err = actor.Remove(ctx, team, ref.SandboxID, transition.Sandbox.ExecutionID)
		done(ctx, errNodeFailed)
	case endRefusedHeld, endRefused:
		opts := sandboxtypes.RestoreOpts{}
		if finish == endRefusedHeld {
			opts.RetryAfter = differentialHold
		}
		_, err = actor.RestoreRunning(ctx, transition, opts)
		done(ctx, &sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.PauseQueueExhaustedError{}})
	case endRestored:
		// A transient transition: the callback puts the record back.
		done(ctx, nil)
	}
	if err != nil {
		trace += " end error: " + err.Error()
	}

	return trace + " wait=" + waitTrace(waiter)
}

// registerTransitionWaiter waits until a waiter on s is registered for the
// transition the sandbox is in, so the transition's end cannot escape it.
func registerTransitionWaiter(t *testing.T, s *Storage, ref recordRef) {
	t.Helper()

	form, ok, err := s.formOfExisting(t.Context(), ref.TeamID, ref.SandboxID)
	require.NoError(t, err)
	require.Truef(t, ok, "no record of %s to wait on", ref.SandboxID)
	id, err := s.redisClient.Get(t.Context(), transitionKeyFor(form, ref.TeamID, ref.SandboxID)).Result()
	require.NoError(t, err)
	waitForRegisteredWaiters(t, s.subManager, transitionRoutingKey(transitionResultKeyFor(form, ref.TeamID, ref.SandboxID, id)), 1)
}

func waitTrace(waiter <-chan error) string {
	select {
	case err := <-waiter:
		if err == nil {
			return "ok"
		}

		return err.Error()
	case <-time.After(5 * time.Second):
		return "no result within 5s"
	}
}

// staleRemoveTrace removes the sandbox naming an execution that was removed
// before, and shows the result, the team's listing and the record after.
func staleRemoveTrace(t *testing.T, s *Storage, ref recordRef, exec string) string {
	t.Helper()

	team := uuid.MustParse(ref.TeamID)
	result := "ok"
	switch err := s.Remove(t.Context(), team, ref.SandboxID, exec); {
	case errors.Is(err, sandboxtypes.ErrExecutionMismatch):
		result = "mismatch"
	case err != nil:
		result = "error: " + err.Error()
	}

	return fmt.Sprintf("stale remove=%s %s get=%s", result, teamListTrace(t, s, team, staleRemoveFilters), getTrace(t, s, ref))
}

func getTrace(t *testing.T, s *Storage, ref recordRef) string {
	t.Helper()

	sbx, err := s.Get(t.Context(), uuid.MustParse(ref.TeamID), ref.SandboxID)
	if errors.Is(err, sandboxtypes.ErrNotFound) {
		return "notfound"
	}
	if err != nil {
		return "get error: " + err.Error()
	}

	return string(sbx.State) + "/" + sbx.ExecutionID
}

func teamListTrace(t *testing.T, s *Storage, team uuid.UUID, filters []listFilter) string {
	t.Helper()

	parts := make([]string, 0, len(filters))
	for _, filter := range filters {
		items, err := s.TeamItems(t.Context(), team, filter.states)
		if err != nil {
			parts = append(parts, filter.name+"=error: "+err.Error())

			continue
		}
		parts = append(parts, filter.name+"="+formatListed(listedOf(items)))
	}

	return strings.Join(parts, " ")
}

func listTrace(t *testing.T, s *Storage, teams []uuid.UUID) string {
	t.Helper()

	parts := make([]string, 0, len(teams))
	for i, team := range teams {
		parts = append(parts, fmt.Sprintf("t%d: %s", i, teamListTrace(t, s, team, listFilters)))
	}

	return strings.Join(parts, " | ")
}

func countTrace(t *testing.T, s *Storage, teams []uuid.UUID) string {
	t.Helper()

	counts, err := s.TeamsWithSandboxCount(t.Context())
	if err != nil {
		return "count error: " + err.Error()
	}
	var parts []string
	for i, team := range teams {
		if count, ok := counts[team]; ok {
			parts = append(parts, fmt.Sprintf("t%d=%d", i, count))
		}
	}

	return "counts=[" + strings.Join(parts, " ") + "]"
}

// orphanIDs reconciles report against s and returns the IDs it reports as
// orphans.
func orphanIDs(t *testing.T, s *Storage, report []nodeReport) string {
	t.Helper()

	now := time.Now()
	nodes := make([]sandboxtypes.NodeSandbox, 0, len(report))
	for _, entry := range report {
		started := now.Add(-2 * orphanGracePeriod)
		if entry.fresh {
			started = now
		}
		nodes = append(nodes, sandboxtypes.NodeSandbox{SandboxID: entry.ref.SandboxID, TeamID: uuid.MustParse(entry.ref.TeamID), StartTime: started})
	}
	var ids []string
	for _, sbx := range s.Reconcile(t.Context(), nodes, "node") {
		ids = append(ids, sbx.SandboxID)
	}

	return formatIDs(ids)
}

func expiredIDs(t *testing.T, s *Storage) ([]sandboxtypes.Sandbox, string) {
	t.Helper()

	expired, err := s.ExpiredItems(t.Context())
	if err != nil {
		return nil, "expired error: " + err.Error()
	}
	ids := make([]string, 0, len(expired))
	for _, sbx := range expired {
		ids = append(ids, sbx.SandboxID)
	}

	return expired, "expired=" + formatIDs(ids)
}

// evictTrace evicts what the expiration sweep returns, as the evictor does:
// a kill transition, the removal, the callback.
func evictTrace(t *testing.T, s *Storage) string {
	t.Helper()

	ctx := t.Context()
	expired, trace := expiredIDs(t, s)
	slices.SortFunc(expired, func(a, b sandboxtypes.Sandbox) int { return strings.Compare(a.SandboxID, b.SandboxID) })
	for _, sbx := range expired {
		transition, alreadyDone, done, err := s.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill, Eviction: true})
		if err != nil {
			trace += " evict error: " + err.Error()

			continue
		}
		if alreadyDone {
			trace += " evict: already done"

			continue
		}
		if err := s.Remove(ctx, sbx.TeamID, sbx.SandboxID, transition.Sandbox.ExecutionID); err != nil {
			trace += " remove error: " + err.Error()
		}
		done(ctx, nil)
	}

	return trace + " evicted"
}

// projectionTrace is the observable projection of s, as the model's.
func projectionTrace(t *testing.T, s *Storage, teams []uuid.UUID, report []nodeReport) string {
	t.Helper()

	_, expired := expiredIDs(t, s)

	return strings.Join([]string{listTrace(t, s, teams), countTrace(t, s, teams), expired, "orphans=" + orphanIDs(t, s, report)}, " | ")
}

// assertInvariants checks the consistency model's invariants 1 to 3 over
// refs: a sandbox has a record in at most one form; every index entry has a
// record, or has dangled for less than the grace period by the last sweep's
// account; every record has its index entry and its expiration member.
func assertInvariants(t *testing.T, s *Storage, refs []recordRef) {
	t.Helper()

	ctx := t.Context()
	teams := make(map[string]struct{})
	for _, ref := range refs {
		teams[ref.TeamID] = struct{}{}
		records := int64(0)
		for _, form := range []keyForm{formTeam, formSandbox} {
			n, err := s.redisClient.Exists(ctx, recordKey(form, ref.TeamID, ref.SandboxID)).Result()
			require.NoError(t, err)
			records += n
		}
		require.LessOrEqualf(t, records, int64(1), "sandbox %s has a record in both forms", ref.SandboxID)
		if records == 0 {
			continue
		}

		form, _, err := s.formOfExisting(ctx, ref.TeamID, ref.SandboxID)
		require.NoError(t, err)
		data, err := s.redisClient.Get(ctx, recordKey(form, ref.TeamID, ref.SandboxID)).Bytes()
		require.NoError(t, err)
		var sbx sandboxtypes.Sandbox
		require.NoError(t, json.Unmarshal(data, &sbx))
		indexed, err := s.redisClient.SIsMember(ctx, GetSandboxStorageTeamIndexKey(ref.TeamID), ref.SandboxID).Result()
		require.NoError(t, err)
		require.Truef(t, indexed, "record of %s has no index entry", ref.SandboxID)
		require.NoErrorf(t, s.redisClient.ZScore(ctx, globalExpirationSet, sandboxExpirationMember(sbx)).Err(), "record of %s has no expiration member", ref.SandboxID)
	}

	for team := range teams {
		members, err := s.redisClient.SMembers(ctx, GetSandboxStorageTeamIndexKey(team)).Result()
		require.NoError(t, err)
		for _, id := range members {
			_, exists, err := s.formOfExisting(ctx, team, id)
			require.NoError(t, err)
			if exists {
				continue
			}
			since, seen := s.danglingSince[recordRef{TeamID: team, SandboxID: id}]
			require.Truef(t, seen && time.Since(since) <= healGracePeriod, "index entry of %s has no record", id)
		}
	}
}
