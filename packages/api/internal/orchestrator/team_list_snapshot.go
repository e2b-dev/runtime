package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// A list request reads a team's whole sandbox index from the store and applies
// its filters, page and cursor in memory, so every list request of a team does
// the same store read and gets the same answer. teamListSnapshots makes a
// team's requests share that read: callers that arrive while a read is in
// flight wait for it, and callers that arrive within the snapshot's lifetime are
// served from memory.
//
// The lifetime is the team's lag allowance, the most a served list may lag the
// store, counted from the moment the read starts.
//
// The shared read outlives the request that started it, so a cancelled caller
// cannot fail the callers waiting on it; it is bounded by cfg.RequestTimeout,
// the budget a request's own read would have had.

// sharedListStates are the states a shared read loads, the union the list
// endpoints ask for; each caller's list is filtered from it. A read without a
// lag allowance loads the caller's own states instead. The store keeps the
// slice it is given, so each read gets its own.
func sharedListStates() []sandbox.State {
	return []sandbox.State{sandbox.StateRunning, sandbox.StatePausing}
}

// listSnapshotOutcome is how a list read was served.
type listSnapshotOutcome string

const (
	listSnapshotDisabled listSnapshotOutcome = "disabled" // no lag allowed: the caller's own store read
	listSnapshotRead     listSnapshotOutcome = "read"     // this caller started the shared read
	listSnapshotShared   listSnapshotOutcome = "shared"   // joined a read already in flight
	listSnapshotHit      listSnapshotOutcome = "hit"      // served from a live snapshot
)

type teamListLoader func(ctx context.Context, states []sandbox.State) ([]sandbox.Sandbox, error)

// teamListRead is a served list: its items, how they were served, and their
// age, counted from the start of the store read that produced them, so a read
// reports its duration and a hit how stale it is.
type teamListRead struct {
	items   []sandbox.Sandbox
	outcome listSnapshotOutcome
	age     time.Duration
}

type teamListSnapshots struct {
	mu      sync.Mutex
	entries map[uuid.UUID]*teamListEntry
}

// teamListEntry is a team's in-flight read or live snapshot. done is closed
// once items and err are set; timer, set with them, drops a kept snapshot.
type teamListEntry struct {
	start time.Time
	done  chan struct{}
	items []sandbox.Sandbox
	err   error
	timer *time.Timer
}

// read returns the team's sandboxes in states from load, sharing one load of
// sharedListStates between the callers that need it at the same time, keeping
// its result for its lifetime and filtering a copy per caller; no states
// means a copy of all the load holds, which is sharedListStates only. Without
// a lag allowance, load runs for this caller alone, with its states, and its
// slice is returned as is: there, no states means every state the store
// holds.
func (s *teamListSnapshots) read(ctx context.Context, teamID uuid.UUID, maxLag time.Duration, states []sandbox.State, load teamListLoader) (teamListRead, error) {
	if maxLag <= 0 {
		start := time.Now()
		items, err := load(ctx, states)

		return teamListRead{items: items, outcome: listSnapshotDisabled, age: time.Since(start)}, err
	}

	e, outcome, age := s.entry(ctx, teamID, maxLag, load)

	return awaitEntry(ctx, e, outcome, age, states)
}

// awaitEntry waits for e's read, unless ctx ends first, and serves it filtered
// to states. A read already complete is served even to a caller whose context
// has ended: the result is in hand and was counted as such. age is the hit's
// age as entry judged it; the other outcomes take their age at completion.
func awaitEntry(ctx context.Context, e *teamListEntry, outcome listSnapshotOutcome, age time.Duration, states []sandbox.State) (teamListRead, error) {
	select {
	case <-e.done:
	default:
		select {
		case <-e.done:
		case <-ctx.Done():
			return teamListRead{outcome: outcome}, ctx.Err()
		}
	}

	if outcome != listSnapshotHit {
		age = time.Since(e.start)
	}
	if e.err != nil {
		return teamListRead{outcome: outcome, age: age}, e.err
	}
	items := utils.Filter(e.items, func(sbx sandbox.Sandbox) bool { return slices.Contains(states, sbx.State) })
	if len(states) == 0 {
		items = slices.Clone(e.items)
	}

	return teamListRead{items: items, outcome: outcome, age: age}, nil
}

// entry returns the team's live entry, starting a read when there is none.
// For a hit it also returns the snapshot's age as judged under the lock, so
// the age reported is the one found within the allowance.
func (s *teamListSnapshots) entry(ctx context.Context, teamID uuid.UUID, maxLag time.Duration, load teamListLoader) (*teamListEntry, listSnapshotOutcome, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[teamID]; ok {
		select {
		case <-e.done:
			// Judged against the allowance in force now, so a lowered flag applies
			// to a snapshot that is already live.
			if age := time.Since(e.start); age < maxLag {
				return e, listSnapshotHit, age
			}
			// Too old for the allowance now in force: replace it, and let go of
			// its items now rather than when its own timer would have.
			if e.timer != nil {
				e.timer.Stop()
			}
		default:
			return e, listSnapshotShared, 0
		}
	}

	if s.entries == nil {
		s.entries = make(map[uuid.UUID]*teamListEntry)
	}

	e := &teamListEntry{start: time.Now(), done: make(chan struct{})}
	s.entries[teamID] = e
	go s.run(context.WithoutCancel(ctx), e, teamID, maxLag, load)

	return e, listSnapshotRead, 0
}

// run performs e's read and publishes the result. A failed read is dropped at
// once; a kept snapshot is dropped by a timer when it expires.
func (s *teamListSnapshots) run(ctx context.Context, e *teamListEntry, teamID uuid.UUID, maxLag time.Duration, load teamListLoader) {
	ctx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	items, err := recoverLoad(ctx, load)

	s.mu.Lock()
	defer s.mu.Unlock()

	e.items, e.err = items, err
	close(e.done)

	expires := e.start.Add(maxLag)
	if err != nil || !time.Now().Before(expires) {
		s.drop(teamID, e)

		return
	}

	e.timer = time.AfterFunc(time.Until(expires), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.drop(teamID, e)
	})
}

// drop removes e if it is still the team's entry. The caller holds mu.
func (s *teamListSnapshots) drop(teamID uuid.UUID, e *teamListEntry) {
	if s.entries[teamID] == e {
		delete(s.entries, teamID)
	}
}

// recoverLoad runs load and turns a panic into an error: the read runs outside
// any request, where nothing else would recover it or print its stack.
func recoverLoad(ctx context.Context, load teamListLoader) (items []sandbox.Sandbox, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.L().Error(ctx, "Team list read panicked", zap.Any("panic", r), zap.Stack("stack"))
			err = fmt.Errorf("team list read panicked: %v", r)
		}
	}()

	return load(ctx, sharedListStates())
}

// ListSandboxes is GetSandboxes for the list endpoints: a team's list requests
// share one store read of its running and pausing sandboxes, filtered per call
// to states. An empty list means every state the read holds: running and
// pausing from a shared read, but every state the store holds (killing and
// snapshotting too) from a request's own read, as GetSandboxes returns it.
// Without a lag allowance it is GetSandboxes: the caller's own store read of
// states, returned as the store gave it.
// The result may lag the store by up to the team's lag allowance, counted from
// the start of the read that produced it; a request that joins a read already in
// flight gets that read's result however long it takes, as a read of its own
// would take as long. A sandbox created, killed, paused or resumed inside that
// window can be missing or listed in its previous state on this replica until
// the snapshot expires, and one resumed inside it can appear on two pages of a
// walk served by different replicas. The endpoints already treat the list as
// best effort. The allowance comes from featureflags.SandboxListMaxLagMsFlag;
// zero keeps every request on its own read, as does a flag that fails to
// evaluate.
func (o *Orchestrator) ListSandboxes(ctx context.Context, teamID uuid.UUID, states []sandbox.State) ([]sandbox.Sandbox, error) {
	ctx, span := tracer.Start(ctx, "list-sandboxes")
	defer span.End()

	maxLagMs, err := o.featureFlagsClient.IntFlagWithError(ctx, featureflags.SandboxListMaxLagMsFlag, featureflags.TeamContext(teamID.String()))
	if err != nil {
		logger.L().Debug(ctx, "Sandbox list lag flag failed to evaluate, using its fallback", zap.Error(err), logger.WithTeamID(teamID.String()))
		maxLagMs = featureflags.SandboxListMaxLagMsFlag.Fallback()
	}
	read, err := o.teamLists.read(ctx, teamID, time.Duration(maxLagMs)*time.Millisecond, states, func(ctx context.Context, states []sandbox.State) ([]sandbox.Sandbox, error) {
		return o.sandboxStore.TeamItems(ctx, teamID, states)
	})

	span.SetAttributes(attribute.String("sandbox.list.read", string(read.outcome)))
	attrs := metric.WithAttributes(telemetry.WithTeamID(teamID.String()), attribute.String("outcome", string(read.outcome)))
	o.listReadsCounter.Add(ctx, 1, attrs)
	if err != nil {
		return nil, err
	}
	// The age is recorded per outcome, not per team: with the flag set
	// fleet-wide a per-team histogram would multiply the counter's series by
	// its buckets. The counter keeps the team, and the age series exist only
	// once some team's flag is set.
	if read.outcome != listSnapshotDisabled {
		o.listAgeHistogram.Record(ctx, read.age.Milliseconds(), metric.WithAttributes(attribute.String("outcome", string(read.outcome))))
	}

	return read.items, nil
}
