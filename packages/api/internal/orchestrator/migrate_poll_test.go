package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/sandboxmigration"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func TestPollAppliesEachOutcome(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	ctx := t.Context()

	placed := h.running(t)
	placedReq := h.start(t, placed)
	completed := h.running(t)
	completedReq := h.start(t, completed)
	aborted := h.running(t)
	abortedReq := h.start(t, aborted)
	stopped := h.running(t)
	stoppedReq := h.start(t, stopped)
	terminated := h.running(t)
	terminatedReq := h.start(t, terminated)
	operator := h.running(t)
	operatorReq := h.start(t, operator)
	operatorHeld := h.encoded(t, operator)

	stoppedMigration := migrationOf(stoppedReq, sandboxmigration.Outcome_OUTCOME_TERMINATED)
	stoppedMigration.SetGuestStopped(true)
	h.migrations.setMigrations(
		migrationOf(placedReq, sandboxmigration.Outcome_OUTCOME_PLACED),
		migrationOf(completedReq, sandboxmigration.Outcome_OUTCOME_COMPLETED),
		migrationOf(abortedReq, sandboxmigration.Outcome_OUTCOME_ABORTED),
		stoppedMigration,
		migrationOf(terminatedReq, sandboxmigration.Outcome_OUTCOME_TERMINATED),
		migrationOf(operatorReq, sandboxmigration.Outcome_OUTCOME_NEEDS_OPERATOR),
	)
	writesBefore := len(h.writes.of(placed.SandboxID))
	h.o.pollMigrations(ctx)

	settled := h.get(t, placed)
	settledRecord := h.encoded(t, placed)
	assert.Equal(t, h.target.ID, settled.NodeID, "PLACED settles on the target")
	assert.Empty(t, settled.LifecycleHold)
	assert.Zero(t, settled.LifecycleHoldUntil)
	placedWrites := h.writes.of(placed.SandboxID)
	require.Len(t, placedWrites, writesBefore+1, "the settlement is one write")

	completedFirst := h.get(t, completed)
	assert.Equal(t, h.target.ID, completedFirst.NodeID, "COMPLETED read first still settles on the target")
	assert.Empty(t, completedFirst.LifecycleHold)

	releasedAborted := h.get(t, aborted)
	assert.Empty(t, releasedAborted.LifecycleHold, "ABORTED releases")
	assert.Equal(t, h.source.ID, releasedAborted.NodeID)

	_, err := h.store.Get(ctx, stopped.TeamID, stopped.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound, "TERMINATED with the guest stopped ends the record")
	require.Eventually(t, func() bool { return h.stops.stoppedCount() == 1 }, 5*time.Second, 5*time.Millisecond, "the end reports the execution's stop")

	releasedTerminated := h.get(t, terminated)
	assert.Empty(t, releasedTerminated.LifecycleHold, "TERMINATED with a guest left releases")
	assert.Equal(t, h.source.ID, releasedTerminated.NodeID)

	assert.Equal(t, operatorHeld, h.encoded(t, operator), "NEEDS_OPERATOR keeps the hold")

	// The next poll reads the placed migration completed: the record is already
	// settled, so nothing is written.
	h.migrations.setMigrations(migrationOf(placedReq, sandboxmigration.Outcome_OUTCOME_COMPLETED), stoppedMigration)
	h.o.pollMigrations(ctx)
	assert.Equal(t, placedWrites, h.writes.of(placed.SandboxID), "COMPLETED after the settlement writes nothing")
	assert.Equal(t, settledRecord, h.encoded(t, placed))
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, h.stops.stoppedCount(), "a repeated end reports nothing")
	assert.Equal(t, 1, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_PLACED, migrationActionSettle), "the settlement is counted once")
	assert.Equal(t, 1, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_COMPLETED, migrationActionSettle), "the other settlement once; the repeat that wrote nothing is not")
	assert.Equal(t, 1, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_TERMINATED, migrationActionEnd))
	assert.Equal(t, 0, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_NEEDS_OPERATOR, migrationActionNone), "nothing to do is not an outcome")
}

// A placed guest whose hold lapsed before the outcome was read leaves the
// record as it was and is reported. A stopped guest whose hold lapsed is
// ended through the ordinary kill: the source answers NotFound for it, the
// record goes and its stop is reported once.
func TestPollReportsAnOutcomeWhoseHoldLapsed(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	ctx := t.Context()
	node := &pauseStubClient{deleteErr: status.Error(codes.NotFound, "no such sandbox")}
	h.source.SetSandboxClient(node)
	placed := h.running(t)
	placedReq := h.start(t, placed)
	stopped := h.running(t)
	stoppedReq := h.start(t, stopped)
	// The holds are gone before the outcomes, as after their deadline.
	for _, s := range []struct {
		sbx sandbox.Sandbox
		id  string
	}{{placed, placedReq.GetMigrationId()}, {stopped, stoppedReq.GetMigrationId()}} {
		require.NoError(t, h.store.ReleaseHeldExecution(ctx, sandbox.Hold{TeamID: s.sbx.TeamID, SandboxID: s.sbx.SandboxID, ExecutionID: s.sbx.ExecutionID, ID: s.id}))
	}
	stoppedMigration := migrationOf(stoppedReq, sandboxmigration.Outcome_OUTCOME_TERMINATED)
	stoppedMigration.SetGuestStopped(true)
	h.migrations.setMigrations(migrationOf(placedReq, sandboxmigration.Outcome_OUTCOME_PLACED), stoppedMigration)

	h.o.pollMigrations(ctx)

	record := h.get(t, placed)
	assert.Equal(t, h.source.ID, record.NodeID, "nothing moved the record")
	assert.Empty(t, record.LifecycleHold)
	assert.Equal(t, 1, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_PLACED, migrationActionUnheld), "the unheld placement is counted")

	_, err := h.store.Get(ctx, stopped.TeamID, stopped.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound, "the late end removes the record")
	assert.Equal(t, 1, node.deleteCount(), "the source is asked to kill the stopped guest once")
	require.Eventually(t, func() bool { return h.stops.stoppedCount() == 1 }, 5*time.Second, 5*time.Millisecond, "the end reports the execution's stop")
	assert.Equal(t, 1, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_TERMINATED, migrationActionEnd), "the late end is counted as an end")
	assert.Zero(t, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_TERMINATED, migrationActionUnheld))

	// Another replica, which has not applied the listing, reads the end again.
	h.o.appliedMigrations.keepListed(nil)
	h.o.pollMigrations(ctx)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, h.stops.stoppedCount(), "a repeated late end reports nothing")
	assert.Equal(t, 1, node.deleteCount())
}

// A late end whose record a later migration took over is not applied: the
// record no longer runs the migration's execution on the source unheld, so
// nothing is killed or removed and no stop is reported.
func TestPollLeavesALateEndOfARecordTakenOver(t *testing.T) {
	t.Parallel()

	for name, settleLater := range map[string]bool{
		"moved on by a later migration": true,
		"held by a later migration":     false,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newMigrationHarness(t)
			ctx := t.Context()
			third := h.node("node-c", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()))
			nodes := map[string]*pauseStubClient{}
			for _, n := range []*nodemanager.Node{h.source, h.target, third} {
				nodes[n.ID] = &pauseStubClient{deleteErr: status.Error(codes.NotFound, "no such sandbox")}
				n.SetSandboxClient(nodes[n.ID])
			}
			sbx := h.running(t)
			firstReq := h.start(t, sbx)
			require.NoError(t, h.store.ReleaseHeldExecution(ctx, sandbox.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: firstReq.GetMigrationId()}))

			_, err := h.o.StartMigration(ctx, sbx.TeamID, sbx.SandboxID, third.ID)
			require.NoError(t, err)
			migrates := h.migrations.migrateRequests()
			secondReq := migrates[len(migrates)-1]
			if settleLater {
				h.migrations.setMigrations(migrationOf(secondReq, sandboxmigration.Outcome_OUTCOME_PLACED))
				h.o.pollMigrations(ctx)
				require.Equal(t, third.ID, h.get(t, sbx).NodeID)
			}
			before := h.encoded(t, sbx)

			lateEnd := migrationOf(firstReq, sandboxmigration.Outcome_OUTCOME_TERMINATED)
			lateEnd.SetGuestStopped(true)
			h.migrations.setMigrations(lateEnd)
			h.o.pollMigrations(ctx)

			assert.Equal(t, before, h.encoded(t, sbx), "the record is left as it was")
			for id, node := range nodes {
				assert.Zero(t, node.deleteCount(), "node %s is not asked to kill", id)
			}
			time.Sleep(50 * time.Millisecond)
			assert.Zero(t, h.stops.stoppedCount(), "no stop is reported")
			assert.Equal(t, 1, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_TERMINATED, migrationActionUnheld), "the late end is counted unheld")
			assert.Zero(t, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_TERMINATED, migrationActionEnd))
		})
	}
}

// A resolved migration stays listed after a later migration of the same
// execution moved the record on: its settlement finds the record unheld on
// another node and is neither counted nor reported as unheld.
func TestPollIgnoresAMigrationALaterOneSuperseded(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	ctx := t.Context()
	third := h.node("node-c", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()))
	sbx := h.running(t)

	firstReq := h.start(t, sbx)
	h.migrations.setMigrations(migrationOf(firstReq, sandboxmigration.Outcome_OUTCOME_PLACED))
	h.o.pollMigrations(ctx)
	require.Equal(t, h.target.ID, h.get(t, sbx).NodeID)

	_, err := h.o.StartMigration(ctx, sbx.TeamID, sbx.SandboxID, third.ID)
	require.NoError(t, err)
	migrates := h.migrations.migrateRequests()
	secondReq := migrates[len(migrates)-1]
	require.Equal(t, h.target.ID, secondReq.GetSource().GetNodeId())
	h.migrations.setMigrations(migrationOf(firstReq, sandboxmigration.Outcome_OUTCOME_PLACED), migrationOf(secondReq, sandboxmigration.Outcome_OUTCOME_PLACED))
	h.o.pollMigrations(ctx)
	require.Equal(t, third.ID, h.get(t, sbx).NodeID)

	// The first migration now reads completed, an outcome this replica has
	// not applied yet.
	h.migrations.setMigrations(migrationOf(firstReq, sandboxmigration.Outcome_OUTCOME_COMPLETED), migrationOf(secondReq, sandboxmigration.Outcome_OUTCOME_PLACED))
	h.o.pollMigrations(ctx)

	assert.Equal(t, third.ID, h.get(t, sbx).NodeID)
	assert.Zero(t, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_COMPLETED, migrationActionUnheld), "the superseded migration is not counted as unheld")
	assert.Zero(t, h.outcomes(t, sandboxmigration.Outcome_OUTCOME_COMPLETED, migrationActionSettle))
}

// A listing this replica already applied is skipped on the next poll, so the
// record is not read or written for it again. A listing the service dropped
// is forgotten, and applied again if it is listed later.
func TestPollSkipsAListingItAlreadyApplied(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	ctx := t.Context()
	sbx := h.running(t)
	req := h.start(t, sbx)
	hold := sandbox.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, SourceNodeID: sbx.NodeID, ID: req.GetMigrationId()}
	aborted := migrationOf(req, sandboxmigration.Outcome_OUTCOME_ABORTED)

	h.migrations.setMigrations(aborted)
	h.o.pollMigrations(ctx)
	require.Empty(t, h.get(t, sbx).LifecycleHold, "the first poll releases")

	// Held again under the same ID: a poll that applied the listing again
	// would release it.
	require.NoError(t, h.store.HoldExecution(ctx, hold, time.Now().Add(time.Hour)))
	h.o.pollMigrations(ctx)
	assert.Equal(t, req.GetMigrationId(), h.get(t, sbx).LifecycleHold, "the applied listing is skipped")

	h.migrations.setMigrations()
	h.o.pollMigrations(ctx)
	h.migrations.setMigrations(aborted)
	h.o.pollMigrations(ctx)
	assert.Empty(t, h.get(t, sbx).LifecycleHold, "a listing dropped and listed again is applied again")
}

// A migration whose record is gone is logged below error, and the rest of the
// list is still applied. Not parallel: it replaces the global logger.
func TestPollContinuesPastARecordGoneMeanwhile(t *testing.T) { //nolint:paralleltest // replaces the global logger
	h := newMigrationHarness(t)
	ctx := t.Context()

	// The record was killed after its hold passed its deadline, while the
	// migration is still listed.
	killed := h.running(t)
	killedID := uuid.NewString()
	h.store.Remove(ctx, killed.TeamID, killed.SandboxID, killed.ExecutionID)
	_, err := h.store.Get(ctx, killed.TeamID, killed.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound)

	next := h.running(t)
	nextReq := h.start(t, next)

	killedMigration := sandboxmigration.Migration_builder{
		MigrationId: killedID,
		SandboxId:   killed.SandboxID,
		ExecutionId: killed.ExecutionID,
		TeamId:      killed.TeamID.String(),
		Target:      nextReq.GetTarget(),
		Outcome:     sandboxmigration.Outcome_OUTCOME_PLACED,
	}.Build()
	h.migrations.setMigrations(killedMigration, migrationOf(nextReq, sandboxmigration.Outcome_OUTCOME_PLACED))

	core, logs := observer.New(zap.DebugLevel)
	restore := logger.ReplaceGlobals(ctx, logger.NewTracedLoggerFromCore(core))
	defer restore()
	h.o.pollMigrations(ctx)

	assert.Equal(t, h.target.ID, h.get(t, next).NodeID, "the migration after it is still applied")
	assert.Zero(t, logs.FilterLevelExact(zap.ErrorLevel).Len(), "nothing is logged as an error")
	gone := logs.FilterField(zap.String("migration_id", killedID)).AllUntimed()
	require.Len(t, gone, 1)
	assert.Equal(t, zap.DebugLevel, gone[0].Level)
}

// RunMigrationPoller polls at once and then every interval, applies what the
// service lists, and stops when its context ends.
func TestRunPollsUntilTheContextEnds(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)
	req := h.start(t, sbx)
	h.migrations.setMigrations(migrationOf(req, sandboxmigration.Outcome_OUTCOME_PLACED))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		h.o.RunMigrationPoller(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(h.migrations.listRequests()) >= 2 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunMigrationPoller did not stop when its context ended")
	}

	assert.Equal(t, h.target.ID, h.get(t, sbx).NodeID)
}
