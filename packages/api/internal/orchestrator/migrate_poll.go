package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/sandboxmigration"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	migrationActionSettle  = "settle"
	migrationActionRelease = "release"
	migrationActionEnd     = "end"
	migrationActionNone    = "none"
	// migrationActionUnheld counts a placed guest whose record was no longer
	// held when the settlement arrived, and a stopped guest whose unheld
	// record could not be ended.
	migrationActionUnheld = "unheld"
)

// appliedListings is what one replica already applied: the outcome of each
// listed migration whose outcome it applied. A poll skips a listing it holds
// instead of taking the record's lock again. It saves lock round trips only
// and is not needed for correctness: a replica that restarts applies every
// listing again, and every write is conditional on the execution and the
// hold.
type appliedListings struct {
	mu       sync.Mutex
	outcomes map[string]sandboxmigration.Outcome
}

func (a *appliedListings) has(migration *sandboxmigration.Migration) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	outcome, ok := a.outcomes[migration.GetMigrationId()]

	return ok && outcome == migration.GetOutcome()
}

func (a *appliedListings) add(migration *sandboxmigration.Migration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.outcomes == nil {
		a.outcomes = map[string]sandboxmigration.Outcome{}
	}
	a.outcomes[migration.GetMigrationId()] = migration.GetOutcome()
}

// keepListed forgets every migration the manager no longer lists.
func (a *appliedListings) keepListed(migrations []*sandboxmigration.Migration) {
	listed := make(map[string]struct{}, len(migrations))
	for _, migration := range migrations {
		listed[migration.GetMigrationId()] = struct{}{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for id := range a.outcomes {
		if _, ok := listed[id]; !ok {
			delete(a.outcomes, id)
		}
	}
}

// RunMigrationPoller polls ListMigrations every poll interval until ctx ends
// and applies each outcome to the record. Every replica runs it: each write is
// conditional on the execution and the hold, so a repeat changes nothing. It
// returns at once when sandbox migration is not configured, and once a poll
// in flight ended when ctx ends.
func (o *Orchestrator) RunMigrationPoller(ctx context.Context) {
	if o.migrations == nil {
		return
	}

	ticker := time.NewTicker(o.migrationPollInterval)
	defer ticker.Stop()

	for {
		o.pollMigrations(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pollMigrations reads every migration this API may still have to settle,
// the unresolved ones and the resolved ones the manager still keeps, and applies
// each outcome it has not applied yet under its own time budget. A replica
// that missed a resolution, or every replica through an outage, still applies
// it while the manager lists it.
func (o *Orchestrator) pollMigrations(ctx context.Context) {
	listCtx, cancel := context.WithTimeout(ctx, migrationCallTimeout)
	defer cancel()
	resp, err := o.migrations.ListMigrations(listCtx, &sandboxmigration.ListMigrationsRequest{})
	if err != nil {
		if ctx.Err() == nil {
			logger.L().Warn(ctx, "Failed to list sandbox migrations", zap.Error(err))
		}

		return
	}

	o.appliedMigrations.keepListed(resp.GetMigrations())
	for _, migration := range resp.GetMigrations() {
		if ctx.Err() != nil {
			return
		}
		if o.appliedMigrations.has(migration) {
			continue
		}
		settleCtx, cancel := context.WithTimeout(ctx, migrationSettleTimeout)
		if o.applyMigration(settleCtx, migration) {
			o.appliedMigrations.add(migration)
		}
		cancel()
	}
}

// applyMigration writes one migration's outcome to its record:
//
//	PLACED, COMPLETED             settle on the target
//	ABORTED                       release
//	TERMINATED with guest_stopped end, reported as a kill
//	TERMINATED                    release
//	IN_PROGRESS, NEEDS_OPERATOR   nothing
//
// A record not held by the migration, gone or of another execution is expected
// once an earlier poll wrote it or the hold expired. An end reports the
// execution's stop once, from the record the removal returned: a repeat finds
// the record gone and reports nothing. It returns false when the outcome was
// not applied and a later poll should try again.
//
// Only a termination can outlive the hold. The manager confirms a placement
// within the operation deadline plus under a minute, and the hold outlives
// that deadline by the termination budget, so a placement is always read while
// the record is held. An unheld placement is reported and moves nothing. An
// end read after the hold lapsed ends the record through the ordinary kill,
// while it still runs the migration's execution on the source: nothing else
// would end it, since the source does not list a stop it was asked for.
func (o *Orchestrator) applyMigration(ctx context.Context, migration *sandboxmigration.Migration) bool {
	outcome := migration.GetOutcome()
	fields := []zap.Field{
		zap.String("migration_id", migration.GetMigrationId()),
		logger.WithSandboxID(migration.GetSandboxId()),
		zap.String("outcome", outcome.String()),
	}

	teamID, err := uuid.Parse(migration.GetTeamId())
	if err != nil {
		logger.L().Error(ctx, "Sandbox migration names no valid team", append(fields, zap.Error(err))...)

		return true
	}
	hold := sandbox.Hold{TeamID: teamID, SandboxID: migration.GetSandboxId(), ExecutionID: migration.GetExecutionId(), ID: migration.GetMigrationId()}

	action := migrationActionNone
	changed := true
	switch outcome {
	case sandboxmigration.Outcome_OUTCOME_PLACED, sandboxmigration.Outcome_OUTCOME_COMPLETED:
		if migration.GetTarget().GetNodeId() == "" {
			logger.L().Error(ctx, "Sandbox migration placed a guest on a target without a node ID, the hold is kept", fields...)

			return true
		}
		action = migrationActionSettle
		changed, err = o.sandboxStore.MoveHeldExecution(ctx, hold, migration.GetTarget().GetNodeId())
	case sandboxmigration.Outcome_OUTCOME_ABORTED:
		action = migrationActionRelease
		err = o.sandboxStore.ReleaseHeldExecution(ctx, hold)
	case sandboxmigration.Outcome_OUTCOME_TERMINATED:
		if migration.GetGuestStopped() {
			action = migrationActionEnd
			var removed sandbox.Sandbox
			if removed, err = o.sandboxStore.RemoveHeldExecution(ctx, hold); err == nil {
				go o.analyticsRemove(context.WithoutCancel(ctx), removed, sandbox.StateActionKill)
			}
		} else {
			action = migrationActionRelease
			err = o.sandboxStore.ReleaseHeldExecution(ctx, hold)
		}
	}
	// Counted once per change the record took, and once per placement or end
	// that found it unheld and left it: a listed migration is read on every tick by every
	// replica, so the reads are not the rate. A settlement already on the
	// record succeeds without writing and is not counted again.
	counted := func(action string) {
		o.migrationOutcomesCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("outcome", outcome.String()),
			attribute.String("action", action),
		))
	}

	fields = append(fields, zap.String("action", action))
	switch {
	case err == nil && action != migrationActionNone && changed:
		counted(action)
	case err == nil:
	case action == migrationActionSettle && errors.Is(err, sandbox.ErrNotHeld) && !o.recordNamesNode(ctx, teamID, migration.GetSandboxId(), migration.GetSource().GetNodeId()):
		// A later migration of the execution moved the record on, while the
		// manager still lists this one.
		logger.L().Debug(ctx, "Sandbox migration outcome superseded by a later migration", append(fields, zap.Error(err))...)
	case action == migrationActionSettle && errors.Is(err, sandbox.ErrNotHeld):
		// The record is unheld and still names the source while the guest
		// runs on the target: the hold lapsed or was released before the
		// settlement. Kill and pause now go to the wrong node.
		counted(migrationActionUnheld)
		logger.L().Error(ctx, "Sandbox migration placed a guest whose record is no longer held", append(fields, zap.Error(err))...)
	case action == migrationActionEnd && errors.Is(err, sandbox.ErrNotHeld):
		if endErr := o.endUnheldExecution(ctx, teamID, migration); endErr != nil {
			// The record is not the migration's running source record any
			// more, or the kill was refused, a hold taken since or a lock
			// lost among them.
			counted(migrationActionUnheld)
			logger.L().Error(ctx, "Sandbox migration stopped a guest whose record is no longer held", append(fields, zap.Error(err), zap.NamedError("end_error", endErr))...)

			break
		}
		counted(migrationActionEnd)
		logger.L().Info(ctx, "Sandbox migration stopped a guest after its hold lapsed, the record is ended", fields...)
	case errors.Is(err, sandbox.ErrNotHeld), errors.Is(err, sandbox.ErrNotFound), errors.Is(err, sandbox.ErrExecutionMismatch):
		logger.L().Debug(ctx, "Sandbox migration outcome already applied or no longer applicable", append(fields, zap.Error(err))...)
	default:
		logger.L().Error(ctx, "Failed to apply a sandbox migration outcome", append(fields, zap.Error(err))...)

		return false
	}

	return true
}

// endUnheldExecution kills the record of a migration's stopped guest after
// the hold lapsed, when the record still runs the migration's execution on
// the source. The source answers NotFound for the stopped guest, the record
// goes and RemoveSandbox reports the stop once.
func (o *Orchestrator) endUnheldExecution(ctx context.Context, teamID uuid.UUID, migration *sandboxmigration.Migration) error {
	record, err := o.sandboxStore.Get(ctx, teamID, migration.GetSandboxId())
	if err != nil {
		return fmt.Errorf("read the record: %w", err)
	}
	if record.State != sandbox.StateRunning || record.ExecutionID != migration.GetExecutionId() || record.NodeID != migration.GetSource().GetNodeId() {
		return fmt.Errorf("the record is %s on node %q, not the migration's source record", record.State, record.NodeID)
	}

	return o.RemoveSandbox(ctx, teamID, migration.GetSandboxId(), sandbox.RemoveOpts{
		Action:            sandbox.StateActionKill,
		Reason:            sandbox.KillReasonEndedOnNode,
		ExpectExecutionID: migration.GetExecutionId(),
	})
}

// recordNamesNode reports whether the sandbox's record names nodeID. A record
// that cannot be read is taken to still name it, a record gone names nothing.
func (o *Orchestrator) recordNamesNode(ctx context.Context, teamID uuid.UUID, sandboxID, nodeID string) bool {
	record, err := o.sandboxStore.Get(ctx, teamID, sandboxID)
	switch {
	case errors.Is(err, sandbox.ErrNotFound):
		return false
	case err != nil:
		return true
	default:
		return record.NodeID == nodeID
	}
}
