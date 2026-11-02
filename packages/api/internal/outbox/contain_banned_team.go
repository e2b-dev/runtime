package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

const (
	containAttemptTimeout = 10 * time.Minute
	// containSettleDelay outlasts any request admitted before the first cache eviction.
	containSettleDelay = cfg.RequestTimeout + 30*time.Second
)

// containmentProgress is the job's recorded output, kept across snoozes and retries.
type containmentProgress struct {
	EvictedAt time.Time `json:"evicted_at"`
}

var errContainmentIncomplete = errors.New("banned team's workloads are not all stopped")

type teamContainer interface {
	Contain(ctx context.Context, teamID uuid.UUID) team.Containment
}

type teamCache interface {
	InvalidateTeamCache(ctx context.Context, teamID uuid.UUID) error
}

type bannedTeamContainmentWorker struct {
	river.WorkerDefaults[outbox.ContainBannedTeam]

	teams     *authdb.Client
	cache     teamCache
	container teamContainer
	settle    time.Duration
	steps     sharedriver.JobSteps
}

func (w *bannedTeamContainmentWorker) Timeout(*river.Job[outbox.ContainBannedTeam]) time.Duration {
	return containAttemptTimeout
}

func (w *bannedTeamContainmentWorker) Work(ctx context.Context, job *river.Job[outbox.ContainBannedTeam]) error {
	teamID := job.Args.TeamID

	var banned bool
	if err := w.steps.Step(ctx, "check_team_banned", func(ctx context.Context) error {
		var err error
		banned, err = w.teamBanned(ctx, teamID)

		return err
	}); err != nil {
		return err
	}
	if !banned {
		w.steps.Skipped(ctx, "contain", "team is not banned")

		return nil
	}

	if err := w.steps.Step(ctx, "evict_team_cache", func(ctx context.Context) error {
		return w.cache.InvalidateTeamCache(ctx, teamID)
	}); err != nil {
		return err
	}

	var progress containmentProgress
	if output := job.Output(); output != nil {
		if err := json.Unmarshal(output, &progress); err != nil {
			return fmt.Errorf("read containment progress: %w", err)
		}
	}
	if progress.EvictedAt.IsZero() {
		progress.EvictedAt = time.Now()
		if err := river.RecordOutput(ctx, progress); err != nil {
			return fmt.Errorf("record containment progress: %w", err)
		}
	}

	sweepStarted := time.Now()
	if err := w.steps.Step(ctx, "contain", func(ctx context.Context) error {
		return containmentErr(w.container.Contain(ctx, teamID))
	}); err != nil {
		return err
	}

	if settled := progress.EvictedAt.Add(w.settle); sweepStarted.Before(settled) {
		return river.JobSnooze(max(time.Until(settled), 0))
	}

	return nil
}

func (w *bannedTeamContainmentWorker) teamBanned(ctx context.Context, teamID uuid.UUID) (bool, error) {
	row, err := w.teams.GetTeamByTeamID(ctx, teamID)
	if dberrors.IsNotFoundError(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read team: %w", err)
	}

	return row.IsBanned, nil
}

func containmentErr(containment team.Containment) error {
	errs := []error{containment.SandboxKillErr, containment.BuildCancelErr}
	if containment.SandboxesFailedToKill > 0 || containment.BuildsFailedToCancel > 0 {
		errs = append(errs, fmt.Errorf("%w: %d sandboxes and %d builds failed to stop",
			errContainmentIncomplete, containment.SandboxesFailedToKill, containment.BuildsFailedToCancel))
	}

	return errors.Join(errs...)
}
