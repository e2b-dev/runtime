// Package outbox works the API's River jobs: the jobs another service enqueues
// in the shared database for work only the API can do, because it holds the
// orchestrator connections.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	rescueStuckJobsAfter = 45 * time.Minute
	jobRetention         = 7 * 24 * time.Hour
	// River reads -1 as never: a discarded job stays until someone retries or
	// deletes it.
	discardedJobRetention time.Duration = -1

	retryFloor = 30 * time.Second
	retryCap   = 15 * time.Minute
)

type Config struct {
	MaxWorkers      int
	BacklogInterval time.Duration
}

type Dependencies struct {
	Pool      *pgxpool.Pool
	Sandboxes teamSandboxes
	Teams     *authdb.Client
	Logger    logger.Logger
	Telemetry *telemetry.Client
	Config    Config
}

type River struct {
	client  *river.Client[pgx.Tx]
	backlog *sharedriver.Backlog
}

func New(deps Dependencies) (*River, error) {
	steps := sharedriver.NewSteps(deps.Telemetry, deps.Logger, sharedriver.StepsConfig{MetricPrefix: "api"})

	workers := river.NewWorkers()
	river.AddWorker(workers, &teamResourcesTeardownWorker{
		sandboxes: deps.Sandboxes,
		teams:     deps.Teams,
		steps:     steps.Job(outbox.TeardownTeamResources{}.Kind()),
	})

	client, err := river.NewClient(riverpgxv5.New(deps.Pool), &river.Config{
		Schema:               dbmodule.RiverSchema,
		Queues:               map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: deps.Config.MaxWorkers}},
		Workers:              workers,
		Plugins:              []rivertype.Plugin{sharedriver.NewOTelMiddleware(deps.Telemetry)},
		Middleware:           []rivertype.Middleware{sharedriver.NewOutboxJobTelemetry(deps.Telemetry, deps.Logger)},
		Logger:               sharedriver.NewLogger(deps.Logger),
		RetryPolicy:          retryPolicy{floor: retryFloor, cap: retryCap},
		RescueStuckJobsAfter: rescueStuckJobsAfter,

		CompletedJobRetentionPeriod: jobRetention,
		CancelledJobRetentionPeriod: jobRetention,
		DiscardedJobRetentionPeriod: discardedJobRetention,
	})
	if err != nil {
		return nil, fmt.Errorf("create river client: %w", err)
	}

	backlog, err := sharedriver.NewBacklog(deps.Pool, deps.Telemetry, deps.Logger, sharedriver.BacklogConfig{
		Schema:   dbmodule.RiverSchema,
		Kinds:    []string{outbox.TeardownTeamResources{}.Kind()},
		Interval: deps.Config.BacklogInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("create river backlog: %w", err)
	}

	return &River{client: client, backlog: backlog}, nil
}

func (r *River) Start(ctx context.Context) error {
	if err := r.client.Start(ctx); err != nil {
		return err
	}
	r.backlog.Start(ctx)

	return nil
}

// Stop waits for running jobs until ctx ends. A job still running then is
// rescued after rescueStuckJobsAfter and retried.
func (r *River) Stop(ctx context.Context) error {
	err := r.client.Stop(ctx)
	r.backlog.Stop()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	return nil
}

// retryPolicy waits attempt⁴ seconds between attempts, held between floor and
// cap, with ±20% jitter so jobs that failed together do not retry together.
type retryPolicy struct {
	floor time.Duration
	cap   time.Duration
}

func (p retryPolicy) NextRetry(job *rivertype.JobRow) time.Time {
	return time.Now().Add(p.delay(job.ID, job.Attempt))
}

func (p retryPolicy) delay(jobID int64, attempt int) time.Duration {
	backoff := time.Duration(attempt*attempt*attempt*attempt) * time.Second
	backoff = min(max(backoff, p.floor), p.cap)

	return backoff + time.Duration(int64(backoff)*(jobID%41-20)/100)
}
