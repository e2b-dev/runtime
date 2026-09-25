package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/host"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/services/fsfreeze"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

// MMDSClient provides access to MMDS metadata.
type MMDSClient interface {
	GetAccessTokenHash(ctx context.Context) (string, error)
}

// LogFlusher flushes and purges logs before the guest is paused.
type LogFlusher interface {
	FlushAndPurge(ctx context.Context) error
}

type noopLogFlusher struct{}

func (noopLogFlusher) FlushAndPurge(context.Context) error { return nil }

// NewNoopLogFlusher returns a log flusher that performs no work.
func NewNoopLogFlusher() LogFlusher { return noopLogFlusher{} }

// DefaultMMDSClient is the production implementation that calls the real MMDS endpoint.
type DefaultMMDSClient struct{}

func (c *DefaultMMDSClient) GetAccessTokenHash(ctx context.Context) (string, error) {
	return host.GetAccessTokenHashFromMMDS(ctx)
}

type API struct {
	isNotFC     bool
	logger      *zerolog.Logger
	accessToken *SecureToken
	defaults    *execcontext.Defaults

	mmdsChan      chan *host.MMDSOpts
	hyperloopLock sync.Mutex
	mmdsClient    MMDSClient

	lastSetTime *utils.AtomicMax
	initLock    *semaphore.Weighted

	caCertInstaller *host.CACertInstaller
	// workloadFreezer freezes/thaws the user+pty cgroups. Shared with the process
	// service (the live-upgrade handover) so every freeze/unfreeze caller — this
	// API's /freeze, /unfreeze and /init deferred thaw, plus the upgrade — is
	// serialized through one lock.
	workloadFreezer *cgroups.WorkloadFreezer
	logFlusher      LogFlusher

	isMountingNFS atomic.Bool
	mountedPaths  sync.Map // map[path]lifecycleID - tracks which lifecycle each path was mounted for

	// fsFreezer freezes/thaws the guest rootfs for filesystem-only pauses;
	// fsFreezeLock serializes /fsfreeze and /fsthaw.
	fsFreezer    fsfreeze.Freezer
	fsFreezeLock *semaphore.Weighted

	// handover, when non-nil, is the outcome of the live-upgrade handover this
	// envd booted from; PostInit advertises it to the orchestrator via the
	// X-Envd-Handover header. Set once at startup, before serving.
	handover *handoverResult

	// memory is the memory protection configured on envd's cgroup chain, read once
	// at construction; PostInit advertises it to the orchestrator via the
	// X-Envd-Memory header and does no reading of its own. It is a boot-time value:
	// it survives a memory resume inside the snapshot, and only a fresh process (a
	// cold boot, a live-upgrade re-exec) reads it again. The chain's values are the
	// init system's, written at boot, so the two agree unless something changed the
	// cgroups after envd started.
	memory cgroups.MemoryProtection

	// initialized flips true on the first authenticated /init. It gates the
	// live-upgrade /upgrade endpoint and the handover fallback thaw so a
	// re-adopted (possibly hostile) guest process can neither drive an upgrade
	// nor be handed a running workload before /init has re-established auth.
	initialized atomic.Bool

	// mmdsPollRunning is set to true while a PollForMMDSOpts goroutine is
	// in flight. CompareAndSwap on this flag prevents PostInit from spawning
	// a new polling goroutine on every /init retry — each goroutine sends up
	// to 1200 HTTP requests over its 60 s lifetime, so N concurrent retries
	// would produce O(N*1200) MMDS calls and O(N) goroutine stack allocations.
	mmdsPollRunning atomic.Bool
}

// Initialized reports whether the first authenticated /init has completed.
func (a *API) Initialized() bool {
	return a.initialized.Load()
}

// handoverResult is the outcome of a live-upgrade handover, reported to the
// orchestrator on the next /init so the envd-side result (otherwise only logged
// in-guest) is observable fleet-wide.
type handoverResult struct {
	// Failed is true when ResumeFromHandover itself errored/panicked post-execve:
	// the workload was NOT re-adopted (it is orphaned and won't be reaped), so
	// the version flipping to the target does NOT mean a healthy sandbox. Distinct
	// from the per-item *Failed counts below, which are partial degradations of an
	// otherwise-successful handover.
	Failed bool `json:"failed"`
	// Every item is total-carried + failed-subset (ok = total - failed).
	Procs          int `json:"procs"`
	ProcsFailed    int `json:"procs_failed"`
	Retained       int `json:"retained"`
	RetainedFailed int `json:"retained_failed"`
	Watchers       int `json:"watchers"`
	WatchersFailed int `json:"watchers_failed"`
}

// SetHandoverResult records the live-upgrade handover outcome so PostInit can
// advertise it. Called once at startup (before serving) when this envd booted
// via --resume-handover. failed reports whether the handover itself failed (the
// workload was not re-adopted), so the orchestrator can tear the sandbox down
// rather than mistake the version flip for success.
func (a *API) SetHandoverResult(failed bool, procs, procsFailed, retained, retainedFailed, watchers, watchersFailed int) {
	a.handover = &handoverResult{
		Failed:         failed,
		Procs:          procs,
		ProcsFailed:    procsFailed,
		Retained:       retained,
		RetainedFailed: retainedFailed,
		Watchers:       watchers,
		WatchersFailed: watchersFailed,
	}
}

func New(l *zerolog.Logger, defaults *execcontext.Defaults, mmdsChan chan *host.MMDSOpts, isNotFC bool, workloadFreezer *cgroups.WorkloadFreezer, logFlushers ...LogFlusher) *API {
	logFlusher := NewNoopLogFlusher()
	if len(logFlushers) > 0 && logFlushers[0] != nil {
		logFlusher = logFlushers[0]
	}

	return &API{
		logger:          l,
		defaults:        defaults,
		mmdsChan:        mmdsChan,
		isNotFC:         isNotFC,
		mmdsClient:      &DefaultMMDSClient{},
		lastSetTime:     utils.NewAtomicMax(),
		accessToken:     &SecureToken{},
		caCertInstaller: host.NewCACertInstaller(l),
		workloadFreezer: workloadFreezer,
		memory:          workloadFreezer.MemoryProtection(),
		logFlusher:      logFlusher,
		initLock:        semaphore.NewWeighted(1),
		fsFreezer:       fsfreeze.New(),
		fsFreezeLock:    semaphore.NewWeighted(1),
	}
}

func (a *API) GetHealth(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	a.logger.Trace().Msg("Health check")

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "")

	w.WriteHeader(http.StatusNoContent)
}

func (a *API) GetMetrics(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	a.logger.Trace().Msg("Get metrics")

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	metrics, err := host.GetMetrics()
	if err != nil {
		a.logger.Error().Err(err).Msg("Failed to get metrics")
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		a.logger.Error().Err(err).Msg("Failed to encode metrics")
	}
}
