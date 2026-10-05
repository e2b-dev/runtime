package clusters

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/synchronization"
)

func requireCanceledOrExpired(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	require.Error(t, err, msgAndArgs...)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), msgAndArgs...)
}

func TestStartupCoordinatorRetriesTargetThreeTimesWithFreshCanceledContexts(t *testing.T) {
	t.Parallel()

	rootCtx, rootCancel := context.WithCancel(t.Context())
	defer rootCancel()

	target := queries.Cluster{ID: uuid.New()}
	ready := make(chan struct{})
	var (
		mu              sync.Mutex
		attempts        []context.Context
		predecessorErrs []error
		targets         []uuid.UUID
	)

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{target}, nil },
		func(ctx context.Context, cluster queries.Cluster) error {
			mu.Lock()
			if len(attempts) > 0 {
				predecessorErrs = append(predecessorErrs, attempts[len(attempts)-1].Err())
			}
			defer mu.Unlock()
			attempts = append(attempts, ctx)
			targets = append(targets, cluster.ID)
			if len(attempts) < startupAttempts {
				return errors.New("probe failed")
			}

			return nil
		},
		func(context.Context) { close(ready) },
	)

	go coordinator.Start(rootCtx)

	select {
	case <-ready:
	case <-t.Context().Done():
		t.Fatal("startup readiness was not released")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attempts, startupAttempts)
	require.Len(t, predecessorErrs, startupAttempts-1)
	require.Equal(t, []uuid.UUID{target.ID, target.ID, target.ID}, targets)
	assert.NotSame(t, attempts[0], attempts[1])
	assert.NotSame(t, attempts[1], attempts[2])
	require.ErrorIs(t, predecessorErrs[0], context.Canceled)
	require.ErrorIs(t, predecessorErrs[1], context.Canceled)
	require.ErrorIs(t, attempts[0].Err(), context.Canceled)
	require.ErrorIs(t, attempts[1].Err(), context.Canceled)
	require.ErrorIs(t, attempts[2].Err(), context.Canceled)
}

func TestStartupDeadlineRetriesWithFreshContexts(t *testing.T) {
	t.Parallel()

	target := queries.Cluster{ID: uuid.New()}
	ready := make(chan struct{})
	deadlineObserved := make(chan error, 1)
	var (
		mu              sync.Mutex
		attempts        []context.Context
		predecessorErrs []error
		targets         []uuid.UUID
	)

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{target}, nil },
		func(ctx context.Context, cluster queries.Cluster) error {
			mu.Lock()
			if len(attempts) > 0 {
				predecessorErrs = append(predecessorErrs, attempts[len(attempts)-1].Err())
			}
			attempts = append(attempts, ctx)
			targets = append(targets, cluster.ID)
			attempt := len(attempts)
			mu.Unlock()

			switch attempt {
			case 1:
				return errors.New("ordinary probe error")
			case 2:
				<-ctx.Done()
				deadlineObserved <- ctx.Err()

				return ctx.Err()
			case 3:
				return nil
			default:
				return errors.New("unexpected startup attempt")
			}
		},
		func(context.Context) { close(ready) },
	)
	coordinator.probeTimeout = 20 * time.Millisecond

	go coordinator.Start(t.Context())

	select {
	case err := <-deadlineObserved:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-t.Context().Done():
		t.Fatal("second startup attempt did not observe its deadline")
	}

	completionCtx, completionCancel := context.WithTimeout(t.Context(), time.Second)
	defer completionCancel()
	select {
	case <-ready:
	case <-completionCtx.Done():
		t.Fatal("startup did not complete after the observed deadline")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attempts, startupAttempts)
	require.Len(t, predecessorErrs, startupAttempts-1)
	require.Equal(t, []uuid.UUID{target.ID, target.ID, target.ID}, targets)
	assert.NotSame(t, attempts[0], attempts[1])
	assert.NotSame(t, attempts[1], attempts[2])
	requireCanceledOrExpired(t, predecessorErrs[0])
	require.ErrorIs(t, predecessorErrs[1], context.DeadlineExceeded)
	requireCanceledOrExpired(t, attempts[0].Err())
	require.ErrorIs(t, attempts[1].Err(), context.DeadlineExceeded)
	requireCanceledOrExpired(t, attempts[2].Err())
}

func TestStartupCoordinatorWaitsForEmptyResponseApplication(t *testing.T) {
	t.Parallel()

	target := queries.Cluster{ID: uuid.New()}
	ready := make(chan struct{})
	readyResult := make(chan bool, 1)
	var snapshotApplied atomic.Bool

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{target}, nil },
		func(context.Context, queries.Cluster) error {
			// An empty discovery response is still a completed snapshot: the
			// target may satisfy startup only after that snapshot is applied.
			snapshotApplied.Store(true)

			return nil
		},
		func(context.Context) {
			readyResult <- snapshotApplied.Load()
			close(ready)
		},
	)

	go coordinator.Start(t.Context())

	select {
	case <-ready:
		require.True(t, <-readyResult, "readiness must follow empty snapshot application")
	case <-t.Context().Done():
		t.Fatal("empty response application did not release startup")
	}
}

func TestStartupEmptyRemoteDiscoveryAppliesBeforeReadiness(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orchestrators":[]}`))
	}))
	t.Cleanup(server.Close)

	lifecycleCtx, cancel := context.WithCancel(t.Context())
	pool := newLifecycleTestPool(lifecycleCtx, cancel)
	t.Cleanup(func() { pool.Close(t.Context()) })
	target := queries.Cluster{
		ID:       uuid.New(),
		Endpoint: strings.TrimPrefix(server.URL, "http://"),
	}
	readyResult := make(chan error, 1)
	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{target}, nil },
		func(attemptCtx context.Context, cluster queries.Cluster) error {
			return pool.startInitialRemoteCluster(lifecycleCtx, attemptCtx, cluster)
		},
		func(ctx context.Context) {
			if pool.clusters.Count() != 1 {
				readyResult <- errors.New("empty remote snapshot was not applied before readiness")

				return
			}
			pool.finishStartup(ctx)
			readyResult <- nil
		},
	)

	go coordinator.Start(lifecycleCtx)

	select {
	case err := <-readyResult:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal("empty remote discovery did not complete startup")
	}
	select {
	case <-pool.StartupReady():
	case <-t.Context().Done():
		t.Fatal("applied empty remote snapshot did not release pool readiness")
	}
}

func TestStartupRemoteFailureRecoversThroughBackgroundSynchronization(t *testing.T) {
	t.Parallel()

	var rejectDiscovery atomic.Bool
	rejectDiscovery.Store(true)
	var discoveryCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		discoveryCalls.Add(1)
		if rejectDiscovery.Load() {
			http.Error(w, "discovery unavailable", http.StatusServiceUnavailable)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orchestrators":[]}`))
	}))
	t.Cleanup(server.Close)

	lifecycleCtx, cancel := context.WithCancel(t.Context())
	pool := newLifecycleTestPool(lifecycleCtx, cancel)
	t.Cleanup(func() { pool.Close(t.Context()) })
	target := queries.Cluster{ID: uuid.New(), Endpoint: strings.TrimPrefix(server.URL, "http://")}
	recovered := make(chan struct{})
	backgroundRoundStarted := make(chan struct{})
	releaseBackgroundRound := make(chan struct{})
	pool.synchronization = synchronization.NewSynchronize(
		"startup-recovery-test",
		"Startup recovery test",
		&startupRecoveryStore{
			pool:                   pool,
			target:                 target,
			recovered:              recovered,
			backgroundRoundStarted: backgroundRoundStarted,
			releaseBackgroundRound: releaseBackgroundRound,
		},
	)

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{target}, nil },
		func(attemptCtx context.Context, cluster queries.Cluster) error {
			return pool.startInitialRemoteCluster(lifecycleCtx, attemptCtx, cluster)
		},
		pool.finishStartup,
	)

	go coordinator.Start(lifecycleCtx)

	completionCtx, completionCancel := context.WithTimeout(t.Context(), time.Second)
	defer completionCancel()
	select {
	case <-pool.StartupReady():
	case <-completionCtx.Done():
		t.Fatal("remote discovery failures did not fail startup open")
	}
	require.Equal(t, int32(startupAttempts), discoveryCalls.Load(), "startup must bound real remote discovery failures")
	require.Zero(t, pool.clusters.Count(), "failed startup probes must not insert the remote target")

	go pool.synchronization.Start(lifecycleCtx, time.Millisecond, time.Second, false)
	select {
	case <-backgroundRoundStarted:
	case <-completionCtx.Done():
		t.Fatal("scheduled background synchronization did not begin recovery")
	}
	rejectDiscovery.Store(false)
	close(releaseBackgroundRound)
	select {
	case <-recovered:
	case <-completionCtx.Done():
		t.Fatal("scheduled background synchronization did not recover the failed-open target")
	}
	require.Equal(t, 1, pool.clusters.Count(), "ordinary synchronization must insert a target recovered after startup fail-open")
}

func TestStartupParallelTargetsObserveIndependentDeadlineRetries(t *testing.T) {
	t.Parallel()

	first := queries.Cluster{ID: uuid.New()}
	second := queries.Cluster{ID: uuid.New()}
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	ready := make(chan struct{})
	type attemptObservation struct {
		target  uuid.UUID
		attempt int
		err     error
	}
	deadlineObserved := make(chan attemptObservation, 2)
	overlapFailure := make(chan attemptObservation, 1)
	unexpectedAttempt := make(chan attemptObservation, 1)
	attempts := map[uuid.UUID][]context.Context{}
	predecessorErrors := map[uuid.UUID][]error{}
	var mu sync.Mutex

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{first, second}, nil },
		func(ctx context.Context, cluster queries.Cluster) error {
			mu.Lock()
			attempt := len(attempts[cluster.ID]) + 1
			if attempt > 1 {
				predecessorErrors[cluster.ID] = append(predecessorErrors[cluster.ID], attempts[cluster.ID][attempt-2].Err())
			}
			attempts[cluster.ID] = append(attempts[cluster.ID], ctx)
			mu.Unlock()

			switch cluster.ID {
			case first.ID:
				switch attempt {
				case 1:
					close(firstStarted)
					select {
					case <-secondStarted:
						return errors.New("first ordinary error")
					case <-ctx.Done():
						observation := attemptObservation{target: cluster.ID, attempt: attempt, err: ctx.Err()}
						select {
						case overlapFailure <- observation:
						default:
						}

						return ctx.Err()
					}
				case 2:
					<-ctx.Done()
					observation := attemptObservation{target: cluster.ID, attempt: attempt, err: ctx.Err()}
					select {
					case deadlineObserved <- observation:
					default:
					}

					return ctx.Err()
				case 3:
					return nil
				}
			case second.ID:
				switch attempt {
				case 1:
					close(secondStarted)
					<-ctx.Done()
					observation := attemptObservation{target: cluster.ID, attempt: attempt, err: ctx.Err()}
					select {
					case deadlineObserved <- observation:
					default:
					}

					return ctx.Err()
				case 2:
					return nil
				}
			}
			observation := attemptObservation{target: cluster.ID, attempt: attempt}
			select {
			case unexpectedAttempt <- observation:
			default:
			}

			return errors.New("unexpected startup attempt")
		},
		func(context.Context) { close(ready) },
	)
	coordinator.probeTimeout = 20 * time.Millisecond

	go coordinator.Start(t.Context())

	completionCtx, completionCancel := context.WithTimeout(t.Context(), time.Second)
	defer completionCancel()
	select {
	case <-firstStarted:
	case <-completionCtx.Done():
		t.Fatal("first target did not start")
	}
	select {
	case <-secondStarted:
	case <-completionCtx.Done():
		t.Fatal("second target did not start while first target was blocked")
	}
	select {
	case observation := <-overlapFailure:
		t.Fatalf("startup targets did not overlap: first target attempt %d observed %v before the second target entered the barrier", observation.attempt, observation.err)
	default:
	}

	observed := map[uuid.UUID]attemptObservation{}
	for range 2 {
		select {
		case observation := <-deadlineObserved:
			observed[observation.target] = observation
		case <-completionCtx.Done():
			t.Fatal("parallel target did not observe its independent deadline")
		}
	}
	firstDeadline, found := observed[first.ID]
	require.True(t, found, "first target must observe its second-attempt deadline")
	require.Equal(t, 2, firstDeadline.attempt)
	require.ErrorIs(t, firstDeadline.err, context.DeadlineExceeded)
	secondDeadline, found := observed[second.ID]
	require.True(t, found, "second target must observe its first-attempt deadline")
	require.Equal(t, 1, secondDeadline.attempt)
	require.ErrorIs(t, secondDeadline.err, context.DeadlineExceeded)

	select {
	case <-ready:
	case <-completionCtx.Done():
		t.Fatal("parallel startup did not complete after both deadline events")
	}
	select {
	case observation := <-unexpectedAttempt:
		t.Fatalf("unexpected startup attempt for target %s attempt %d", observation.target, observation.attempt)
	default:
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attempts[first.ID], 3)
	require.Len(t, attempts[second.ID], 2)
	require.Len(t, predecessorErrors[first.ID], 2)
	require.Len(t, predecessorErrors[second.ID], 1)
	requireCanceledOrExpired(t, predecessorErrors[first.ID][0], "the ordinary-error attempt must complete before retrying")
	require.ErrorIs(t, predecessorErrors[first.ID][1], context.DeadlineExceeded, "the expired attempt must finish before retrying")
	require.ErrorIs(t, predecessorErrors[second.ID][0], context.DeadlineExceeded, "the expired attempt must finish before retrying")
	assert.NotSame(t, attempts[first.ID][0], attempts[first.ID][1])
	assert.NotSame(t, attempts[first.ID][1], attempts[first.ID][2])
	assert.NotSame(t, attempts[second.ID][0], attempts[second.ID][1])
	requireCanceledOrExpired(t, attempts[first.ID][0].Err(), "the ordinary-error attempt must be canceled or expire")
	require.ErrorIs(t, attempts[first.ID][1].Err(), context.DeadlineExceeded)
	requireCanceledOrExpired(t, attempts[first.ID][2].Err(), "the successful attempt must be canceled or expire")
	require.ErrorIs(t, attempts[second.ID][0].Err(), context.DeadlineExceeded)
	requireCanceledOrExpired(t, attempts[second.ID][1].Err(), "the successful attempt must be canceled or expire")
}

type startupRecoveryStore struct {
	pool                   *Pool
	target                 queries.Cluster
	recovered              chan<- struct{}
	backgroundRoundStarted chan<- struct{}
	releaseBackgroundRound <-chan struct{}
	recover                sync.Once
	backgroundRound        sync.Once
}

func (s *startupRecoveryStore) SourceList(ctx context.Context) ([]queries.Cluster, error) {
	s.backgroundRound.Do(func() { close(s.backgroundRoundStarted) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.releaseBackgroundRound:
	}

	return []queries.Cluster{s.target}, nil
}

func (s *startupRecoveryStore) SourceExists(ctx context.Context, source []queries.Cluster, cluster *Cluster) bool {
	return s.pool.store.SourceExists(ctx, source, cluster)
}

func (s *startupRecoveryStore) PoolList(ctx context.Context) []*Cluster {
	return s.pool.store.PoolList(ctx)
}

func (s *startupRecoveryStore) PoolExists(ctx context.Context, cluster queries.Cluster) bool {
	return s.pool.store.PoolExists(ctx, cluster)
}

func (s *startupRecoveryStore) PoolInsert(ctx context.Context, cluster queries.Cluster) {
	s.pool.store.PoolInsert(ctx, cluster)
	if s.pool.clusters.Count() == 1 {
		s.recover.Do(func() { close(s.recovered) })
	}
}

func (s *startupRecoveryStore) PoolUpdate(ctx context.Context, cluster *Cluster) {
	s.pool.store.PoolUpdate(ctx, cluster)
}

func (s *startupRecoveryStore) PoolRemove(ctx context.Context, cluster *Cluster) {
	s.pool.store.PoolRemove(ctx, cluster)
}

func TestStartupCancellationPreventsReadiness(t *testing.T) {
	t.Parallel()

	rootCtx, rootCancel := context.WithCancel(t.Context())
	target := queries.Cluster{ID: uuid.New()}
	attemptStarted := make(chan struct{})
	attemptCanceled := make(chan error, 1)
	ready := make(chan struct{})

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{target}, nil },
		func(ctx context.Context, _ queries.Cluster) error {
			close(attemptStarted)
			<-ctx.Done()
			attemptCanceled <- ctx.Err()

			return ctx.Err()
		},
		func(context.Context) { close(ready) },
	)

	go coordinator.Start(rootCtx)
	select {
	case <-attemptStarted:
	case <-t.Context().Done():
		t.Fatal("startup attempt did not start")
	}
	rootCancel()

	select {
	case err := <-attemptCanceled:
		require.ErrorIs(t, err, context.Canceled)
	case <-t.Context().Done():
		t.Fatal("startup attempt did not observe cancellation")
	}

	select {
	case <-ready:
		t.Fatal("canceled startup must not become ready")
	default:
	}
}

func TestStartupCoordinatorFailsOpenAfterThreeFailures(t *testing.T) {
	t.Parallel()

	target := queries.Cluster{ID: uuid.New()}
	ready := make(chan struct{})
	var attempts atomic.Int32

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) { return []queries.Cluster{target}, nil },
		func(context.Context, queries.Cluster) error {
			attempts.Add(1)

			return errors.New("probe failed")
		},
		func(context.Context) { close(ready) },
	)

	go coordinator.Start(t.Context())

	select {
	case <-ready:
	case <-t.Context().Done():
		t.Fatal("three failed attempts did not release startup")
	}
	require.Equal(t, int32(startupAttempts), attempts.Load())
}

func TestStartupCoordinatorWaitsForFirstSuccessfulInventory(t *testing.T) {
	t.Parallel()

	inventoryFailed := make(chan struct{})
	retryInventory := make(chan struct{})
	ready := make(chan struct{})
	inventoryCalls := 0
	var targetStarts atomic.Int32

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) {
			inventoryCalls++
			if inventoryCalls == 1 {
				close(inventoryFailed)

				return nil, errors.New("inventory unavailable")
			}

			return nil, nil
		},
		func(context.Context, queries.Cluster) error {
			targetStarts.Add(1)

			return errors.New("empty first successful inventory must not probe a target")
		},
		func(context.Context) { close(ready) },
	)
	coordinator.waitRetry = func(ctx context.Context) bool {
		select {
		case <-ctx.Done():
			return false
		case <-retryInventory:
			return true
		}
	}

	go coordinator.Start(t.Context())
	select {
	case <-inventoryFailed:
	case <-t.Context().Done():
		t.Fatal("initial inventory did not run")
	}
	select {
	case <-ready:
		t.Fatal("failed inventory must keep startup blocked")
	default:
	}

	close(retryInventory)
	select {
	case <-ready:
	case <-t.Context().Done():
		t.Fatal("successful empty inventory did not release startup")
	}
	require.Equal(t, 2, inventoryCalls)
	require.Zero(t, targetStarts.Load(), "empty first successful inventory must not probe a target")
}

func TestStartupCoordinatorFreezesInitialTargets(t *testing.T) {
	t.Parallel()

	initial := queries.Cluster{ID: uuid.New()}
	late := queries.Cluster{ID: uuid.New()}
	ready := make(chan struct{})
	var inventoryCalls atomic.Int32
	var started []uuid.UUID
	var mu sync.Mutex

	coordinator := newStartupCoordinator(
		func(context.Context) ([]queries.Cluster, error) {
			if inventoryCalls.Add(1) == 1 {
				return []queries.Cluster{initial}, nil
			}

			return []queries.Cluster{initial, late}, errors.New("late discovery must not re-arm startup")
		},
		func(_ context.Context, cluster queries.Cluster) error {
			mu.Lock()
			started = append(started, cluster.ID)
			mu.Unlock()

			return nil
		},
		func(context.Context) { close(ready) },
	)

	go coordinator.Start(t.Context())

	select {
	case <-ready:
	case <-t.Context().Done():
		t.Fatal("initial frozen target did not complete startup")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, int32(1), inventoryCalls.Load())
	require.Equal(t, []uuid.UUID{initial.ID}, started)
}
