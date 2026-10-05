package orchestrator

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type eventCounter struct {
	mu    sync.Mutex
	count atomic.Int32
	metas []sandbox.CreationMetadata
}

func (ec *eventCounter) callback() sandbox.CreationCallback {
	return func(_ context.Context, _ sandbox.Sandbox, meta sandbox.CreationMetadata) {
		ec.mu.Lock()
		ec.metas = append(ec.metas, meta)
		ec.mu.Unlock()
		ec.count.Add(1)
	}
}

func (ec *eventCounter) get() int {
	return int(ec.count.Load())
}

func (ec *eventCounter) getMetas() []sandbox.CreationMetadata {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	return append([]sandbox.CreationMetadata{}, ec.metas...)
}

func newOrchestratorWithCounter(t *testing.T) (*Orchestrator, *eventCounter) {
	t.Helper()

	ec := &eventCounter{}

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider(), nil)
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	store := sandbox.NewStore(
		storage,
		redisreservations.NewReservationStorage(client, storage.Notifier()),
		sandbox.Callbacks{
			AsyncNewlyCreatedSandbox: ec.callback(),
		},
	)

	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")
	counter, _ := meter.Int64Counter("test-created")

	ffClient, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)

	algo := placement.NewBestOfK(placement.DefaultBestOfKConfig()).(*placement.BestOfK)

	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	node.ClusterID = uuid.Nil

	o := &Orchestrator{
		sandboxStore:            store,
		nodes:                   smap.New[*nodemanager.Node](),
		placementAlgorithm:      algo,
		featureFlagsClient:      ffClient,
		createdSandboxesCounter: counter,
	}
	o.registerNode(node)

	return o, ec
}

func successFetcher() SandboxDataFetcher {
	return func(_ context.Context) (SandboxMetadata, *api.APIError) {
		return SandboxMetadata{
			TemplateID:     "tpl-test",
			BaseTemplateID: "base-tpl",
			Build:          testBuild(),
		}, nil
	}
}

func failingFetcher(code int, msg string) SandboxDataFetcher {
	return func(_ context.Context) (SandboxMetadata, *api.APIError) {
		return SandboxMetadata{}, &api.APIError{
			Code:      code,
			ClientMsg: msg,
			Err:       &fetcherError{msg: msg},
		}
	}
}

type fetcherError struct{ msg string }

func (e *fetcherError) Error() string { return e.msg }

func fixedSandboxID(t *testing.T) string {
	t.Helper()

	return "sbx-" + uuid.New().String()[:8]
}

func TestCreateSandbox_FreshCreate_FiresCallbackOnce(t *testing.T) {
	t.Parallel()

	o, ec := newOrchestratorWithCounter(t)
	team := testTeam()
	sbxID := fixedSandboxID(t)
	now := time.Now()

	_, apiErr := o.CreateSandbox(
		t.Context(),
		sbxID,
		uuid.New().String(),
		team,
		successFetcher(),
		now, now.Add(time.Hour), time.Hour,
		false,
		false,
		sandbox.CreationMetadata{IsResume: false, TeamName: "test-team"},
	)
	require.Nil(t, apiErr)

	require.Eventually(t, func() bool { return ec.get() == 1 }, time.Second, 10*time.Millisecond)

	metas := ec.getMetas()
	require.Len(t, metas, 1)
	assert.False(t, metas[0].IsResume)
	assert.Equal(t, "test-team", metas[0].TeamName)
}

func TestCreateSandbox_Resume_FiresCallbackOnceWithResumeFlag(t *testing.T) {
	t.Parallel()

	o, ec := newOrchestratorWithCounter(t)
	team := testTeam()
	sbxID := fixedSandboxID(t)
	now := time.Now()

	_, apiErr := o.CreateSandbox(
		t.Context(),
		sbxID,
		uuid.New().String(),
		team,
		successFetcher(),
		now, now.Add(time.Hour), time.Hour,
		true,
		false,
		sandbox.CreationMetadata{IsResume: true, TeamName: "test-team"},
	)
	require.Nil(t, apiErr)

	require.Eventually(t, func() bool { return ec.get() == 1 }, time.Second, 10*time.Millisecond)

	metas := ec.getMetas()
	require.Len(t, metas, 1)
	assert.True(t, metas[0].IsResume)
}

// TestCreateSandbox_ConcurrentRace_LoserPathSkipsCallback is the regression
// test for the PostHog `created_instance` over-counting bug: the loser of a
// concurrent race must not trigger the analytics callback.
func TestCreateSandbox_ConcurrentRace_LoserPathSkipsCallback(t *testing.T) {
	t.Parallel()

	o, ec := newOrchestratorWithCounter(t)
	team := testTeam()
	sbxID := fixedSandboxID(t)
	now := time.Now()

	release := make(chan struct{})
	winnerStarted := make(chan struct{}, 1)

	//nolint:unparam // SandboxDataFetcher signature requires the *APIError return
	winnerFetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
		select {
		case winnerStarted <- struct{}{}:
		default:
		}
		<-release

		return SandboxMetadata{
			TemplateID:     "tpl-test",
			BaseTemplateID: "base-tpl",
			Build:          testBuild(),
		}, nil
	}

	var winnerErr, loserErr *api.APIError
	var wg sync.WaitGroup

	wg.Go(func() {
		_, winnerErr = o.CreateSandbox(
			t.Context(),
			sbxID, uuid.New().String(),
			team, winnerFetcher,
			now, now.Add(time.Hour), time.Hour,
			false,
			false,
			sandbox.CreationMetadata{TeamName: "winner"},
		)
	})

	<-winnerStarted

	wg.Go(func() {
		_, loserErr = o.CreateSandbox(
			t.Context(),
			sbxID, uuid.New().String(),
			team, successFetcher(),
			now, now.Add(time.Hour), time.Hour,
			false,
			false,
			sandbox.CreationMetadata{TeamName: "loser"},
		)
	})

	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	require.Nil(t, winnerErr)
	require.Nil(t, loserErr)

	require.Eventually(t, func() bool { return ec.get() == 1 }, time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, ec.get())

	metas := ec.getMetas()
	require.Len(t, metas, 1)
	assert.Equal(t, "winner", metas[0].TeamName)
}

func TestCreateSandbox_ManyConcurrentRacers_OnlyOneCallback(t *testing.T) {
	t.Parallel()

	const N = 20

	o, ec := newOrchestratorWithCounter(t)
	team := testTeam()
	sbxID := fixedSandboxID(t)
	now := time.Now()

	release := make(chan struct{})
	winnerEntered := make(chan struct{}, 1)
	var winnerOnce sync.Once

	//nolint:unparam // SandboxDataFetcher signature requires the *APIError return
	fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
		winnerOnce.Do(func() {
			winnerEntered <- struct{}{}
			<-release
		})

		return SandboxMetadata{
			TemplateID:     "tpl-test",
			BaseTemplateID: "base-tpl",
			Build:          testBuild(),
		}, nil
	}

	successCount := atomic.Int32{}
	var wg sync.WaitGroup

	wg.Go(func() {
		_, apiErr := o.CreateSandbox(
			t.Context(),
			sbxID, uuid.New().String(),
			team, fetcher,
			now, now.Add(time.Hour), time.Hour,
			false,
			false,
			sandbox.CreationMetadata{},
		)
		if apiErr == nil {
			successCount.Add(1)
		}
	})

	<-winnerEntered

	for range N - 1 {
		wg.Go(func() {
			_, apiErr := o.CreateSandbox(
				t.Context(),
				sbxID, uuid.New().String(),
				team, successFetcher(),
				now, now.Add(time.Hour), time.Hour,
				false,
				false,
				sandbox.CreationMetadata{},
			)
			if apiErr == nil {
				successCount.Add(1)
			}
		})
	}

	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(N), successCount.Load())

	require.Eventually(t, func() bool { return ec.get() == 1 }, time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, ec.get())
}

// TestCreateSandbox_SequentialDuplicate_SecondCallSkipsCallback covers the
// /connect retry pattern: a subsequent call for an already-stored sandbox
// hits ErrAlreadyExists and must not trigger the callback.
func TestCreateSandbox_SequentialDuplicate_SecondCallSkipsCallback(t *testing.T) {
	t.Parallel()

	o, ec := newOrchestratorWithCounter(t)
	team := testTeam()
	sbxID := fixedSandboxID(t)
	now := time.Now()

	_, apiErr := o.CreateSandbox(
		t.Context(),
		sbxID, uuid.New().String(),
		team, successFetcher(),
		now, now.Add(time.Hour), time.Hour,
		false,
		false,
		sandbox.CreationMetadata{},
	)
	require.Nil(t, apiErr)
	require.Eventually(t, func() bool { return ec.get() == 1 }, time.Second, 10*time.Millisecond)

	_, apiErr = o.CreateSandbox(
		t.Context(),
		sbxID, uuid.New().String(),
		team, successFetcher(),
		now, now.Add(time.Hour), time.Hour,
		false,
		false,
		sandbox.CreationMetadata{},
	)
	require.Nil(t, apiErr)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, ec.get())
}

func TestCreateSandbox_FetcherError_NoCallback(t *testing.T) {
	t.Parallel()

	o, ec := newOrchestratorWithCounter(t)
	team := testTeam()
	now := time.Now()

	_, apiErr := o.CreateSandbox(
		t.Context(),
		fixedSandboxID(t), uuid.New().String(),
		team,
		failingFetcher(http.StatusInternalServerError, "synthetic-fetcher-failure"),
		now, now.Add(time.Hour), time.Hour,
		false,
		false,
		sandbox.CreationMetadata{},
	)
	require.NotNil(t, apiErr)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, ec.get())
}

func TestCreateSandbox_QuotaExceeded_NoCallback(t *testing.T) {
	t.Parallel()

	o, ec := newOrchestratorWithCounter(t)
	now := time.Now()

	team := testTeam()
	team.Limits.SandboxConcurrency = 1

	_, apiErr := o.CreateSandbox(
		t.Context(),
		fixedSandboxID(t), uuid.New().String(),
		team, successFetcher(),
		now, now.Add(time.Hour), time.Hour,
		false,
		false,
		sandbox.CreationMetadata{},
	)
	require.Nil(t, apiErr)
	require.Eventually(t, func() bool { return ec.get() == 1 }, time.Second, 10*time.Millisecond)

	_, apiErr = o.CreateSandbox(
		t.Context(),
		fixedSandboxID(t), uuid.New().String(),
		team, successFetcher(),
		now, now.Add(time.Hour), time.Hour,
		false,
		false,
		sandbox.CreationMetadata{},
	)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusTooManyRequests, apiErr.Code)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, ec.get())
}

func TestStoreReconcile_DoesNotFireCallback(t *testing.T) {
	t.Parallel()

	o, ec := newOrchestratorWithCounter(t)

	sbx := sandbox.Sandbox{
		SandboxID:         "reconcile-test-" + uuid.New().String()[:8],
		TemplateID:        "tpl-test",
		BaseTemplateID:    "base-tpl",
		TeamID:            uuid.New(),
		BuildID:           uuid.New(),
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		MaxInstanceLength: time.Hour,
		State:             sandbox.StateRunning,
		NodeID:            "node-1",
	}

	require.NoError(t, o.sandboxStore.Add(t.Context(), sbx, nil))

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, ec.get())
}
