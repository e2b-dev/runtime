package orchestrator

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/sandboxmigration"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// fakeMigrationService stands in for the migration service: it validates each request
// against the contract, records it and answers with canned migrations.
type fakeMigrationService struct {
	sandboxmigration.UnimplementedSandboxMigrationManagerServiceServer

	mu         sync.Mutex
	migrates   []*sandboxmigration.MigrateRequest
	lists      []*sandboxmigration.ListMigrationsRequest
	migrateErr error
	// migrateErrs answer the first Migrate calls in order, before migrateErr.
	migrateErrs []error
	// unavailable refuses every call with Unavailable, as a service that is
	// down or not serving yet.
	unavailable bool
	// lostReply makes Migrate fail after the migration was accepted, so
	// GetMigration still finds it.
	lostReply  bool
	migrations []*sandboxmigration.Migration
	// onMigrate runs before Migrate answers, to observe the record as the
	// migration service would find it.
	onMigrate func(*sandboxmigration.MigrateRequest)
}

var errMigrationServiceUnavailable = status.Error(codes.Unavailable, "the migration service is not serving")

func (f *fakeMigrationService) Migrate(_ context.Context, req *sandboxmigration.MigrateRequest) (*sandboxmigration.MigrateResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	f.mu.Lock()
	if f.unavailable {
		f.mu.Unlock()

		return nil, errMigrationServiceUnavailable
	}
	f.migrates = append(f.migrates, req)
	onMigrate, migrateErr := f.onMigrate, f.migrateErr
	if len(f.migrateErrs) > 0 {
		migrateErr, f.migrateErrs = f.migrateErrs[0], f.migrateErrs[1:]
	}
	f.mu.Unlock()
	if onMigrate != nil {
		onMigrate(req)
	}
	if migrateErr != nil {
		return nil, migrateErr
	}

	return sandboxmigration.MigrateResponse_builder{Migration: migrationOf(req, sandboxmigration.Outcome_OUTCOME_IN_PROGRESS)}.Build(), nil
}

func (f *fakeMigrationService) GetMigration(_ context.Context, req *sandboxmigration.GetMigrationRequest) (*sandboxmigration.GetMigrationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return nil, errMigrationServiceUnavailable
	}
	if f.lostReply {
		for _, m := range f.migrates {
			if m.GetMigrationId() == req.GetMigrationId() {
				return sandboxmigration.GetMigrationResponse_builder{Migration: migrationOf(m, sandboxmigration.Outcome_OUTCOME_IN_PROGRESS)}.Build(), nil
			}
		}
	}

	return nil, status.Error(codes.NotFound, "no such migration")
}

func (f *fakeMigrationService) ListMigrations(_ context.Context, req *sandboxmigration.ListMigrationsRequest) (*sandboxmigration.ListMigrationsResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return nil, errMigrationServiceUnavailable
	}
	f.lists = append(f.lists, req)

	return sandboxmigration.ListMigrationsResponse_builder{Migrations: f.migrations}.Build(), nil
}

func (f *fakeMigrationService) setUnavailable(unavailable bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unavailable = unavailable
}

func (f *fakeMigrationService) setMigrations(migrations ...*sandboxmigration.Migration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.migrations = migrations
}

func (f *fakeMigrationService) migrateRequests() []*sandboxmigration.MigrateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*sandboxmigration.MigrateRequest(nil), f.migrates...)
}

func (f *fakeMigrationService) listRequests() []*sandboxmigration.ListMigrationsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*sandboxmigration.ListMigrationsRequest(nil), f.lists...)
}

func migrationOf(req *sandboxmigration.MigrateRequest, outcome sandboxmigration.Outcome) *sandboxmigration.Migration {
	return sandboxmigration.Migration_builder{
		MigrationId: req.GetMigrationId(),
		SandboxId:   req.GetSandboxId(),
		ExecutionId: req.GetExecutionId(),
		TeamId:      req.GetTeamId(),
		Source:      req.GetSource(),
		Target:      req.GetTarget(),
		Outcome:     outcome,
	}.Build()
}

// recordWrites records every Redis write that carries a sandbox record, by
// sandbox, whatever key or script carries it.
type recordWrites struct {
	mu       sync.Mutex
	payloads map[string][][]byte
}

func (w *recordWrites) observe(args []any) {
	for _, arg := range args {
		var raw []byte
		switch v := arg.(type) {
		case []byte:
			raw = v
		case string:
			raw = []byte(v)
		default:
			continue
		}
		var sbx sandbox.Sandbox
		if json.Unmarshal(raw, &sbx) != nil || sbx.SandboxID == "" {
			continue
		}
		w.mu.Lock()
		w.payloads[sbx.SandboxID] = append(w.payloads[sbx.SandboxID], append([]byte(nil), raw...))
		w.mu.Unlock()
	}
}

func (w *recordWrites) of(sandboxID string) [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([][]byte(nil), w.payloads[sandboxID]...)
}

func (w *recordWrites) DialHook(next redis.DialHook) redis.DialHook { return next }

func (w *recordWrites) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		w.observe(cmd.Args())

		return next(ctx, cmd)
	}
}

func (w *recordWrites) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			w.observe(cmd.Args())
		}

		return next(ctx, cmds)
	}
}

// The budgets the harness's flags set.
const (
	testOperationBudget   = 7 * time.Minute
	testTerminationBudget = 3 * time.Minute
)

type migrationHarness struct {
	flags      *ldtestdata.TestDataSource
	metrics    *sdkmetric.ManualReader
	o          *Orchestrator
	store      *sandbox.Store
	migrations *fakeMigrationService
	writes     *recordWrites
	// stops counts the execution stops the poller reported.
	stops     *recordingCollector
	clusterID uuid.UUID
	source    *nodemanager.Node
	target    *nodemanager.Node
}

// newMigrationHarness builds an Orchestrator on a Redis store, with a source
// and a target node in one cluster and the fake migration service behind a
// real gRPC connection.
func newMigrationHarness(t *testing.T) *migrationHarness {
	t.Helper()
	ctx := t.Context()

	client := redis_utils.SetupInstance(t)
	writes := &recordWrites{payloads: map[string][][]byte{}}
	client.AddHook(writes)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(ctx)
	t.Cleanup(func() { storage.Close(context.WithoutCancel(ctx)) })
	store := sandbox.NewStore(storage, redisreservations.NewReservationStorage(client, storage.Notifier()), sandbox.Callbacks{})

	migrations := &fakeMigrationService{}
	server := grpc.NewServer()
	sandboxmigration.RegisterSandboxMigrationManagerServiceServer(server, migrations)
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	flags := ldtestdata.DataSource()
	flags.Update(flags.Flag(featureflags.SandboxMigrationOperationBudgetSeconds.Key()).ValueForAll(ldvalue.Int(int(testOperationBudget.Seconds()))))
	flags.Update(flags.Flag(featureflags.SandboxMigrationTerminationBudgetSeconds.Key()).ValueForAll(ldvalue.Int(int(testTerminationBudget.Seconds()))))
	ffClient, err := featureflags.NewClientWithDatasource(flags)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ffClient.Close(context.WithoutCancel(ctx)) })

	reader := sdkmetric.NewManualReader()
	outcomes, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Int64Counter(string(telemetry.ApiOrchestratorMigrationOutcomes))
	require.NoError(t, err)

	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)
	stops := &recordingCollector{}
	o := &Orchestrator{
		sandboxStore:             store,
		nodes:                    smap.New[*nodemanager.Node](),
		featureFlagsClient:       ffClient,
		posthogClient:            posthog,
		analytics:                analyticscollector.NewAnalyticsWithClient(stops),
		migrations:               sandboxmigration.NewSandboxMigrationManagerServiceClient(conn),
		migrationPollInterval:    20 * time.Millisecond,
		migrationOutcomesCounter: outcomes,
	}

	h := &migrationHarness{flags: flags, metrics: reader, o: o, store: store, migrations: migrations, writes: writes, stops: stops, clusterID: uuid.New()}
	h.source = h.node("node-a", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()))
	h.target = h.node("node-b", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()))

	return h
}

// node registers a test node in the harness's cluster.
func (h *migrationHarness) node(id string, status api.NodeStatus, opts ...nodemanager.TestOptions) *nodemanager.Node {
	node := nodemanager.NewTestNode(id, status, 0, 8, opts...)
	node.ClusterID = h.clusterID
	h.o.registerNode(node)

	return node
}

// running stores a running Full-Router execution on the source node.
func (h *migrationHarness) running(t *testing.T) sandbox.Sandbox {
	t.Helper()

	return h.runningWith(t, func(*sandbox.Sandbox) {})
}

// runningWith stores a running Full-Router execution changed by change.
func (h *migrationHarness) runningWith(t *testing.T, change func(*sandbox.Sandbox)) sandbox.Sandbox {
	t.Helper()

	executionID := uuid.NewString()
	sbx := sandbox.Sandbox{
		SandboxID:         "sbx" + uuid.NewString()[:8],
		TemplateID:        "template",
		TeamID:            uuid.New(),
		ExecutionID:       executionID,
		NodeID:            h.source.ID,
		ClusterID:         h.clusterID,
		StartTime:         time.Now().Add(-time.Hour),
		EndTime:           time.Now().Add(time.Hour),
		MaxInstanceLength: 24 * time.Hour,
		State:             sandbox.StateRunning,
		NetworkPlacement: &orchestrator.SandboxNetworkPlacement{
			ExecutionId:    executionID,
			NetworkMode:    string(catalog.NetworkModeFullRouter),
			RouterId:       uuid.NewString(),
			IngressAddress: "10.0.0.1",
			IngressPort:    5007,
		},
	}
	change(&sbx)
	require.NoError(t, h.store.Add(t.Context(), sbx, nil))

	return sbx
}

func (h *migrationHarness) get(t *testing.T, sbx sandbox.Sandbox) sandbox.Sandbox {
	t.Helper()

	stored, err := h.store.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)

	return stored
}

// encoded is the stored record as JSON, comparable where the record's proto
// fields are not.
func (h *migrationHarness) encoded(t *testing.T, sbx sandbox.Sandbox) string {
	t.Helper()

	data, err := json.Marshal(h.get(t, sbx))
	require.NoError(t, err)

	return string(data)
}

// holdAtMigrate records the record's hold as the migration service finds it when
// Migrate arrives.
func (h *migrationHarness) holdAtMigrate(sbx sandbox.Sandbox) func() (string, int64) {
	var (
		mu    sync.Mutex
		hold  string
		until int64
	)
	h.migrations.onMigrate = func(*sandboxmigration.MigrateRequest) {
		stored, err := h.store.Get(context.Background(), sbx.TeamID, sbx.SandboxID)
		if err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		hold, until = stored.LifecycleHold, stored.LifecycleHoldUntil
	}

	return func() (string, int64) {
		mu.Lock()
		defer mu.Unlock()

		return hold, until
	}
}

// start starts a migration of sbx to the target and returns the request the
// migration service received.
func (h *migrationHarness) start(t *testing.T, sbx sandbox.Sandbox) *sandboxmigration.MigrateRequest {
	t.Helper()

	_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.NoError(t, err)
	migrates := h.migrations.migrateRequests()
	require.NotEmpty(t, migrates)

	return migrates[len(migrates)-1]
}

func TestStartHoldsTheRecordThenCallsMigrate(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)

	heldAtMigrate := h.holdAtMigrate(sbx)
	before := time.Now()
	migration, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.NoError(t, err)

	migrates := h.migrations.migrateRequests()
	require.Len(t, migrates, 1)
	req := migrates[0]
	assert.Equal(t, req.GetMigrationId(), migration.GetMigrationId(), "StartMigration returns the migration service's migration")
	assert.Equal(t, sbx.SandboxID, req.GetSandboxId())
	assert.Equal(t, sbx.ExecutionID, req.GetExecutionId())
	assert.Equal(t, sbx.TeamID.String(), req.GetTeamId())
	assert.Equal(t, sbx.NetworkPlacement.GetRouterId(), req.GetRouterId())
	assert.Equal(t, h.source.ID, req.GetSource().GetNodeId())
	assert.Equal(t, h.source.Metadata().ServiceInstanceID, req.GetSource().GetServiceInstanceId())
	assert.Equal(t, h.target.ID, req.GetTarget().GetNodeId())
	assert.Equal(t, h.target.Metadata().ServiceInstanceID, req.GetTarget().GetServiceInstanceId())

	deadline := req.GetOperationDeadline().AsTime()
	assert.WithinDuration(t, before.Add(testOperationBudget), deadline, time.Second)

	held := h.get(t, sbx)
	assert.Equal(t, req.GetMigrationId(), held.LifecycleHold)
	assert.WithinDuration(t, deadline.Add(testTerminationBudget), time.UnixMilli(held.LifecycleHoldUntil), time.Second)
	assert.Equal(t, h.source.ID, held.NodeID, "the hold does not move the record")
	holdAtMigrate, untilAtMigrate := heldAtMigrate()
	assert.Equal(t, req.GetMigrationId(), holdAtMigrate, "the record is held under the migration before the migration service acts")
	assert.Equal(t, held.LifecycleHoldUntil, untilAtMigrate)
}

func TestStartRefuses(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)

	tests := []struct {
		name    string
		prepare func(t *testing.T) sandbox.Sandbox
		target  string
		err     error
	}{
		{
			name: "a sandbox that is not running",
			prepare: func(t *testing.T) sandbox.Sandbox {
				t.Helper()
				sbx := h.running(t)
				_, _, _, err := h.store.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
				require.NoError(t, err)

				return sbx
			},
			err: errNotMigratable,
		},
		{
			name: "an execution on the worker's own network",
			prepare: func(t *testing.T) sandbox.Sandbox {
				t.Helper()
				sbx := h.running(t)
				_, err := h.store.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
					s.NetworkPlacement = nil

					return s, nil
				})
				require.NoError(t, err)

				return sbx
			},
			err: errNotMigratable,
		},
		{
			name: "an execution whose egress alone goes through a Router",
			prepare: func(t *testing.T) sandbox.Sandbox {
				t.Helper()
				sbx := h.running(t)
				_, err := h.store.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
					s.NetworkPlacement.NetworkMode = string(catalog.NetworkModeEgressRouter)

					return s, nil
				})
				require.NoError(t, err)

				return sbx
			},
			err: errNotMigratable,
		},
		{
			name: "a placement left from an earlier execution",
			prepare: func(t *testing.T) sandbox.Sandbox {
				t.Helper()
				sbx := h.running(t)
				_, err := h.store.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
					s.NetworkPlacement.ExecutionId = uuid.NewString()

					return s, nil
				})
				require.NoError(t, err)

				return sbx
			},
			err: errNotMigratable,
		},
		{
			name:    "an unknown target",
			prepare: h.running,
			target:  "node-" + uuid.NewString(),
			err:     errNodeUnavailable,
		},
		{
			name:    "the node the sandbox runs on",
			prepare: h.running,
			target:  h.source.ID,
			err:     errNotMigratable,
		},
		{
			name: "a record another operation holds",
			prepare: func(t *testing.T) sandbox.Sandbox {
				t.Helper()
				sbx := h.running(t)
				require.NoError(t, h.store.HoldExecution(t.Context(), sandbox.Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: uuid.NewString()}, time.Now().Add(time.Hour)))

				return sbx
			},
			err: sandbox.ErrHoldConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sbx := tt.prepare(t)
			before := h.encoded(t, sbx)
			target := tt.target
			if target == "" {
				target = h.target.ID
			}

			_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, target)
			require.ErrorIs(t, err, tt.err)
			assert.Empty(t, h.migrations.migrateRequests(), "the migration service is not called")
			assert.Equal(t, before, h.encoded(t, sbx), "the record is left as it was")
		})
	}
}

// A known target that cannot take requests, or that reports the
// same orchestrator process as the source, is refused like an unknown one.
func TestStartRefusesAnUnusableTarget(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)

	draining := h.node("node-c", api.NodeStatusDraining, nodemanager.WithServiceInstanceID(uuid.NewString()))
	sameProcess := h.node("node-d", api.NodeStatusReady, nodemanager.WithServiceInstanceID(h.source.Metadata().ServiceInstanceID))
	// The target is known only in another cluster than the sandbox's.
	elsewhere := nodemanager.NewTestNode("node-e", api.NodeStatusReady, 0, 8, nodemanager.WithServiceInstanceID(uuid.NewString()))
	h.o.registerNode(elsewhere)

	_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, draining.ID)
	require.ErrorIs(t, err, errNodeUnavailable)
	_, err = h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, sameProcess.ID)
	require.ErrorIs(t, err, errNotMigratable)
	_, err = h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, "node-e")
	require.ErrorIs(t, err, errNodeUnavailable)
	assert.Empty(t, h.migrations.migrateRequests())
	assert.Empty(t, h.get(t, sbx).LifecycleHold)
}

// An execution migrates only to a target whose hugepage pool has room for
// it, under the same admission a create runs.
func TestStartChecksTheTargetsMemoryPool(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	h.flags.Update(h.flags.Flag(featureflags.PlacementHugepageLimitPercent.Key()).ValueForAll(ldvalue.Int(95)))
	const pageBytes = 2 << 20
	// 2000 pages of 2 MiB; 1900 committed leaves 100 pages, less than the 512 MiB (256 pages) the sandbox needs.
	full := h.node("node-full", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()),
		nodemanager.WithHugePages(2000, 1800, 100, pageBytes), nodemanager.WithHugePagesCapacity(2000),
		nodemanager.WithMemoryTotalBytes(64<<30), nodemanager.WithMemoryUsedBytes(1<<30))
	roomy := h.node("node-roomy", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()),
		nodemanager.WithHugePages(2000, 100, 0, pageBytes), nodemanager.WithHugePagesCapacity(2000),
		nodemanager.WithMemoryTotalBytes(64<<30), nodemanager.WithMemoryUsedBytes(1<<30))

	sbx := h.runningWith(t, func(sbx *sandbox.Sandbox) {
		sbx.VCpu, sbx.RamMB = 1, 512
	})
	_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, full.ID)
	require.ErrorIs(t, err, errNodeUnavailable, "no room in the pool")
	assert.Empty(t, h.migrations.migrateRequests())
	assert.Empty(t, h.get(t, sbx).LifecycleHold)
	_, err = h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, roomy.ID)
	require.NoError(t, err)
}

// A draining source still serves the sandbox and is the usual one to migrate
// off.
func TestStartMigratesOffADrainingSource(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)
	// Registering under the source's ID replaces it.
	draining := h.node(h.source.ID, api.NodeStatusDraining, nodemanager.WithServiceInstanceID(uuid.NewString()))

	req := h.start(t, sbx)
	assert.Equal(t, draining.Metadata().ServiceInstanceID, req.GetSource().GetServiceInstanceId())
}

func TestStartReleasesTheHoldWhenMigrateFails(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)
	h.migrations.migrateErr = status.Error(codes.FailedPrecondition, "another migration of this execution is unresolved")
	heldAtMigrate := h.holdAtMigrate(sbx)

	_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "the migration service's answer reaches the caller")

	migrates := h.migrations.migrateRequests()
	require.Len(t, migrates, 1)
	holdAtMigrate, _ := heldAtMigrate()
	assert.Equal(t, migrates[0].GetMigrationId(), holdAtMigrate, "the hold was taken before the call")
	released := h.get(t, sbx)
	assert.Empty(t, released.LifecycleHold)
	assert.Equal(t, h.source.ID, released.NodeID)
}

// A Migrate that failed without a refusal, a lost reply or a timeout, may
// still be accepted by the service after the call returned: the hold stays
// whether or not the service already knows the migration, and the caller
// learns it is undecided.
func TestStartKeepsTheHoldWhenMigrateIsUndecided(t *testing.T) {
	t.Parallel()

	for name, accepted := range map[string]bool{"accepted after the reply was lost": true, "not yet accepted": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newMigrationHarness(t)
			sbx := h.running(t)
			h.migrations.migrateErr = status.Error(codes.DeadlineExceeded, "no reply")
			h.migrations.lostReply = accepted

			_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
			require.ErrorIs(t, err, errMigrationUndecided)
			migrates := h.migrations.migrateRequests()
			require.Len(t, migrates, migrateAttempts, "an undecided Migrate is tried again")
			for _, req := range migrates {
				assert.Equal(t, migrates[0].GetMigrationId(), req.GetMigrationId(), "every try names the same migration")
			}
			assert.Equal(t, migrates[0].GetMigrationId(), h.get(t, sbx).LifecycleHold, "the hold stays for the poll to resolve")
		})
	}
}

// A Migrate that timed out is tried again under the same migration ID, and
// the service's answer to the repeat is the accepted migration. The record is
// held once, before the first try.
func TestStartRetriesAnUndecidedMigrate(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)
	h.migrations.migrateErrs = []error{status.Error(codes.DeadlineExceeded, "no reply in time")}

	migration, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.NoError(t, err)

	migrates := h.migrations.migrateRequests()
	require.Len(t, migrates, 2)
	assert.Equal(t, migrates[0].GetMigrationId(), migrates[1].GetMigrationId(), "the retry names the same migration")
	assert.Equal(t, migrates[0].GetMigrationId(), migration.GetMigrationId())
	assert.Equal(t, migration.GetMigrationId(), h.get(t, sbx).LifecycleHold)
	holds := 0
	for _, payload := range h.writes.of(sbx.SandboxID) {
		var written sandbox.Sandbox
		require.NoError(t, json.Unmarshal(payload, &written))
		if written.LifecycleHold != "" {
			holds++
		}
	}
	assert.Equal(t, 1, holds, "the record is held once")
}

// Aborted says the source did not describe the execution in time and the
// manager wrote nothing. It is tried again under the same migration ID, and
// an Aborted last answer releases the hold: no try of that ID was accepted.
func TestStartReleasesTheHoldWhenMigrateStaysAborted(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)
	h.migrations.migrateErr = status.Error(codes.Aborted, "the source did not describe the execution")

	_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.Error(t, err)
	require.NotErrorIs(t, err, errMigrationUndecided)
	assert.Equal(t, codes.Aborted, status.Code(err), "the migration service's answer reaches the caller")
	assert.Contains(t, err.Error(), h.source.ID, "the error names the source")

	migrates := h.migrations.migrateRequests()
	require.Len(t, migrates, migrateAttempts, "an Aborted Migrate is tried again")
	for _, req := range migrates {
		assert.Equal(t, migrates[0].GetMigrationId(), req.GetMigrationId(), "every try names the same migration")
	}
	released := h.get(t, sbx)
	assert.Empty(t, released.LifecycleHold, "the hold is released")
	assert.Equal(t, h.source.ID, released.NodeID)
}

// An Aborted answer followed by an accepted one starts the migration and
// keeps the hold.
func TestStartRetriesAnAbortedMigrate(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)
	h.migrations.migrateErrs = []error{status.Error(codes.Aborted, "the source did not describe the execution")}

	migration, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.NoError(t, err)

	migrates := h.migrations.migrateRequests()
	require.Len(t, migrates, 2)
	assert.Equal(t, migrates[0].GetMigrationId(), migrates[1].GetMigrationId(), "the retry names the same migration")
	assert.Equal(t, migration.GetMigrationId(), h.get(t, sbx).LifecycleHold, "the hold is kept")
}

// A migration service that does not answer stops StartMigration before the record is held, so
// no hold waits out its budgets on a migration nobody started.
func TestStartHoldsNothingWhileTheMigrationServiceIsUnreachable(t *testing.T) {
	t.Parallel()
	h := newMigrationHarness(t)
	sbx := h.running(t)
	before := h.encoded(t, sbx)
	h.migrations.setUnavailable(true)

	_, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.ErrorIs(t, err, errManagerUnavailable)
	assert.Equal(t, codes.Unavailable, status.Code(err), "the migration service's answer reaches the caller")
	assert.Empty(t, h.migrations.migrateRequests(), "Migrate is not called")
	assert.Equal(t, before, h.encoded(t, sbx), "the record is not held")

	// Once GetMigration answers NotFound again, StartMigration proceeds.
	h.migrations.setUnavailable(false)
	migration, err := h.o.StartMigration(t.Context(), sbx.TeamID, sbx.SandboxID, h.target.ID)
	require.NoError(t, err)
	assert.Len(t, h.migrations.migrateRequests(), 1)
	assert.Equal(t, migration.GetMigrationId(), h.get(t, sbx).LifecycleHold)
}

// Without a migration client every migration call reports that sandbox
// migration is not configured, and the poller returns at once.
func TestMigrationWithoutAClientIsNotConfigured(t *testing.T) {
	t.Parallel()
	o := &Orchestrator{}

	_, err := o.StartMigration(t.Context(), uuid.New(), "sbx", "node-b")
	require.ErrorIs(t, err, errMigrationNotConfigured)
	_, err = o.GetMigration(t.Context(), uuid.NewString())
	require.ErrorIs(t, err, errMigrationNotConfigured)
	_, err = o.TerminateMigration(t.Context(), uuid.NewString())
	require.ErrorIs(t, err, errMigrationNotConfigured)
	assert.Contains(t, err.Error(), "sandbox migration is not configured")

	done := make(chan struct{})
	go func() {
		o.RunMigrationPoller(t.Context())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunMigrationPoller did not return without a client")
	}
}

// outcomes returns how many times the migration outcomes counter saw the
// outcome and action pair.
func (h *migrationHarness) outcomes(t *testing.T, outcome sandboxmigration.Outcome, action string) int {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, h.metrics.Collect(t.Context(), &collected))
	want := attribute.NewSet(attribute.String("outcome", outcome.String()), attribute.String("action", action))
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || m.Name != string(telemetry.ApiOrchestratorMigrationOutcomes) {
				continue
			}
			for _, point := range sum.DataPoints {
				if point.Attributes.Equals(&want) {
					return int(point.Value)
				}
			}
		}
	}

	return 0
}
