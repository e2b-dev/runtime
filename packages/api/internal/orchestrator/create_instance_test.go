package orchestrator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
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
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	e2bcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// testBuild returns a minimal queries.EnvBuild that satisfies CreateSandbox.
func testBuild() queries.EnvBuild {
	diskSize := int64(1024)
	envdVer := "0.1.0"

	return queries.EnvBuild{
		ID:                 uuid.New(),
		Vcpu:               2,
		RamMb:              512,
		TotalDiskSizeMb:    &diskSize,
		KernelVersion:      "5.10",
		FirecrackerVersion: "v1.7.0_abc1234",
		EnvdVersion:        &envdVer,
	}
}

// newCreateSandboxTestOrchestrator constructs the minimal Orchestrator needed for
// CreateSandbox, with a single ready node already registered.
func newCreateSandboxTestOrchestrator(t *testing.T, nodeOpts ...nodemanager.TestOptions) *Orchestrator {
	t.Helper()

	return newCreateSandboxTestOrchestratorWithFlags(t, ldtestdata.DataSource(), nodeOpts...)
}

// newCreateSandboxTestOrchestratorWithFlags is newCreateSandboxTestOrchestrator
// with a caller-supplied flag source, for tests that drive a feature flag.
func newCreateSandboxTestOrchestratorWithFlags(t *testing.T, flagSource *ldtestdata.TestDataSource, nodeOpts ...nodemanager.TestOptions) *Orchestrator {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider(), nil)
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	store := sandbox.NewStore(
		storage,
		redisreservations.NewReservationStorage(client, storage.Notifier()),
		sandbox.Callbacks{
			AddSandboxToRoutingTable: func(context.Context, sandbox.Sandbox) {},
			AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
		},
	)

	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")
	counter, _ := meter.Int64Counter("test-created-sandboxes")

	ffClient, ffErr := featureflags.NewClientWithDatasource(flagSource)
	require.NoError(t, ffErr)

	algo := placement.NewBestOfK(placement.DefaultBestOfKConfig()).(*placement.BestOfK)

	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8, nodeOpts...)
	node.ClusterID = uuid.Nil // match consts.LocalClusterID (fallback when team.ClusterID is nil)

	o := &Orchestrator{
		sandboxStore:            store,
		nodes:                   smap.New[*nodemanager.Node](),
		placementAlgorithm:      algo,
		featureFlagsClient:      ffClient,
		createdSandboxesCounter: counter,
		routingCatalog:          e2bcatalog.NewRedisSandboxCatalog(client),
	}

	o.registerNode(node)

	return o
}

func testTeam() *teamtypes.Team {
	return &teamtypes.Team{
		Team: &authqueries.Team{
			ID:   uuid.New(),
			Name: "test-team",
		},
		Limits: &teamtypes.TeamLimits{
			SandboxConcurrency: 10,
			MaxLengthHours:     24,
		},
	}
}

func TestBuildEgressConfigPreservesWildcardRuleKey(t *testing.T) {
	t.Parallel()

	const domain = "*.github.com"
	config := buildEgressConfig(nil, nil, map[string][]dbtypes.SandboxNetworkRule{
		domain: {{Transform: &dbtypes.SandboxNetworkTransform{Headers: map[string]string{"X-Test": "value"}}}},
	})

	rules, ok := config.GetRules()[domain]
	require.True(t, ok)
	require.Len(t, rules.GetRules(), 1)
	assert.Equal(t, "value", rules.GetRules()[0].GetTransform().GetHeaders()["X-Test"])
}

func TestBuildNetworkConfigHTTPSPorts(t *testing.T) {
	t.Parallel()

	httpsPorts := []uint32{443, 8443}
	network := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{
		Ingress: &dbtypes.SandboxNetworkIngressConfig{HTTPSPorts: httpsPorts},
	}, nil, nil)

	assert.Equal(t, httpsPorts, network.GetIngress().GetHttpsPorts())
}

// TestCreateSandbox_StaleDataAfterConcurrentPause exercises CreateSandbox with
// two sequential resume attempts where the snapshot changes between them.
//
// Because CreateSandbox accepts a SandboxDataFetcher callback that is invoked
// AFTER the concurrency lock is acquired, the second call reads the fresh V2
// data even though the snapshot was mutated between calls.
func TestCreateSandbox_StaleDataAfterConcurrentPause(t *testing.T) {
	t.Parallel()

	t.Run("lazy fetcher reads fresh data after lock", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t)
		team := testTeam()
		sandboxID := "sbx-race-" + uuid.New().String()[:8]
		build := testBuild()
		now := time.Now()

		// Mutable snapshot source simulating the cache.
		type snapshot struct {
			templateID string
			metadata   map[string]string
		}

		snap := &snapshot{
			templateID: "tpl-v1",
			metadata:   map[string]string{"snapshot": "v1"},
		}

		// The fetcher closure captures the mutable snap pointer and reads
		// current values at call time (after Reserve() acquires the lock).
		makeFetcher := func() SandboxDataFetcher {
			return func(_ context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{
					TemplateID:     snap.templateID,
					BaseTemplateID: "base-tpl",
					Metadata:       snap.metadata,
					Build:          build,
				}, nil
			}
		}

		// Resume-1: fetcher will read V1.
		sbx1, apiErr := o.CreateSandbox(
			t.Context(),
			sandboxID,
			uuid.New().String(),
			team,
			makeFetcher(),
			now,
			now.Add(time.Hour),
			time.Hour,
			true,
			false,
			sandbox.CreationMetadata{IsResume: true},
		)
		require.Nil(t, apiErr)
		assert.Equal(t, "tpl-v1", sbx1.TemplateID)
		assert.Equal(t, "base-tpl", sbx1.BaseTemplateID)

		// Clean up reservation.
		o.sandboxStore.Remove(t.Context(), team.Team.ID, sandboxID, sbx1.ExecutionID)

		// Snapshot changes to V2.
		snap.templateID = "tpl-v2"
		snap.metadata = map[string]string{"snapshot": "v2"}

		// Resume-2: fetcher will read V2 because it runs after Reserve().
		sbx2, apiErr := o.CreateSandbox(
			t.Context(),
			sandboxID,
			uuid.New().String(),
			team,
			makeFetcher(),
			now,
			now.Add(time.Hour),
			time.Hour,
			true,
			false,
			sandbox.CreationMetadata{IsResume: true},
		)
		require.Nil(t, apiErr)

		// The sandbox SHOULD have been created with V2 (fresh) data.
		assert.Equal(t, "tpl-v2", sbx2.TemplateID,
			"CreateSandbox must use the latest snapshot data, not stale pre-lock values")
		assert.Equal(t, "base-tpl", sbx2.BaseTemplateID,
			"CreateSandbox must preserve the base template ID")
		assert.Equal(t, "v2", sbx2.Metadata["snapshot"],
			"CreateSandbox must use the latest metadata, not stale pre-lock values")
	})
}

// A joined start rides whatever is already in flight, which may be a memory
// restore — an explicit filesystem-boot demand must be refused, not silently
// answered with the other start's result.
func TestCreateSandbox_FilesystemBootDemandRefusesJoin(t *testing.T) {
	t.Parallel()

	o := newCreateSandboxTestOrchestrator(t)
	team := testTeam()
	sandboxID := "sbx-join-" + uuid.New().String()[:8]
	build := testBuild()
	now := time.Now()

	winnerEntered := make(chan struct{})
	winnerRelease := make(chan struct{})
	var winnerFetcher SandboxDataFetcher = func(_ context.Context) (SandboxMetadata, *api.APIError) {
		close(winnerEntered)
		<-winnerRelease

		return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
	}

	winnerDone := make(chan struct{})
	go func() {
		defer close(winnerDone)
		o.CreateSandbox(t.Context(), sandboxID, uuid.New().String(), team, winnerFetcher,
			now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})
	}()

	// The winner holds the reservation while blocked in its fetcher.
	<-winnerEntered

	joinerDone := make(chan *api.APIError, 1)
	go func() {
		_, joinErr := o.CreateSandbox(t.Context(), sandboxID, uuid.New().String(), team,
			func(_ context.Context) (SandboxMetadata, *api.APIError) {
				t.Error("joiner must not fetch data")

				return SandboxMetadata{}, nil
			},
			now, now.Add(time.Hour), time.Hour, true, true, sandbox.CreationMetadata{IsResume: true})
		joinerDone <- joinErr
	}()

	select {
	case apiErr := <-joinerDone:
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusConflict, apiErr.Code)
	case <-time.After(20 * time.Second):
		t.Fatal("joiner did not return: it joined the in-flight start instead of being refused")
	}

	close(winnerRelease)
	<-winnerDone
}

// The demand is only proven honored by the response echo: a node that predates
// the field succeeds without echoing, and the resume must fail loudly instead
// of handing back a silent memory restore.
func TestCreateSandbox_UnconfirmedFilesystemBootFailsLoud(t *testing.T) {
	t.Parallel()

	fetcher := func(build queries.EnvBuild) SandboxDataFetcher {
		return func(_ context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build, FilesystemBoot: true}, nil
		}
	}

	t.Run("legacy node without echo: 503, nothing handed back", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithLegacySandboxClient())
		team := testTeam()
		now := time.Now()

		_, apiErr := o.CreateSandbox(t.Context(), "sbx-echo-"+uuid.New().String()[:8], uuid.New().String(), team,
			fetcher(testBuild()), now, now.Add(time.Hour), time.Hour, true, true, sandbox.CreationMetadata{IsResume: true})

		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
	})

	t.Run("echoing node: demand served", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t)
		team := testTeam()
		now := time.Now()

		_, apiErr := o.CreateSandbox(t.Context(), "sbx-echo-"+uuid.New().String()[:8], uuid.New().String(), team,
			fetcher(testBuild()), now, now.Add(time.Hour), time.Hour, true, true, sandbox.CreationMetadata{IsResume: true})

		assert.Nil(t, apiErr)
	})
}

// TestCreateSandbox_StoresResolvedFirecrackerVersion pins the mechanism the
// version gates are built on: when the orchestrator echoes the resolved
// Firecracker version on the create response, the sandbox record must carry
// THAT value (marked resolved) — a regression that silently kept the declared
// build version would put the gates back to approximating. A node that
// predates the echo keeps the declared version, unmarked.
func TestCreateSandbox_StoresResolvedFirecrackerVersion(t *testing.T) {
	t.Parallel()

	fetcher := func(build queries.EnvBuild) SandboxDataFetcher {
		return func(_ context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
		}
	}

	t.Run("echoing node: record carries the resolved version", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t)
		team := testTeam()
		now := time.Now()
		build := testBuild()

		sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-fcv-"+uuid.New().String()[:8], uuid.New().String(), team,
			fetcher(build), now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})
		require.Nil(t, apiErr)

		assert.Equal(t, nodemanager.MockResolvedFirecrackerVersion, sbx.FirecrackerVersion,
			"the record must store the orchestrator's echo, not the declared build version")
		assert.NotEqual(t, build.FirecrackerVersion, sbx.FirecrackerVersion,
			"the mock echo is deliberately distinct from the declared version")
		assert.True(t, sbx.FirecrackerVersionResolved)
	})

	t.Run("legacy node without echo: declared version, unmarked", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithLegacySandboxClient())
		team := testTeam()
		now := time.Now()
		build := testBuild()

		sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-fcv-"+uuid.New().String()[:8], uuid.New().String(), team,
			fetcher(build), now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})
		require.Nil(t, apiErr)

		assert.Equal(t, build.FirecrackerVersion, sbx.FirecrackerVersion)
		assert.False(t, sbx.FirecrackerVersionResolved)
	})
}

// buildOnCPU returns testBuild() recorded as having been built on cpuModel.
func buildOnCPU(cpuModel string) queries.EnvBuild {
	build := testBuild()
	arch, family := "x86_64", "6"
	build.CpuArchitecture = &arch
	build.CpuFamily = &family
	build.CpuModel = &cpuModel

	return build
}

// fsOnlyPinFlagSource serves fs-only-resume-cpu-model as cpuModel.
func fsOnlyPinFlagSource(cpuModel string) *ldtestdata.TestDataSource {
	source := ldtestdata.DataSource()
	source.Update(source.Flag("fs-only-resume-cpu-model").ValueForAll(ldvalue.String(cpuModel)))

	return source
}

// A filesystem-only snapshot is held to its build's CPU model, so the
// cross-generation upgrade a memory restore may take is withheld from it. The
// pin has to beat snapshot node affinity too: the origin node reaches placement
// as the preferred node, which otherwise skips the CPU filter.
func TestCreateSandbox_FilesystemOnlySnapshotPinsCPUModel(t *testing.T) {
	t.Parallel()

	resumeFetcher := func(nodeID string, filesystemOnly bool) SandboxDataFetcher {
		return func(_ context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{
				TemplateID:             "tpl",
				BaseTemplateID:         "base-tpl",
				Build:                  buildOnCPU(machineinfo.IceLakeModel),
				NodeID:                 &nodeID,
				FilesystemOnlySnapshot: filesystemOnly,
			}, nil
		}
	}

	tests := []struct {
		name string
		// nil exercises the flag's own default rather than an injected value.
		pinnedModel    *string
		nodeCPUModel   string
		filesystemOnly bool
		wantErrCode    string
	}{
		{"fs-only, pinned to n2: n4 node refused", new(machineinfo.IceLakeModel), machineinfo.EmeraldRapidsModel, true, errCodeNoCompatibleNode},
		{"fs-only, empty pin: n4 node accepted", new(""), machineinfo.EmeraldRapidsModel, true, ""},
		{"memory snapshot, pinned to n2: n4 node accepted", new(machineinfo.IceLakeModel), machineinfo.EmeraldRapidsModel, false, ""},
		{"fs-only, pinned to n2, n2 node: affinity kept", new(machineinfo.IceLakeModel), machineinfo.IceLakeModel, true, ""},
		// A deployment with no LaunchDarkly must place as it did before the pin
		// existed, not strand every filesystem-only resume on a compiled default.
		{"fs-only, no flag rule: unpinned", nil, machineinfo.EmeraldRapidsModel, true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flagSource := ldtestdata.DataSource()
			if tt.pinnedModel != nil {
				flagSource = fsOnlyPinFlagSource(*tt.pinnedModel)
			}

			o := newCreateSandboxTestOrchestratorWithFlags(t, flagSource,
				nodemanager.WithCPUInfo("x86_64", "6", tt.nodeCPUModel))
			now := time.Now()

			_, apiErr := o.CreateSandbox(t.Context(), "sbx-pin-"+uuid.New().String()[:8], uuid.New().String(),
				testTeam(), resumeFetcher("node-1", tt.filesystemOnly),
				now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})

			if tt.wantErrCode == "" {
				assert.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantErrCode, apiErr.ErrorCode)
			assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
		})
	}
}

// A build predating CPU recording is still subject to the pin: the pin matches
// on what the node reports, so there is no build machine info to exempt it.
func TestCreateSandbox_FilesystemOnlySnapshotWithoutBuildCPUStillPinned(t *testing.T) {
	t.Parallel()

	resume := func(nodeID string) SandboxDataFetcher {
		return func(_ context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{
				TemplateID:             "tpl",
				BaseTemplateID:         "base-tpl",
				Build:                  testBuild(), // no CPU columns recorded
				NodeID:                 &nodeID,
				FilesystemOnlySnapshot: true,
			}, nil
		}
	}

	tests := []struct {
		name         string
		nodeCPUModel string
		wantErrCode  string
	}{
		{"node reports the pinned model", machineinfo.IceLakeModel, ""},
		{"node reports another model", machineinfo.EmeraldRapidsModel, errCodeNoCompatibleNode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := newCreateSandboxTestOrchestratorWithFlags(t, fsOnlyPinFlagSource(machineinfo.IceLakeModel),
				nodemanager.WithCPUInfo("x86_64", "6", tt.nodeCPUModel))
			now := time.Now()

			_, apiErr := o.CreateSandbox(t.Context(), "sbx-nocpu-"+uuid.New().String()[:8], uuid.New().String(),
				testTeam(), resume("node-1"),
				now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})

			if tt.wantErrCode == "" {
				assert.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantErrCode, apiErr.ErrorCode)
		})
	}
}
