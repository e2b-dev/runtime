package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	apiclusters "github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
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

	return newCreateSandboxTestOrchestratorOnRedis(t, redis_utils.SetupInstance(t), flagSource, nodeOpts...)
}

// newCreateSandboxTestOrchestratorOnRedis builds one API instance on client, so
// several instances can share a Redis server.
func newCreateSandboxTestOrchestratorOnRedis(t *testing.T, client redis.UniversalClient, flagSource *ldtestdata.TestDataSource, nodeOpts ...nodemanager.TestOptions) *Orchestrator {
	t.Helper()

	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	store := sandbox.NewStore(
		storage,
		redisreservations.NewReservationStorage(client, storage.Notifier()),
		sandbox.Callbacks{
			AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
		},
	)

	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")
	counter, _ := meter.Int64Counter("test-created-sandboxes")
	noCeilingCounter, _ := meter.Int64Counter("test-placements-without-hugepage-ceiling")

	ffClient, ffErr := featureflags.NewClientWithDatasource(flagSource)
	require.NoError(t, ffErr)

	algo := placement.NewBestOfK(placement.DefaultBestOfKConfig()).(*placement.BestOfK)

	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8, nodeOpts...)
	node.ClusterID = uuid.Nil // match consts.LocalClusterID (fallback when team.ClusterID is nil)

	o := &Orchestrator{
		sandboxStore:               store,
		nodes:                      smap.New[*nodemanager.Node](),
		placementAlgorithm:         algo,
		featureFlagsClient:         ffClient,
		createdSandboxesCounter:    counter,
		noCeilingPlacementsCounter: noCeilingCounter,
	}
	o.featureUnsupportedCounter, _ = meter.Int64Counter("test-feature-unsupported")

	o.registerNode(node)

	return o
}

func testTeam() *teamtypes.Team {
	return &teamtypes.Team{
		Team: &dbtypes.Team{
			ID:   uuid.New(),
			Name: "test-team",
		},
		Limits: &teamtypes.TeamLimits{
			SandboxConcurrency: 10,
			MaxLengthHours:     24,
			MaxVcpu:            8,
		},
	}
}

func TestBuildEgressConfigPreservesWildcardRuleKey(t *testing.T) {
	t.Parallel()

	const domain = "*.github.com"
	config := buildEgressConfig(nil, nil, map[string][]dbtypes.SandboxNetworkRule{
		domain: {{Transform: &dbtypes.SandboxNetworkTransform{Headers: map[string]string{"X-Test": "value"}}}},
	}, nil)

	rules, ok := config.GetRules()[domain]
	require.True(t, ok)
	require.Len(t, rules.GetRules(), 1)
	assert.Equal(t, "value", rules.GetRules()[0].GetTransform().GetHeaders()["X-Test"])
}

// The TLS block has to survive validated config -> stored form -> wire. A
// field-by-field copy that misses a hop leaves the API accepting TLS while the
// orchestrator dials in the clear.
func TestEgressProxyTLSRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("enabled block reaches the wire", func(t *testing.T) {
		t.Parallel()

		stored := &dbtypes.SandboxNetworkEgressConfig{}
		ApplyValidatedEgressProxy(stored, &sandbox_network.EgressProxyConfig{
			Address:  "proxy.example.com:1080",
			Username: "alice",
			Password: "s3cret",
			TLS: &sandbox_network.EgressProxyTLSConfig{
				Enabled:    true,
				ServerName: "cert-name.test",
				CACert:     "-----BEGIN CERTIFICATE-----\nstub\n-----END CERTIFICATE-----",
			},
		})

		require.NotNil(t, stored.EgressProxyTLS, "stored config must keep the TLS block")
		assert.True(t, stored.EgressProxyTLS.Enabled)

		wire := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{Egress: stored}, nil, nil)
		proxyTLS := wire.GetEgress().GetEgressProxyTls()

		require.NotNil(t, proxyTLS, "the orchestrator must be told the hop is TLS")
		assert.True(t, proxyTLS.GetEnabled())
		assert.Equal(t, "cert-name.test", proxyTLS.GetServerName())
		assert.Equal(t, stored.EgressProxyTLS.CACert, string(proxyTLS.GetCaCert()))
	})

	t.Run("proxy without TLS stays plaintext", func(t *testing.T) {
		t.Parallel()

		stored := &dbtypes.SandboxNetworkEgressConfig{}
		ApplyValidatedEgressProxy(stored, &sandbox_network.EgressProxyConfig{
			Address: "proxy.example.com:1080",
		})

		assert.Nil(t, stored.EgressProxyTLS)

		wire := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{Egress: stored}, nil, nil)
		assert.Nil(t, wire.GetEgress().GetEgressProxyTls(),
			"an existing BYOP config must not acquire TLS it never asked for")
		assert.Equal(t, "proxy.example.com:1080", wire.GetEgress().GetEgressProxyAddress())
	})

	t.Run("nil config clears the proxy", func(t *testing.T) {
		t.Parallel()

		// A PUT that omits egressProxy arrives as a fresh config and a nil
		// source.
		stored := &dbtypes.SandboxNetworkEgressConfig{}
		ApplyValidatedEgressProxy(stored, nil)

		assert.Empty(t, stored.EgressProxyAddress)
		assert.Nil(t, stored.EgressProxyTLS)
	})
}

const defaultNameserverCIDR = sandbox_network.DefaultNameserver + "/32"

func TestBuildNetworkConfigDnsServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		allowed    []string
		dnsServers *[]string
		want       []string
		wantNot    []string
	}{
		{"domain allows the default nameserver when unset", []string{"example.com"}, nil, []string{defaultNameserverCIDR}, nil},
		{"empty list allows no nameserver", []string{"example.com"}, &[]string{}, nil, []string{defaultNameserverCIDR}},
		{"listed servers replace the default", []string{"example.com"}, &[]string{"1.1.1.1", "9.9.9.9"}, []string{"1.1.1.1/32", "9.9.9.9/32"}, []string{defaultNameserverCIDR}},
		{"explicit allowOut entry wins over an empty list", []string{"example.com", "8.8.8.8"}, &[]string{}, []string{defaultNameserverCIDR}, nil},
		{"ip-only allow list never gets the default", []string{"1.1.1.1"}, nil, nil, []string{defaultNameserverCIDR}},
		{"ip-only allow list ignores listed servers", []string{"1.1.1.1"}, &[]string{"9.9.9.9"}, nil, []string{"9.9.9.9/32"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wire := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{
				Egress: &dbtypes.SandboxNetworkEgressConfig{
					AllowedAddresses: tt.allowed,
					DeniedAddresses:  []string{sandbox_network.AllInternetTrafficCIDR},
					DnsServers:       tt.dnsServers,
				},
			}, nil, nil)

			cidrs := wire.GetEgress().GetAllowedCidrs()
			for _, cidr := range tt.want {
				assert.Contains(t, cidrs, cidr)
			}
			for _, cidr := range tt.wantNot {
				assert.NotContains(t, cidrs, cidr)
			}
		})
	}
}

// The firewall rejects overlapping ranges, so a DNS server that an allowed
// CIDR already covers, or that is listed twice, must only be sent once.
func TestBuildNetworkConfigDnsServersSkipsCoveredServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		allowed    []string
		dnsServers *[]string
		want       []string
	}{
		{"default inside an allowed subnet", []string{"example.com", "8.8.8.0/24"}, nil, []string{"8.8.8.0/24"}},
		{"default also listed in allowOut", []string{"example.com", "8.8.8.8"}, nil, []string{defaultNameserverCIDR}},
		{"listed server inside an allowed subnet", []string{"example.com", "1.1.1.0/24"}, &[]string{"1.1.1.1", "9.9.9.9"}, []string{"1.1.1.0/24", "9.9.9.9/32"}},
		{"repeated server", []string{"example.com"}, &[]string{"1.1.1.1", "1.1.1.1"}, []string{"1.1.1.1/32"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wire := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{
				Egress: &dbtypes.SandboxNetworkEgressConfig{
					AllowedAddresses: tt.allowed,
					DeniedAddresses:  []string{sandbox_network.AllInternetTrafficCIDR},
					DnsServers:       tt.dnsServers,
				},
			}, nil, nil)

			assert.Equal(t, tt.want, wire.GetEgress().GetAllowedCidrs())
		})
	}
}

// Resume, fork and auto-resume rebuild egress from the stored snapshot config,
// so the list has to survive a JSON round trip, including an empty one.
func TestDnsServersSurvivePausedConfig(t *testing.T) {
	t.Parallel()

	roundTrip := func(t *testing.T, dnsServers *[]string) *dbtypes.SandboxNetworkEgressConfig {
		t.Helper()

		data, err := json.Marshal(dbtypes.PausedSandboxConfig{
			Version: dbtypes.PausedSandboxConfigVersion,
			Network: &dbtypes.SandboxNetworkConfig{Egress: &dbtypes.SandboxNetworkEgressConfig{
				AllowedAddresses: []string{"example.com"},
				DnsServers:       dnsServers,
			}},
		})
		require.NoError(t, err)

		var resumed dbtypes.PausedSandboxConfig
		require.NoError(t, json.Unmarshal(data, &resumed))
		require.NotNil(t, resumed.Network)
		require.NotNil(t, resumed.Network.Egress)

		return resumed.Network.Egress
	}

	t.Run("empty list stays empty after resume", func(t *testing.T) {
		t.Parallel()

		egress := roundTrip(t, &[]string{})
		require.NotNil(t, egress.DnsServers)
		assert.Empty(t, *egress.DnsServers)

		wire := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{Egress: egress}, nil, nil)
		assert.NotContains(t, wire.GetEgress().GetAllowedCidrs(), defaultNameserverCIDR)
	})

	t.Run("listed servers stay after resume", func(t *testing.T) {
		t.Parallel()

		egress := roundTrip(t, &[]string{"1.1.1.1"})
		require.NotNil(t, egress.DnsServers)
		assert.Equal(t, []string{"1.1.1.1"}, *egress.DnsServers)

		cidrs := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{Egress: egress}, nil, nil).GetEgress().GetAllowedCidrs()
		assert.Contains(t, cidrs, "1.1.1.1/32")
		assert.NotContains(t, cidrs, defaultNameserverCIDR)
	})

	t.Run("unset list keeps the default", func(t *testing.T) {
		t.Parallel()

		egress := roundTrip(t, nil)
		assert.Nil(t, egress.DnsServers)

		wire := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{Egress: egress}, nil, nil)
		assert.Contains(t, wire.GetEgress().GetAllowedCidrs(), defaultNameserverCIDR)
	})
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

// buildOnIceLake returns testBuild() recorded as having been built on an n2 (Ice Lake) host.
func buildOnIceLake() queries.EnvBuild {
	build := testBuild()
	arch, family := "x86_64", "6"
	build.CpuArchitecture = &arch
	build.CpuFamily = &family
	cpuModel := machineinfo.IceLakeModel
	build.CpuModel = &cpuModel

	return build
}

// fsOnlyPinFlagSource serves fs-only-resume-cpu-model as cpuModel.
func fsOnlyPinFlagSource(cpuModel string) *ldtestdata.TestDataSource {
	source := ldtestdata.DataSource()
	source.Update(source.Flag("fs-only-resume-cpu-model").ValueForAll(ldvalue.String(cpuModel)))

	return source
}

// Only a resume is pinned to the node its snapshot was taken on. A start that
// names that node but is not a resume (a fork) is placed by load, so the forks
// of one sandbox are not all sent to the original's node.
func TestCreateSandbox_NodeAffinityOnlyForResume(t *testing.T) {
	t.Parallel()

	fromSnapshotOn := func(nodeID string) SandboxDataFetcher {
		return func(_ context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{
				TemplateID:     "tpl",
				BaseTemplateID: "base-tpl",
				Build:          testBuild(),
				NodeID:         &nodeID,
			}, nil
		}
	}

	tests := []struct {
		name     string
		isResume bool
		wantNode string
	}{
		{"resume stays on the snapshot's node", true, "node-origin"},
		{"fork is placed on the less loaded node", false, "node-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := newCreateSandboxTestOrchestrator(t)
			// Loaded well past node-1, so best-of-K never prefers it.
			origin := nodemanager.NewTestNode("node-origin", api.NodeStatusReady, 30, 8)
			origin.ClusterID = uuid.Nil
			o.registerNode(origin)
			now := time.Now()

			sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-affinity-"+uuid.New().String()[:8], uuid.New().String(),
				testTeam(), fromSnapshotOn(origin.ID),
				now, now.Add(time.Hour), time.Hour, tt.isResume, false, sandbox.CreationMetadata{IsResume: tt.isResume})

			require.Nil(t, apiErr)
			assert.Equal(t, tt.wantNode, sbx.NodeID)
		})
	}
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
				Build:                  buildOnIceLake(),
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

// A memory:false resume of a memory snapshot cold-boots too, so it is pinned
// like a filesystem-only snapshot; a plain memory restore is not.
func TestCreateSandbox_MemoryFalseResumePinsCPUModel(t *testing.T) {
	t.Parallel()

	extra := ldvalue.FromJSONMarshal(map[string][]string{"106": {"173"}})

	tests := []struct {
		name           string
		pin            string
		extra          ldvalue.Value
		filesystemBoot bool
		nodeCPUModel   string
		wantErrCode    string
	}{
		{"memory:false, pinned to n2: n4 node refused", machineinfo.IceLakeModel, ldvalue.Null(), true, machineinfo.EmeraldRapidsModel, errCodeNoCompatibleNode},
		{"memory:false, pinned to n2: n2 node accepted", machineinfo.IceLakeModel, ldvalue.Null(), true, machineinfo.IceLakeModel, ""},
		{"memory:false, empty pin: n4 node accepted", "", ldvalue.Null(), true, machineinfo.EmeraldRapidsModel, ""},
		{"memory:false, pinned to n2, extra pair: c4 node refused", machineinfo.IceLakeModel, extra, true, machineinfo.GraniteRapidsModel, errCodeNoCompatibleNode},
		{"memory restore, pinned to n2: n4 node accepted", machineinfo.IceLakeModel, ldvalue.Null(), false, machineinfo.EmeraldRapidsModel, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := fsOnlyPinFlagSource(tt.pin)
			source.Update(source.Flag("extra-compatible-node-models").ValueForAll(tt.extra))

			o := newCreateSandboxTestOrchestratorWithFlags(t, source,
				nodemanager.WithCPUInfo("x86_64", "6", tt.nodeCPUModel))
			now := time.Now()
			nodeID := "node-1"

			_, apiErr := o.CreateSandbox(t.Context(), "sbx-memfalse-"+uuid.New().String()[:8], uuid.New().String(),
				testTeam(), func(_ context.Context) (SandboxMetadata, *api.APIError) {
					return SandboxMetadata{
						TemplateID:     "tpl",
						BaseTemplateID: "base-tpl",
						Build:          buildOnIceLake(),
						NodeID:         &nodeID,
						FilesystemBoot: tt.filesystemBoot,
					}, nil
				},
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

// A sandbox created from a memory:false snapshot template cold-boots, so it is
// pinned like a filesystem-only resume. Such a create has no snapshot row; only
// the build records the kind.
func TestCreateSandbox_FilesystemOnlyBuildPinsCPUModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filesystemOnly bool
		nodeCPUModel   string
		wantErrCode    string
	}{
		{"fs-only build, pinned to n2: n4 node refused", true, machineinfo.EmeraldRapidsModel, errCodeNoCompatibleNode},
		{"fs-only build, pinned to n2: n2 node accepted", true, machineinfo.IceLakeModel, ""},
		{"memory build, pinned to n2: n4 node accepted", false, machineinfo.EmeraldRapidsModel, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := newCreateSandboxTestOrchestratorWithFlags(t, fsOnlyPinFlagSource(machineinfo.IceLakeModel),
				nodemanager.WithCPUInfo("x86_64", "6", tt.nodeCPUModel))
			build := buildOnIceLake()
			build.FilesystemOnly = tt.filesystemOnly
			now := time.Now()

			_, apiErr := o.CreateSandbox(t.Context(), "sbx-fsbuild-"+uuid.New().String()[:8], uuid.New().String(),
				testTeam(), func(_ context.Context) (SandboxMetadata, *api.APIError) {
					return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "tpl", Build: build}, nil
				},
				now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})

			if tt.wantErrCode == "" {
				assert.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantErrCode, apiErr.ErrorCode)
		})
	}
}

// extra-compatible-node-models lets an n2 build run on a Granite Rapids node
// for memory and filesystem-only resumes alike, and adds nothing when unset.
func TestCreateSandbox_ExtraCompatibleNodeModels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		extra          ldvalue.Value
		filesystemOnly bool
		wantErrCode    string
	}{
		{"memory, flag unset: refused", ldvalue.Null(), false, errCodeNoCompatibleNode},
		{"memory, pair listed: accepted", ldvalue.FromJSONMarshal(map[string][]string{"106": {"173"}}), false, ""},
		{"fs-only, pair listed: accepted", ldvalue.FromJSONMarshal(map[string][]string{"106": {"173"}}), true, ""},
		{"memory, other pair listed: refused", ldvalue.FromJSONMarshal(map[string][]string{"207": {"173"}}), false, errCodeNoCompatibleNode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			source.Update(source.Flag("extra-compatible-node-models").ValueForAll(tt.extra))

			o := newCreateSandboxTestOrchestratorWithFlags(t, source,
				nodemanager.WithCPUInfo("x86_64", "6", machineinfo.GraniteRapidsModel))
			now := time.Now()
			nodeID := "node-1"

			_, apiErr := o.CreateSandbox(t.Context(), "sbx-extra-"+uuid.New().String()[:8], uuid.New().String(),
				testTeam(), func(_ context.Context) (SandboxMetadata, *api.APIError) {
					return SandboxMetadata{
						TemplateID:             "tpl",
						BaseTemplateID:         "base-tpl",
						Build:                  buildOnIceLake(),
						NodeID:                 &nodeID,
						FilesystemOnlySnapshot: tt.filesystemOnly,
					}, nil
				},
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

// createOnNodeModel resumes an n2-built snapshot on a single node reporting
// nodeModel, and returns the placement error.
func createOnNodeModel(t *testing.T, source *ldtestdata.TestDataSource, team *teamtypes.Team, nodeModel string, filesystemOnly bool) *api.APIError {
	t.Helper()

	o := newCreateSandboxTestOrchestratorWithFlags(t, source, nodemanager.WithCPUInfo("x86_64", "6", nodeModel))
	now := time.Now()
	nodeID := "node-1"

	_, apiErr := o.CreateSandbox(t.Context(), "sbx-extra-"+uuid.New().String()[:8], uuid.New().String(),
		team, func(_ context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{
				TemplateID:             "tpl",
				BaseTemplateID:         "base-tpl",
				Build:                  buildOnIceLake(),
				NodeID:                 &nodeID,
				FilesystemOnlySnapshot: filesystemOnly,
			}, nil
		},
		now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})

	return apiErr
}

// A rule targeting one cluster admits the extra pair for that cluster's teams
// only (a team without a cluster is on the local cluster), so the cluster
// context must reach the flag evaluation.
func TestCreateSandbox_ExtraCompatibleNodeModelsTargetsCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		targeted    uuid.UUID
		wantErrCode string
	}{
		{"rule targets the team's cluster: accepted", consts.LocalClusterID, ""},
		{"rule targets another cluster: refused", uuid.New(), errCodeNoCompatibleNode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			source.Update(source.Flag("extra-compatible-node-models").
				Variations(ldvalue.Null(), ldvalue.FromJSONMarshal(map[string][]string{"106": {"173"}})).
				FallthroughVariationIndex(0).
				VariationIndexForKey(featureflags.ClusterKind, tt.targeted.String(), 1))

			apiErr := createOnNodeModel(t, source, testTeam(), machineinfo.GraniteRapidsModel, false)

			if tt.wantErrCode == "" {
				assert.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantErrCode, apiErr.ErrorCode)
		})
	}
}

// The fs-only pin narrows rather than yields to the extra pairs.
func TestCreateSandbox_FsOnlyPinNarrowsExtraCompatibleNodeModels(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	source.Update(source.Flag("fs-only-resume-cpu-model").ValueForAll(ldvalue.String(machineinfo.IceLakeModel)))
	source.Update(source.Flag("extra-compatible-node-models").ValueForAll(ldvalue.FromJSONMarshal(map[string][]string{"106": {"173"}})))

	apiErr := createOnNodeModel(t, source, testTeam(), machineinfo.GraniteRapidsModel, true)

	require.NotNil(t, apiErr)
	assert.Equal(t, errCodeNoCompatibleNode, apiErr.ErrorCode)
}

// A malformed flag never blocks a start: placement continues on the hardcoded
// rule, which admits 207 for a 106 build, and the extras stay empty.
func TestCreateSandbox_MalformedExtraCompatibleNodeModelsKeepsHardcodedRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nodeModel   string
		wantErrCode string
	}{
		{"hardcoded pair: accepted", machineinfo.EmeraldRapidsModel, ""},
		{"extra-only pair: refused", machineinfo.GraniteRapidsModel, errCodeNoCompatibleNode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			source.Update(source.Flag("extra-compatible-node-models").ValueForAll(ldvalue.FromJSONMarshal(map[string]string{"106": "173"})))

			apiErr := createOnNodeModel(t, source, testTeam(), tt.nodeModel, false)

			if tt.wantErrCode == "" {
				assert.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantErrCode, apiErr.ErrorCode)
		})
	}
}

func TestCreateSandbox_CreatedCounterCarriesTeam(t *testing.T) {
	t.Parallel()

	o := newCreateSandboxTestOrchestrator(t)
	reader := sdkmetric.NewManualReader()
	counter, err := telemetry.GetCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator"), telemetry.ApiOrchestratorCreatedSandboxes)
	require.NoError(t, err)
	o.createdSandboxesCounter = counter

	team := testTeam()
	now := time.Now()
	build := testBuild()
	fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
		return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
	}

	_, apiErr := o.CreateSandbox(t.Context(), "sbx-team-"+uuid.New().String()[:8], uuid.New().String(), team,
		fetcher, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{})
	require.Nil(t, apiErr)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
	sum, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	teamID, ok := sum.DataPoints[0].Attributes.Value("team.id")
	require.True(t, ok)
	assert.Equal(t, team.ID.String(), teamID.AsString())
}

// Under the hugepage limit, a placement on a node that reports no pool ceiling
// went unchecked; the counter is the only sign of it. Nothing else counts: the
// other memory checks bound every node.
func TestCreateSandbox_CountsPlacementWithoutHugepageCeiling(t *testing.T) {
	t.Parallel()

	const pageSize = 2 << 20
	for _, tc := range []struct {
		name               string
		limitPercent       int
		overcommitPercent  int
		firecrackerVersion string
		nodeOpts           []nodemanager.TestOptions
		want               map[string]int64
	}{
		{
			name:         "no ceiling reported",
			limitPercent: 80,
			want:         map[string]int64{placement.NoHugepageCapacity: 1},
		},
		{
			name:         "ceiling reported",
			limitPercent: 80,
			nodeOpts:     []nodemanager.TestOptions{nodemanager.WithHugePages(1000, 0, 0, pageSize), nodemanager.WithHugePagesCapacity(1000)},
			want:         map[string]int64{},
		},
		{
			name:         "limit off",
			limitPercent: 0,
			want:         map[string]int64{},
		},
		{
			name:               "limit, sandbox without hugepages",
			limitPercent:       80,
			firecrackerVersion: "v1.6.0_abc1234",
			nodeOpts:           []nodemanager.TestOptions{nodemanager.WithMemoryTotalBytes(4096 << 20)},
			want:               map[string]int64{},
		},
		{
			name:              "overcommit cap",
			overcommitPercent: 100,
			nodeOpts:          []nodemanager.TestOptions{nodemanager.WithHugePages(1000, 0, 0, pageSize), nodemanager.WithHugePagesBase(1000)},
			want:              map[string]int64{},
		},
		{
			name:               "overcommit cap, sandbox without hugepages",
			overcommitPercent:  100,
			firecrackerVersion: "v1.6.0_abc1234",
			nodeOpts:           []nodemanager.TestOptions{nodemanager.WithMemoryTotalBytes(4096 << 20)},
			want:               map[string]int64{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			source.Update(source.Flag(featureflags.PlacementHugepageLimitPercent.Key()).ValueForAll(ldvalue.Int(tc.limitPercent)))
			source.Update(source.Flag(featureflags.PlacementMemoryMaxOvercommit.Key()).ValueForAll(ldvalue.Int(tc.overcommitPercent)))
			o := newCreateSandboxTestOrchestratorWithFlags(t, source, tc.nodeOpts...)

			reader := sdkmetric.NewManualReader()
			counter, err := telemetry.GetCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator"), telemetry.ApiOrchestratorPlacementsWithoutHugepageCeiling)
			require.NoError(t, err)
			o.noCeilingPlacementsCounter = counter

			build := testBuild()
			if tc.firecrackerVersion != "" {
				build.FirecrackerVersion = tc.firecrackerVersion
			}
			fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
			}

			now := time.Now()
			_, apiErr := o.CreateSandbox(t.Context(), "sbx-"+uuid.New().String()[:8], uuid.New().String(), testTeam(),
				fetcher, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{})
			require.Nil(t, apiErr)

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &rm))
			assert.Equal(t, tc.want, placementsWithoutCeilingByReason(t, rm, "node-1"))
		})
	}
}

// The cap comes from the flag on each placement: past it, the only node is a
// capacity shortage; raised, the same node takes the sandbox. A sandbox
// without hugepages is measured against the node's total memory instead.
func TestCreateSandbox_MemoryOvercommitCapRefusesAFullNode(t *testing.T) {
	t.Parallel()

	const pageSize = 2 << 20
	for _, tc := range []struct {
		name               string
		overcommitPercent  int
		allocatedMiB       uint64
		firecrackerVersion string
		wantErrCode        string
	}{
		// A base pool of 1000 pages is 2000 MiB, the node's total memory 4096
		// MiB; every build asks for 512.
		{name: "hugepages, past the pool", overcommitPercent: 100, allocatedMiB: 1600, wantErrCode: errCodeCapacityUnavailable},
		{name: "hugepages, under a raised cap", overcommitPercent: 125, allocatedMiB: 1600},
		{name: "no hugepages, past the pool but under total memory", overcommitPercent: 100, allocatedMiB: 1600, firecrackerVersion: "v1.6.0_abc1234"},
		{name: "no hugepages, past total memory", overcommitPercent: 100, allocatedMiB: 3700, firecrackerVersion: "v1.6.0_abc1234", wantErrCode: errCodeCapacityUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			source.Update(source.Flag(featureflags.PlacementMemoryMaxOvercommit.Key()).ValueForAll(ldvalue.Int(tc.overcommitPercent)))
			o := newCreateSandboxTestOrchestratorWithFlags(t, source,
				nodemanager.WithAllocatedMemoryBytes(tc.allocatedMiB<<20),
				nodemanager.WithMemoryTotalBytes(4096<<20),
				nodemanager.WithHugePages(1000, 0, 0, pageSize),
				nodemanager.WithHugePagesBase(1000),
			)

			build := testBuild()
			if tc.firecrackerVersion != "" {
				build.FirecrackerVersion = tc.firecrackerVersion
			}
			fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
			}

			now := time.Now()
			_, apiErr := o.CreateSandbox(t.Context(), "sbx-"+uuid.New().String()[:8], uuid.New().String(), testTeam(),
				fetcher, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{})

			if tc.wantErrCode == "" {
				require.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
			assert.Equal(t, tc.wantErrCode, apiErr.ErrorCode)
		})
	}
}

// The limit measures a sandbox without hugepages against the node's used and
// total memory, whatever room its hugepage pool has.
func TestCreateSandbox_MemoryLimitRefusesABusyNodeForOrdinaryPages(t *testing.T) {
	t.Parallel()

	const pageSize = 2 << 20
	for _, tc := range []struct {
		name        string
		usedMiB     uint64
		wantErrCode string
	}{
		// The pool is 2000 of the node's 4096 MiB, so 0.95 of the other 2096
		// is 1991; the build asks for 512.
		{name: "room in memory", usedMiB: 3300},
		{name: "memory nearly full", usedMiB: 3500, wantErrCode: errCodeCapacityUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			source.Update(source.Flag(featureflags.PlacementHugepageLimitPercent.Key()).ValueForAll(ldvalue.Int(95)))
			o := newCreateSandboxTestOrchestratorWithFlags(t, source,
				nodemanager.WithMemoryTotalBytes(4096<<20),
				nodemanager.WithMemoryUsedBytes(tc.usedMiB<<20),
				nodemanager.WithHugePages(1000, 0, 0, pageSize),
				nodemanager.WithHugePagesCapacity(1250),
			)

			build := testBuild()
			build.FirecrackerVersion = "v1.6.0_abc1234"
			fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
			}

			now := time.Now()
			_, apiErr := o.CreateSandbox(t.Context(), "sbx-"+uuid.New().String()[:8], uuid.New().String(), testTeam(),
				fetcher, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{})

			if tc.wantErrCode == "" {
				require.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
			assert.Equal(t, tc.wantErrCode, apiErr.ErrorCode)
		})
	}
}

// TestCreateSandbox_ResizableBuildNeedsNewOrchestrator: a build with
// max_vcpu sends max_vcpus and is placed only on a node at the
// resizable floor, including equal counts; a fixed-size build places on any node.
func TestCreateSandbox_ResizableBuildNeedsNewOrchestrator(t *testing.T) {
	t.Parallel()

	floor := apiclusters.ResizableTemplateBuildsMinVersion
	atFloor := floor.String() + "-abc1234-ee"
	beforeFloor := semver.New(floor.Major(), floor.Minor(), floor.Patch()-1, "", "").String() + "-abc1234-ee"

	tests := []struct {
		name        string
		maxVcpu     *int64
		nodeVersion string
		wantErrCode string
	}{
		{"fixed-size build, old node", nil, beforeFloor, ""},
		{"equal counts, old node", new(int64(2)), beforeFloor, errCodeFeatureUnsupported},
		{"resizable build, old node", new(int64(8)), beforeFloor, errCodeFeatureUnsupported},
		{"equal counts, node at the floor", new(int64(2)), atFloor, ""},
		{"resizable build, node at the floor", new(int64(8)), atFloor, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rpc := &cpuRecordingSandboxClient{}
			o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(tt.nodeVersion),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(rpc) })
			reader := sdkmetric.NewManualReader()
			counter, err := telemetry.GetCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator"), telemetry.ApiOrchestratorSandboxFeatureUnsupported)
			require.NoError(t, err)
			o.featureUnsupportedCounter = counter

			build := testBuild()
			build.MaxVcpu = tt.maxVcpu
			fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
			}
			now := time.Now()

			_, apiErr := o.CreateSandbox(t.Context(), "sbx-resize-"+uuid.New().String()[:8], uuid.New().String(),
				testTeam(), fetcher, now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &rm))

			if tt.wantErrCode == "" {
				require.Nil(t, apiErr)
				assert.Equal(t, int32(1), rpc.calls.Load())
				request := rpc.request.Load()
				require.NotNil(t, request)
				assert.Equal(t, build.Vcpu, request.GetSandbox().GetVcpu())
				var wantMax int64
				if tt.maxVcpu != nil {
					wantMax = *tt.maxVcpu
				}
				assert.Equal(t, wantMax, request.GetSandbox().GetMaxVcpus())
				assert.Empty(t, rm.ScopeMetrics, "counter must not fire on a placed sandbox")

				return
			}

			require.NotNil(t, apiErr)
			assert.Zero(t, rpc.calls.Load(), "an incompatible node must never receive Create")
			assert.Equal(t, tt.wantErrCode, apiErr.ErrorCode)
			assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)

			require.Len(t, rm.ScopeMetrics, 1)
			require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
			sum, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			feature, ok := sum.DataPoints[0].Attributes.Value("feature")
			require.True(t, ok)
			assert.Equal(t, "resizable-vcpu", feature.AsString())
		})
	}
}

type cpuRecordingSandboxClient struct {
	orchestrator.SandboxServiceClient

	request atomic.Pointer[orchestrator.SandboxCreateRequest]
	calls   atomic.Int32
}

func (c *cpuRecordingSandboxClient) Create(_ context.Context, req *orchestrator.SandboxCreateRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	c.calls.Add(1)
	c.request.Store(proto.Clone(req).(*orchestrator.SandboxCreateRequest))

	return &orchestrator.SandboxCreateResponse{}, nil
}

// placementsWithoutCeilingByReason reads the counter's points, all of which
// must name nodeID.
func placementsWithoutCeilingByReason(t *testing.T, rm metricdata.ResourceMetrics, nodeID string) map[string]int64 {
	t.Helper()

	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != string(telemetry.ApiOrchestratorPlacementsWithoutHugepageCeiling) {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is not an int64 sum", m.Name)

			for _, point := range sum.DataPoints {
				node, found := point.Attributes.Value("node.id")
				require.True(t, found, "datapoint carries no node.id")
				require.Equal(t, nodeID, node.AsString())
				reason, found := point.Attributes.Value("reason")
				require.True(t, found, "datapoint carries no reason")

				got[reason.AsString()] = point.Value
			}
		}
	}

	return got
}

type placementSandboxClient struct {
	orchestrator.SandboxServiceClient

	placement *orchestrator.SandboxNetworkPlacement
	creates   atomic.Int32
}

func (c *placementSandboxClient) Create(context.Context, *orchestrator.SandboxCreateRequest, ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	c.creates.Add(1)

	return &orchestrator.SandboxCreateResponse{NetworkPlacement: c.placement}, nil
}

func (c *placementSandboxClient) Update(context.Context, *orchestrator.SandboxUpdateRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func requirePlacement(t *testing.T, want, got *orchestrator.SandboxNetworkPlacement) {
	t.Helper()

	require.Truef(t, proto.Equal(want, got), "network placement: want %v, got %v", want, got)
}

func plainSandboxData(_ context.Context) (SandboxMetadata, *api.APIError) {
	return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: testBuild()}, nil
}

func storedPlacement(t *testing.T, o *Orchestrator, teamID uuid.UUID, sandboxID string) *orchestrator.SandboxNetworkPlacement {
	t.Helper()

	stored, err := o.sandboxStore.Get(t.Context(), teamID, sandboxID)
	require.NoError(t, err)

	return stored.NetworkPlacement
}

// pendingProbeHook closes probed when its client checks whether a reservation
// is still pending, which a joining caller does only after it subscribed to
// the result and found none.
type pendingProbeHook struct {
	probed chan struct{}
	once   sync.Once
}

func (h *pendingProbeHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *pendingProbeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *pendingProbeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "zscore" && strings.HasSuffix(fmt.Sprint(cmd.Args()[1]), ":reservations:pending") {
			h.once.Do(func() { close(h.probed) })
		}

		return err
	}
}

func TestCreateSandbox_RetainsWinningPlacement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		placement *orchestrator.SandboxNetworkPlacement
	}{
		{name: "legacy"},
		{name: "egress-router", placement: &orchestrator.SandboxNetworkPlacement{
			NetworkMode:    "egress-router",
			RouterId:       "router-a",
			IngressAddress: "10.0.0.11",
			IngressPort:    5007,
		}},
		{name: "full-router", placement: &orchestrator.SandboxNetworkPlacement{
			NetworkMode:    "full-router",
			RouterId:       "router-b",
			IngressAddress: "10.8.0.2",
			IngressPort:    5010,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			executionID := uuid.NewString()
			want := tt.placement
			if want != nil {
				want.ExecutionId = executionID
			}
			client := redis_utils.SetupContainerInstance(t)
			rpc := &placementSandboxClient{placement: want}
			o := newCreateSandboxTestOrchestratorOnRedis(t, client, ldtestdata.DataSource(),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(rpc) })
			origin := nodemanager.NewTestNode("node-origin", api.NodeStatusReady, 0, 8,
				nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed")))
			origin.ClusterID = uuid.Nil
			o.registerNode(origin)

			joinerClient := redis.NewClient(&redis.Options{Addr: client.(*redis.Client).Options().Addr})
			t.Cleanup(func() { _ = joinerClient.Close() })
			probe := &pendingProbeHook{probed: make(chan struct{})}
			joinerClient.AddHook(probe)
			// The joining API's own node would answer with another placement if
			// it were ever asked to start the sandbox.
			joinerRPC := &placementSandboxClient{placement: &orchestrator.SandboxNetworkPlacement{
				ExecutionId: uuid.NewString(), NetworkMode: "full-router", RouterId: "router-loser", IngressAddress: "10.9.0.9", IngressPort: 5999,
			}}
			joinerAPI := newCreateSandboxTestOrchestratorOnRedis(t, joinerClient, ldtestdata.DataSource(),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(joinerRPC) })
			joined := make(chan sandbox.Sandbox, 1)
			team := testTeam()
			sandboxID := "sbx-placement"
			now := time.Now()

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			sbx, apiErr := o.CreateSandbox(ctx, sandboxID, executionID, team,
				func(ctx context.Context) (SandboxMetadata, *api.APIError) {
					// The winner has reserved the start. Hold it until the other API is waiting.
					go func() {
						result, joinErr := joinerAPI.CreateSandbox(ctx, sandboxID, uuid.NewString(), team, plainSandboxData,
							now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
						assert.Nil(t, joinErr)
						joined <- result
					}()
					select {
					case <-probe.probed:
					case <-ctx.Done():
						t.Fatal("joiner did not wait on the pending start")
					}

					data, err := plainSandboxData(ctx)
					data.NodeID = &origin.ID // Fail on the snapshot's worker before retrying node-1.

					return data, err
				}, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})
			require.Nil(t, apiErr)
			assert.Equal(t, "node-1", sbx.NodeID)
			requirePlacement(t, want, sbx.NetworkPlacement)
			joiner := <-joined
			assert.Equal(t, executionID, joiner.ExecutionID)
			assert.Equal(t, sbx.NodeID, joiner.NodeID)
			requirePlacement(t, want, joiner.NetworkPlacement)
			requirePlacement(t, want, storedPlacement(t, o, team.ID, sandboxID))
			assert.EqualValues(t, 1, rpc.creates.Load())
			assert.Zero(t, joinerRPC.creates.Load())

			wantResp := &proxygrpc.SandboxResumeResponse{OrchestratorIp: "127.0.0.1", NetworkPlacement: want}
			if want.GetNetworkMode() == "full-router" {
				wantResp.OrchestratorIp = ""
			}
			for _, started := range []struct {
				api *Orchestrator
				sbx sandbox.Sandbox
			}{{o, sbx}, {joinerAPI, joiner}} {
				resp, err := started.api.StartedSandboxResumeResponse(started.sbx)
				require.NoError(t, err)
				require.Truef(t, proto.Equal(wantResp, resp), "want %v, got %v", wantResp, resp)
			}

			_, apiErr = o.KeepAliveFor(ctx, team.ID, sandboxID, 2*time.Hour, false)
			require.Nil(t, apiErr)
			requirePlacement(t, want, storedPlacement(t, o, team.ID, sandboxID))
			require.Nil(t, o.UpdateSandboxNetworkConfig(ctx, team.ID, sandboxID, []string{"1.1.1.1"}, nil, nil, nil, nil, nil))
			requirePlacement(t, want, storedPlacement(t, o, team.ID, sandboxID))

			transition, _, finish, err := o.sandboxStore.StartRemoving(ctx, team.ID, sandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
			require.NoError(t, err)
			requirePlacement(t, want, transition.Sandbox.NetworkPlacement)
			restored, err := o.sandboxStore.RestoreRunning(ctx, transition, sandbox.RestoreOpts{})
			require.NoError(t, err)
			finish(ctx, sandbox.ErrTransitionRestored)
			requirePlacement(t, want, restored.NetworkPlacement)
			requirePlacement(t, want, storedPlacement(t, o, team.ID, sandboxID))

			o.sandboxStore.Remove(ctx, team.ID, sandboxID, sbx.ExecutionID)
			rpc.placement = nil
			sbx, apiErr = o.CreateSandbox(ctx, sandboxID, uuid.NewString(), team, plainSandboxData,
				now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})
			require.Nil(t, apiErr)
			requirePlacement(t, nil, sbx.NetworkPlacement)
			requirePlacement(t, nil, storedPlacement(t, o, team.ID, sandboxID))
		})
	}
}
