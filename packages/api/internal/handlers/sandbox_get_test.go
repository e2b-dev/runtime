package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestGetSandboxesSandboxID_PausedMetadata(t *testing.T) {
	t.Parallel()

	redisClient := redis_utils.SetupContainerInstance(t)
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.Background())) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	snapshots := snapshotcache.NewSnapshotCache(nil, redisClient)
	orch, err := orchestrator.New(ctx, cfg.Config{}, &telemetry.Client{MeterProvider: noop.NewMeterProvider()},
		nil, true, nil, redisClient, nil, clusters.NewTestPool(), flags, nil, snapshots, nil,
		insecure.NewCredentials(), insecure.NewCredentials())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, orch.Close(context.Background()))
		require.NoError(t, snapshots.Close(context.Background()))
	})
	store := &APIStore{orchestrator: orch, snapshotCache: snapshots}
	teamID := uuid.New()

	for _, tt := range []struct {
		name       string
		config     *dbtypes.PausedSandboxConfig
		aliases    []string
		wantMounts string
	}{
		{
			name: "mounts without alias",
			config: &dbtypes.PausedSandboxConfig{VolumeMounts: []*dbtypes.SandboxVolumeMountConfig{
				{ID: "vol-1", Type: "persistent", Name: "data", Path: "/mnt/data"},
				{ID: "vol-2", Type: "persistent", Name: "models", Path: "/opt/models"},
			}},
			wantMounts: `[{"name":"data","path":"/mnt/data"},{"name":"models","path":"/opt/models"}]`,
		},
		{name: "empty mounts with aliases", config: &dbtypes.PausedSandboxConfig{}, aliases: []string{"first", "second"}, wantMounts: `[]`},
		{name: "legacy snapshot without config"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const sandboxID = "pausedsandbox"
			diskSize := int64(1024)
			envdVersion := "v1.0.0"
			data, err := json.Marshal(snapshotcache.SnapshotInfo{
				Aliases: tt.aliases,
				Snapshot: queries.Snapshot{
					SandboxID: sandboxID, TeamID: teamID, BaseEnvID: "template", Config: tt.config,
				},
				EnvBuild: queries.EnvBuild{TotalDiskSizeMb: &diskSize, EnvdVersion: &envdVersion},
			})
			require.NoError(t, err)
			require.NoError(t, redisClient.Set(t.Context(), "snapshot:last:"+sandboxID, data, 5*time.Minute).Err())

			recorder := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(recorder)
			ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes/"+sandboxID, nil)
			auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{Team: &authqueries.Team{ID: teamID}})
			store.GetSandboxesSandboxID(ginCtx, sandboxID)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			var body map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			assert.JSONEq(t, `"paused"`, string(body["state"]))
			if tt.wantMounts == "" {
				assert.NotContains(t, body, "volumeMounts")
			} else {
				assert.JSONEq(t, tt.wantMounts, string(body["volumeMounts"]))
			}
			if len(tt.aliases) == 0 {
				assert.NotContains(t, body, "alias")
			} else {
				assert.JSONEq(t, `"first"`, string(body["alias"]))
			}
		})
	}
}

func TestSandboxPauseModeToAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config *dbtypes.PausedSandboxConfig
		want   api.SandboxPauseMode
	}{
		{"row without a config is a full snapshot", nil, api.Full},
		{"full snapshot", &dbtypes.PausedSandboxConfig{}, api.Full},
		{"filesystem-only snapshot", &dbtypes.PausedSandboxConfig{FilesystemOnly: true}, api.Filesystem},
		{
			"an auto-pause policy does not change the latest pause",
			&dbtypes.PausedSandboxConfig{AutoPauseFilesystemOnly: true},
			api.Full,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := sandboxPauseModeToAPI(tt.config)

			require.NotNil(t, result)
			assert.Equal(t, tt.want, *result)
		})
	}
}

// The read-back is the one place stored proxy configuration travels back to
// the caller, so what it omits matters as much as what it returns.
func TestDBNetworkConfigToAPI_EgressProxy(t *testing.T) {
	t.Parallel()

	t.Run("credentials and CA bundle are withheld", func(t *testing.T) {
		t.Parallel()

		result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
			Egress: &dbtypes.SandboxNetworkEgressConfig{
				EgressProxyAddress:  "proxy.example.com:1080",
				EgressProxyUsername: "alice",
				EgressProxyPassword: "s3cret",
				EgressProxyTLS: &dbtypes.SandboxEgressProxyTLSConfig{
					Enabled:    true,
					ServerName: "cert-name.test",
					CACert:     "-----BEGIN CERTIFICATE-----\nstub\n-----END CERTIFICATE-----",
				},
			},
		})

		require.NotNil(t, result.EgressProxy)
		assert.Equal(t, "proxy.example.com:1080", result.EgressProxy.Address)
		require.NotNil(t, result.EgressProxy.Username)
		assert.Equal(t, "alice", *result.EgressProxy.Username)
		assert.Nil(t, result.EgressProxy.Password, "the proxy password must never be read back")

		require.NotNil(t, result.EgressProxy.Tls)
		assert.True(t, result.EgressProxy.Tls.Enabled)
		require.NotNil(t, result.EgressProxy.Tls.ServerName)
		assert.Equal(t, "cert-name.test", *result.EgressProxy.Tls.ServerName)
		assert.Nil(t, result.EgressProxy.Tls.CaCert,
			"the bundle is public material but multi-kilobyte, and the caller supplied it")
	})

	t.Run("a plaintext proxy reports no TLS", func(t *testing.T) {
		t.Parallel()

		result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
			Egress: &dbtypes.SandboxNetworkEgressConfig{
				EgressProxyAddress: "proxy.example.com:1080",
			},
		})

		require.NotNil(t, result.EgressProxy)
		assert.Nil(t, result.EgressProxy.Tls)
	})

	t.Run("no proxy reports nothing", func(t *testing.T) {
		t.Parallel()

		result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
			Egress: &dbtypes.SandboxNetworkEgressConfig{},
		})

		assert.Nil(t, result.EgressProxy)
	})
}

// GET returns the stored list, so callers can see which servers are allowed.
// An empty list is shown as [], and an unset one is left out.
func TestDBNetworkConfigToAPI_DnsServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stored   *[]string
		wantJSON string
	}{
		{"listed servers are echoed", &[]string{"1.1.1.1", "9.9.9.9"}, `"dnsServers":["1.1.1.1","9.9.9.9"]`},
		{"empty list is echoed", &[]string{}, `"dnsServers":[]`},
		{"unset list is omitted", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
				Egress: &dbtypes.SandboxNetworkEgressConfig{
					AllowedAddresses: []string{"example.com"},
					DnsServers:       tt.stored,
				},
			})
			require.NotNil(t, result)
			assert.Equal(t, tt.stored, result.DnsServers)

			data, err := json.Marshal(result)
			require.NoError(t, err)
			if tt.wantJSON == "" {
				assert.NotContains(t, string(data), "dnsServers")
			} else {
				assert.Contains(t, string(data), tt.wantJSON)
			}
		})
	}
}
