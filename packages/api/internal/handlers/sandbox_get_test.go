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
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const egressCredential = "Bearer third-party-credential"

func injectsACredential() *dbtypes.PausedSandboxConfig {
	return &dbtypes.PausedSandboxConfig{Network: &dbtypes.SandboxNetworkConfig{
		Egress: &dbtypes.SandboxNetworkEgressConfig{Rules: map[string][]dbtypes.SandboxNetworkRule{
			"api.example.com": {{Transform: &dbtypes.SandboxNetworkTransform{
				Headers: map[string]string{"Authorization": egressCredential},
			}}},
		}},
	}}
}

// newSandboxGetTestStore returns a store whose orchestrator and snapshot cache
// read only Redis, so a test seeds both through the returned client.
func newSandboxGetTestStore(t *testing.T) (*APIStore, redis.UniversalClient) {
	t.Helper()

	redisClient := redis_utils.SetupContainerInstance(t)
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	snapshots := snapshotcache.NewSnapshotCache(nil, redisClient)
	orch, err := orchestrator.New(ctx, cfg.Config{}, &telemetry.Client{MeterProvider: noop.NewMeterProvider()},
		nil, true, nil, redisClient, nil, clusters.NewTestPool(), flags, nil, snapshots, nil,
		insecure.NewCredentials(), insecure.NewCredentials(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, orch.Close(context.WithoutCancel(t.Context())))
		require.NoError(t, snapshots.Close(context.WithoutCancel(t.Context())))
	})

	tokens, err := sandbox.NewAccessTokenGenerator("seed")
	require.NoError(t, err)

	return &APIStore{orchestrator: orch, snapshotCache: snapshots, accessTokenGenerator: tokens}, redisClient
}

// seedPausedSnapshot writes the snapshot cache entry a paused get reads. A miss
// would fall through to the nil DB and panic.
func seedPausedSnapshot(t *testing.T, redisClient redis.UniversalClient, aliases []string, snapshot queries.Snapshot) {
	t.Helper()

	diskSize := int64(1024)
	envdVersion := "v1.0.0"
	data, err := json.Marshal(snapshotcache.SnapshotInfo{ //nolint:musttag // Snapshot rows use their Go field names in the shared cache format.
		Aliases:  aliases,
		Snapshot: snapshot,
		EnvBuild: queries.EnvBuild{TotalDiskSizeMb: &diskSize, EnvdVersion: &envdVersion},
		Format:   snapshotcache.SnapshotInfoFormat,
	})
	require.NoError(t, err)
	require.NoError(t, redisClient.Set(t.Context(), "snapshot:last:v2:"+snapshot.SandboxID, data, 5*time.Minute).Err())
}

// getSandboxBody returns the get response's fields and its raw body.
func getSandboxBody(t *testing.T, store *APIStore, teamID uuid.UUID, role, sandboxID string) (map[string]json.RawMessage, string) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes/"+sandboxID, nil)
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{Team: &dbtypes.Team{ID: teamID}, Role: role})
	store.GetSandboxesSandboxID(ginCtx, sandboxID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	return body, recorder.Body.String()
}

func TestGetSandboxesSandboxID_PausedMetadata(t *testing.T) {
	t.Parallel()

	store, redisClient := newSandboxGetTestStore(t)
	teamID := uuid.New()

	for _, tt := range []struct {
		name        string
		sandboxID   string
		config      *dbtypes.PausedSandboxConfig
		aliases     []string
		wantMounts  string
		role        string
		secure      bool
		wantSecrets bool
	}{
		{
			name:      "mounts without alias",
			sandboxID: "pausedmounts",
			config: &dbtypes.PausedSandboxConfig{VolumeMounts: []*dbtypes.SandboxVolumeMountConfig{
				{ID: "vol-1", Type: "persistent", Name: "data", Path: "/mnt/data"},
				{ID: "vol-2", Type: "persistent", Name: "models", Path: "/opt/models"},
			}},
			wantMounts: `[{"name":"data","path":"/mnt/data"},{"name":"models","path":"/opt/models"}]`,
		},
		{name: "empty mounts with aliases", sandboxID: "pausedaliases", config: &dbtypes.PausedSandboxConfig{}, aliases: []string{"first", "second"}, wantMounts: `[]`},
		{name: "legacy snapshot without config", sandboxID: "pausedlegacy"},
		{name: "secure sandbox read by a member", sandboxID: "pausedmember", config: injectsACredential(), wantMounts: `[]`, role: authtypes.TeamRoleMember, secure: true, wantSecrets: true},
		{name: "secure sandbox read by a viewer", sandboxID: "pausedviewer", config: injectsACredential(), wantMounts: `[]`, role: authtypes.TeamRoleViewer, secure: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sandboxID := tt.sandboxID
			seedPausedSnapshot(t, redisClient, tt.aliases, queries.Snapshot{
				SandboxID: sandboxID, TeamID: teamID, BaseEnvID: "template", Config: tt.config, EnvSecure: tt.secure,
			})

			body, raw := getSandboxBody(t, store, teamID, tt.role, sandboxID)
			assert.JSONEq(t, `"paused"`, string(body["state"]))
			if tt.wantSecrets {
				assert.Contains(t, body, "envdAccessToken")
				assert.Contains(t, raw, egressCredential)
			} else {
				assert.NotContains(t, body, "envdAccessToken")
				assert.NotContains(t, raw, egressCredential)
			}
			if tt.wantMounts == "" {
				assert.NotContains(t, body, "volumeMounts")
			} else {
				assert.JSONEq(t, tt.wantMounts, string(body["volumeMounts"]))
			}
			// No alias answers "", as a running sandbox does.
			wantAlias := `""`
			if len(tt.aliases) > 0 {
				wantAlias = `"first"`
			}
			assert.JSONEq(t, wantAlias, string(body["alias"]))
		})
	}
}

func TestGetSandboxesSandboxID_PausedTTL(t *testing.T) {
	t.Parallel()

	store, redisClient := newSandboxGetTestStore(t)
	storage, err := sandboxredis.NewStorage(redisClient, noop.NewMeterProvider())
	require.NoError(t, err)
	teamID := uuid.New()
	ttl := 2 * time.Hour
	expiresAt := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

	running := func(state sandbox.State, ttl *time.Duration) func(*testing.T, string) {
		return func(t *testing.T, sandboxID string) {
			t.Helper()
			now := time.Now()
			require.NoError(t, storage.Add(t.Context(), sandbox.Sandbox{
				SandboxID: sandboxID, ExecutionID: uuid.NewString(), TeamID: teamID, TemplateID: "template",
				StartTime: now, EndTime: now.Add(time.Hour), MaxInstanceLength: time.Hour,
				ClusterID: consts.LocalClusterID, State: state, PausedTTL: ttl,
			}))
		}
	}
	paused := func(config *dbtypes.PausedSandboxConfig, expiresAt *time.Time) func(*testing.T, string) {
		return func(t *testing.T, sandboxID string) {
			t.Helper()
			seedPausedSnapshot(t, redisClient, nil, queries.Snapshot{
				SandboxID: sandboxID, TeamID: teamID, BaseEnvID: "template", Config: config, ExpiresAt: expiresAt,
			})
		}
	}

	for _, tt := range []struct {
		name          string
		sandboxID     string
		seed          func(t *testing.T, sandboxID string)
		wantTTL       string
		wantExpiresAt string
	}{
		{
			name: "running with a TTL", sandboxID: "runningttl",
			seed:    running(sandbox.StateRunning, &ttl),
			wantTTL: `7200`, wantExpiresAt: `null`,
		},
		{
			name: "running without a TTL", sandboxID: "runningnottl",
			seed:    running(sandbox.StateRunning, nil),
			wantTTL: `null`, wantExpiresAt: `null`,
		},
		{
			name: "pausing has no expiry until the pause finishes", sandboxID: "pausingttl",
			seed:    running(sandbox.StatePausing, &ttl),
			wantTTL: `7200`, wantExpiresAt: `null`,
		},
		{
			name: "paused with a TTL", sandboxID: "pausedttl",
			seed:    paused(&dbtypes.PausedSandboxConfig{PausedTTL: new(int64(7200))}, &expiresAt),
			wantTTL: `7200`, wantExpiresAt: `"2026-11-12T10:00:00Z"`,
		},
		{
			name: "paused without a TTL", sandboxID: "pausednottl",
			seed:    paused(&dbtypes.PausedSandboxConfig{}, nil),
			wantTTL: `null`, wantExpiresAt: `null`,
		},
		{
			name: "legacy snapshot without config", sandboxID: "pausedlegacyttl",
			seed:    paused(nil, nil),
			wantTTL: `null`, wantExpiresAt: `null`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.seed(t, tt.sandboxID)

			body, _ := getSandboxBody(t, store, teamID, "", tt.sandboxID)
			require.Contains(t, body, "pausedRetentionSeconds")
			require.Contains(t, body, "expiresAt")
			assert.JSONEq(t, tt.wantTTL, string(body["pausedRetentionSeconds"]))
			assert.JSONEq(t, tt.wantExpiresAt, string(body["expiresAt"]))
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
