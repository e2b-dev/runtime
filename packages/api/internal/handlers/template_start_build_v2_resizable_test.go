package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	templatemanager "github.com/e2b-dev/infra/packages/api/internal/template-manager"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// captureTemplateClient records the TemplateConfig the API sends to the builder
// and fails the build on the first status poll so the background sync ends.
type captureTemplateClient struct {
	templatemanagergrpc.TemplateServiceClient

	mu       sync.Mutex
	template *templatemanagergrpc.TemplateConfig
}

func (c *captureTemplateClient) TemplateCreate(_ context.Context, in *templatemanagergrpc.TemplateCreateRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.template = in.GetTemplate()

	return &emptypb.Empty{}, nil
}

func (c *captureTemplateClient) TemplateBuildStatus(_ context.Context, _ *templatemanagergrpc.TemplateStatusRequest, _ ...grpc.CallOption) (*templatemanagergrpc.TemplateBuildStatusResponse, error) {
	return &templatemanagergrpc.TemplateBuildStatusResponse{
		Status: templatemanagergrpc.TemplateBuildState_Failed,
		Reason: &templatemanagergrpc.TemplateBuildStatusReason{Message: "test builder"},
	}, nil
}

func (c *captureTemplateClient) created() *templatemanagergrpc.TemplateConfig {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.template
}

func TestPostV2TemplatesTemplateIDBuildsBuildIDResizable(t *testing.T) {
	t.Parallel()

	oldBuilder := "0.1.0"
	floorBuilder := clusters.ResizableTemplateBuildsMinVersion.String()
	for _, tt := range []struct {
		name     string
		register string
		builder  string
		status   int
		message  string
		maxVCpu  *int32
	}{
		{"resizable build needs a builder at the floor", `,"maxCpuCount":8`, oldBuilder, http.StatusServiceUnavailable, "No template builder supports resizable templates yet", nil},
		{"resizable build sends maxVCpuCount to the builder", `,"maxCpuCount":8`, floorBuilder, http.StatusAccepted, "", new(int32(8))},
		{"equal capacity rejects an old builder", `,"maxCpuCount":2`, oldBuilder, http.StatusServiceUnavailable, "No template builder supports resizable templates yet", nil},
		{"equal capacity is sent explicitly", `,"maxCpuCount":2`, floorBuilder, http.StatusAccepted, "", new(int32(2))},
		{"fixed-size build accepts any builder", "", oldBuilder, http.StatusAccepted, "", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &captureTemplateClient{}
			store, db := resizableBuildStore(t, clusters.TestBuilder{Version: tt.builder, Client: &clusters.GRPCClient{Template: client}})
			team := templateClusterTeam(t, db, nil)

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v3/templates", strings.NewReader(`{"name":"resizable","cpuCount":2`+tt.register+`}`))
			c.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, c, team)
			store.PostV3Templates(c, api.PostV3TemplatesParams{})
			registered, err := api.ParsePostV3TemplatesResponse(w.Result())
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, registered.StatusCode(), string(registered.Body))
			templateID, buildID := registered.JSON202.TemplateID, registered.JSON202.BuildID

			w = httptest.NewRecorder()
			c, _ = gin.CreateTestContext(w)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, fmt.Sprintf("/v2/templates/%s/builds/%s", templateID, buildID), strings.NewReader(`{"fromImage":"alpine","steps":[]}`))
			c.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, c, team)
			store.PostV2TemplatesTemplateIDBuildsBuildID(c, templateID, buildID)
			require.Equal(t, tt.status, c.Writer.Status(), w.Body.String())

			if tt.status != http.StatusAccepted {
				require.Contains(t, w.Body.String(), tt.message)
				require.Nil(t, client.created(), "a rejected build must not reach the builder")
				build, err := db.SqlcClient.GetTemplateBuildWithTemplate(t.Context(), queries.GetTemplateBuildWithTemplateParams{TemplateID: templateID, BuildID: uuid.MustParse(buildID)})
				require.NoError(t, err)
				require.Equal(t, dbtypes.BuildStatusGroupPending, build.EnvBuild.StatusGroup)

				return
			}

			created := client.created()
			require.NotNil(t, created)
			require.Equal(t, int32(2), created.GetVCpuCount())
			require.Equal(t, tt.maxVCpu, created.MaxVCpuCount)
		})
	}
}

// resizableBuildStore is templateClusterStore with resizable templates enabled
// and a single builder in the default cluster.
func resizableBuildStore(t *testing.T, builder clusters.TestBuilder) (*APIStore, *testutils.Database) {
	t.Helper()
	db := testutils.SetupDatabase(t)
	redis := redisutils.SetupInstance(t)
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ResizableTemplatesFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)
	store := &APIStore{
		sqlcDB:              db.SqlcClient,
		templateCache:       templatecache.NewTemplateCache(db.SqlcClient, redis),
		templateBuildsCache: templatecache.NewTemplateBuildCache(db.SqlcClient, redis),
		featureFlags:        flags,
		posthog:             posthog,
	}
	pool := clusters.NewTestPool(clusters.NewTestBuilderCluster(consts.LocalClusterID, builder))
	store.templateManager, err = templatemanager.New(db.SqlcClient, pool, store.templateBuildsCache, store.templateCache, flags)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		require.NoError(t, store.templateCache.Close(ctx))
		require.NoError(t, store.templateBuildsCache.Close(ctx))
		require.NoError(t, flags.Close(ctx))
	})

	return store, db
}
