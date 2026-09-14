package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
)

func TestUserAgentToTemplateVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		userAgent string
		want      string
	}{
		{
			name:      "current JS SDK",
			userAgent: "e2b-js-sdk/2.31.0",
			want:      templates.TemplateV2LatestVersion,
		},
		{
			name:      "current JS SDK with CLI integration",
			userAgent: "e2b-js-sdk/v2.31.0 e2b-cli/2.13.0",
			want:      templates.TemplateV2LatestVersion,
		},
		{
			name:      "old JS SDK with CLI integration",
			userAgent: "e2b-js-sdk/2.2.0 e2b-cli/2.13.0",
			want:      templates.TemplateV2BetaVersion,
		},
		{
			name:      "current Python SDK with integration",
			userAgent: "e2b-python-sdk/2.31.0 custom-client/1.0.0",
			want:      templates.TemplateV2LatestVersion,
		},
		{
			name:      "unrecognized user agent",
			userAgent: "custom-client/1.0.0",
			want:      templates.TemplateV2LatestVersion,
		},
		{
			name:      "empty user agent",
			userAgent: "",
			want:      templates.TemplateV2LatestVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := userAgentToTemplateVersion(t.Context(), logger.L(), tt.userAgent)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestPostV3TemplatesMinimumFreeDiskPersistence(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)
	store := &APIStore{
		sqlcDB:        db.SqlcClient,
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
		featureFlags:  flags,
		posthog:       posthog,
	}
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		require.NoError(t, store.templateCache.Close(ctx))
		require.NoError(t, flags.Close(ctx))
	})

	for _, tt := range []struct {
		name      string
		fields    string
		status    int
		persisted int64
	}{
		{"omitted", "", http.StatusAccepted, 10240},
		{"zero", `,"minFreeDiskMb":0`, http.StatusAccepted, 0},
		{"positive", `,"minFreeDiskMb":1024`, http.StatusAccepted, 1024},
		{"maximum", `,"minFreeDiskMb":25600`, http.StatusAccepted, 25600},
		{"retired-zero-uses-default", `,"freeDiskSpaceMB":0`, http.StatusAccepted, 10240},
		{"retired-positive-uses-default", `,"freeDiskSpaceMB":1024`, http.StatusAccepted, 10240},
		{"new-zero-wins", `,"minFreeDiskMb":0,"freeDiskSpaceMB":1024`, http.StatusAccepted, 0},
		{"new-positive-wins", `,"minFreeDiskMb":1024,"freeDiskSpaceMB":0`, http.StatusAccepted, 1024},
		{"negative", `,"minFreeDiskMb":-1`, http.StatusBadRequest, -1},
		{"over-limit", `,"minFreeDiskMb":25601`, http.StatusBadRequest, -1},
		{"retired-over-limit-uses-default", `,"freeDiskSpaceMB":25601`, http.StatusAccepted, 10240},
		{"retired-value-cannot-override-invalid-new", `,"minFreeDiskMb":25601,"freeDiskSpaceMB":0`, http.StatusBadRequest, -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			teamID := testutils.CreateTestTeam(t, db)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			body := fmt.Sprintf(`{"name":"minimum-%s"%s}`, tt.name, tt.fields)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v3/templates", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			auth.SetTeamInfoForTest(t, c, &types.Team{
				Team: &authqueries.Team{ID: teamID, Slug: testutils.GetTeamSlug(t, t.Context(), db, teamID)},
				Limits: &types.TeamLimits{
					BuildConcurrency:      32,
					MaxVcpu:               8,
					MaxRamMb:              8192,
					DefaultFreeDiskSizeMb: 10240,
					MaxFreeDiskSizeMb:     25600,
				},
			})

			//nolint:contextcheck // PostV3Templates reads the context from c.Request.
			store.PostV3Templates(c)
			require.Equal(t, tt.status, recorder.Code, recorder.Body.String())

			var count, persisted int64
			err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `
				SELECT count(*), COALESCE(max(eb.free_disk_size_mb), -1)
				FROM public.envs e
				JOIN public.env_build_assignments eba ON eba.env_id = e.id
				JOIN public.env_builds eb ON eb.id = eba.build_id
				WHERE e.team_id = $1`, func(rows pgx.Rows) error {
				require.True(t, rows.Next())

				return rows.Scan(&count, &persisted)
			}, teamID)
			require.NoError(t, err)
			require.Equal(t, tt.persisted, persisted)
			if tt.status == http.StatusAccepted {
				require.EqualValues(t, 1, count)
			} else {
				require.Zero(t, count, "rejected requests must not register a build")
			}
		})
	}
}

func TestPostV2TemplatesTemplateIDBuildsBuildIDRequiresSource(t *testing.T) {
	t.Parallel()

	// The V1 flow sent an empty fromImage; it must fail as client input before the build row is touched.
	for _, tt := range []struct{ name, body, message string }{
		{"no source", `{"steps":[]}`, "must specify either fromImage or fromTemplate"},
		{"empty fromImage", `{"fromImage":"","steps":[]}`, "must specify either fromImage or fromTemplate"},
		{"empty fromTemplate", `{"fromTemplate":"","steps":[]}`, "must specify either fromImage or fromTemplate"},
		{"both sources", `{"fromImage":"alpine","fromTemplate":"base","steps":[]}`, "cannot specify both fromImage and fromTemplate"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/templates/tpl/builds/00000000-0000-0000-0000-000000000000", strings.NewReader(tt.body))
			c.Request.Header.Set("Content-Type", "application/json")

			(&APIStore{}).PostV2TemplatesTemplateIDBuildsBuildID(c, "tpl", "00000000-0000-0000-0000-000000000000")

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), tt.message)
		})
	}
}
