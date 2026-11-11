package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

type clusterMetadataQueries struct {
	queries.DBTX

	reads []string
}

func (q *clusterMetadataQueries) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	q.reads = append(q.reads, sql)

	return q.DBTX.QueryRow(ctx, sql, args...)
}

func (q *clusterMetadataQueries) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.reads = append(q.reads, sql)

	return q.DBTX.Query(ctx, sql, args...)
}

func TestAdminClusterMetadataDoesNotReadTeamsOrTemplates(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	domain := "sandbox.example.test"
	cluster := insertNamedCluster(t, db, "metadata", "metadata.example.test", &domain)
	team := assignTeam(t, db, cluster)
	template := testutils.CreateTestTemplate(t, db, team)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET cluster_id = $1 WHERE id = $2`, cluster, template))
	recorded := &clusterMetadataQueries{DBTX: db.SqlcClient.Pool()}
	store := &APIStore{sqlcDB: &sqlcdb.Client{Queries: queries.New(recorded)}}
	response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+cluster.String()+"/metadata", "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var detail api.ClusterDetail
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
	require.Equal(t, api.ClusterDetail{Id: cluster, Name: "metadata", Endpoint: "metadata.example.test", EndpointTls: true, SandboxProxyDomain: &domain}, detail)
	require.NotContains(t, response.Body.String(), "token")
	require.NotContains(t, response.Body.String(), "teamCount")
	require.NotContains(t, response.Body.String(), "templateCount")
	require.Len(t, recorded.reads, 1)
	query := strings.ToLower(recorded.reads[0])
	require.NotContains(t, query, "teams")
	require.NotContains(t, query, "envs")
	require.NotContains(t, query, "count(")

	recorded.reads = nil
	missing := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+uuid.NewString()+"/metadata", "")
	require.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
	require.Len(t, recorded.reads, 1)
}
