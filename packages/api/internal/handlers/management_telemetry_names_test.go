package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

func TestTelemetryNamesExcludeCredentialsAndDeletedRecords(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := insertAdminTestTeam(t, db, adminTestTeam{name: "Team name", createdAt: time.Now()})
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `INSERT INTO public.clusters(id,name,endpoint,token,telemetry_client_id,deletion_protection) VALUES ($1,'Cluster name','private.example:443','private-token','client_test',false)`, clusterID))
	store := &APIStore{sqlcReadDB: db.SqlcClient, config: cfg.Config{TelemetryReaderID: "spiffe://test.internal/ns/observability/sa/reader"}}
	read := func() api.TelemetryNames {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequestWithContext(mtls.ContextWithPeer(t.Context(), mtls.Peer{ID: store.config.TelemetryReaderID, TLS: true, ChainVerified: true}), http.MethodGet, "/unstable/management/telemetry/names", nil)
		store.ManagementTelemetryNames(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.NotContains(t, recorder.Body.String(), "private-token")
		assert.NotContains(t, recorder.Body.String(), "private.example")
		var names api.TelemetryNames
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &names))

		return names
	}
	require.NotContains(t, read().Teams, teamID.String())
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id=$1 WHERE id=$2`, clusterID, teamID))
	names := read()
	assert.Equal(t, "Team name", names.Teams[teamID.String()].Name)
	require.Len(t, names.Clusters, 1)
	assert.Equal(t, clusterID, names.Clusters[0].Id)
	assert.Equal(t, "Cluster name", names.Clusters[0].Name)
	require.NotNil(t, names.Clusters[0].ClientId)
	assert.Equal(t, "client_test", *names.Clusters[0].ClientId)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET name='Renamed' WHERE id=$1`, teamID))
	assert.Equal(t, "Renamed", read().Teams[teamID.String()].Name)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET deleted_at=now(),is_blocked=true WHERE id=$1`, teamID))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.clusters SET deletion_requested_at=now() WHERE id=$1`, clusterID))
	names = read()
	assert.NotContains(t, names.Teams, teamID.String())
	assert.Empty(t, names.Clusters)
}

func TestTelemetryNamesRejectsUnverifiedReaders(t *testing.T) {
	t.Parallel()
	const reader = "spiffe://test.internal/ns/observability/sa/reader"
	for _, tc := range []struct {
		name    string
		allowed string
		peer    mtls.Peer
	}{
		{name: "missing certificate", allowed: reader},
		{name: "unverified certificate", allowed: reader, peer: mtls.Peer{ID: reader, TLS: true}},
		{name: "different service", allowed: reader, peer: mtls.Peer{ID: "spiffe://test.internal/ns/edge/sa/proxy", TLS: true, ChainVerified: true}},
		{name: "disabled reader", peer: mtls.Peer{ID: reader, TLS: true, ChainVerified: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &APIStore{config: cfg.Config{TelemetryReaderID: tc.allowed}}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequestWithContext(mtls.ContextWithPeer(t.Context(), tc.peer), http.MethodGet, "/unstable/management/telemetry/names", nil)
			c.Request.Header.Set("Authorization", "Bearer telemetry-token")
			c.Request.Header.Set("X-Admin-Token", "admin-token")
			c.Request.Header.Set("X-Forwarded-Client-Cert", reader)
			store.ManagementTelemetryNames(c)
			assert.Equal(t, http.StatusUnauthorized, recorder.Code)
		})
	}
}
