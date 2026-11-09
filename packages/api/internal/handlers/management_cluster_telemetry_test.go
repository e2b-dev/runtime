package handlers

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestTelemetryRefreshPreservesClusterSettings(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID := uuid.New()
	registration := managementClusterRegistration()
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	readSettings := func() string {
		var settings string
		require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(), `SELECT (to_jsonb(c)-'telemetry_client_id')::text FROM public.clusters c WHERE id=$1`, clusterID).Scan(&settings))

		return settings
	}
	before := readSettings()
	refresh := func(id uuid.UUID, client string) int {
		body := mustJSON(t, api.ManagementUpdateClusterTelemetryJSONRequestBody{TelemetryClientId: client})

		return callManagement(t, store, http.MethodPut, "/unstable/management/clusters/"+id.String()+"/telemetry", body).Code
	}
	for range 2 {
		require.Equal(t, http.StatusNoContent, refresh(clusterID, "client_saved"))
		require.JSONEq(t, before, readSettings())
	}
	var client string
	require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(), `SELECT telemetry_client_id FROM public.clusters WHERE id=$1`, clusterID).Scan(&client))
	require.Equal(t, "client_saved", client)
	registration.TelemetryClientId = new("client_replacement")
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, clusterID, registration).Code)
	registeredSettings := readSettings()
	require.Equal(t, http.StatusConflict, refresh(clusterID, "client_saved"))
	require.JSONEq(t, registeredSettings, readSettings())
	require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(), `SELECT telemetry_client_id FROM public.clusters WHERE id=$1`, clusterID).Scan(&client))
	require.Equal(t, "client_replacement", client)
	other := uuid.New()
	registration.AuthOrgId = new("org_other")
	registration.TelemetryClientId = nil
	require.Equal(t, http.StatusNoContent, callManagementRegisterCluster(t, store, other, registration).Code)
	require.Equal(t, http.StatusConflict, refresh(other, client))
	require.Equal(t, http.StatusNotFound, refresh(uuid.New(), "client_absent"))
	require.Equal(t, http.StatusBadRequest, refresh(clusterID, " "))
	_, err := db.SqlcClient.Pool().Exec(t.Context(), `UPDATE public.clusters SET deletion_requested_at=now() WHERE id=$1`, clusterID)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, refresh(clusterID, "client_changed"))
	require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(), `SELECT telemetry_client_id FROM public.clusters WHERE id=$1`, clusterID).Scan(&client))
	require.Equal(t, "client_replacement", client)
}
