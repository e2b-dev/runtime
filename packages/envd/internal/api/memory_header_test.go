package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// TestPostInit_ReportsMemoryProtection pins the header contract the orchestrator decodes
// against: X-Envd-Memory is on every /init response, error responses included, its JSON
// is exactly the four fields in bytes, and the handler serialises the value New read at
// construction rather than reading anything itself.
func TestPostInit_ReportsMemoryProtection(t *testing.T) {
	t.Parallel()

	postInit := func(t *testing.T, a *API, body []byte) *httptest.ResponseRecorder {
		t.Helper()

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", bytes.NewReader(body))
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		a.PostInit(rec, req)

		return rec
	}

	t.Run("on an error response, with the value the freezer reports", func(t *testing.T) {
		t.Parallel()

		// A manager that cannot address cgroups by path yields a partial zero, which is
		// distinguishable from the struct's zero value: the header below can only read
		// this way if New asked the freezer.
		rec := postInit(t, newAPIWithCgroupManager(cgroups.NewNoopManager()), []byte("{not json"))

		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.JSONEq(t, `{"request":0,"low":0,"floor":0,"partial":true}`, rec.Header().Get("X-Envd-Memory"))
	})

	t.Run("on a 204", func(t *testing.T) {
		t.Parallel()

		api := newAPIWithCgroupManager(&fakeCgroupManager{})
		api.isNotFC = true
		body, err := json.Marshal(PostInitJSONBody{})
		require.NoError(t, err)

		rec := postInit(t, api, body)

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.JSONEq(t, `{"request":0,"low":0,"floor":0,"partial":true}`, rec.Header().Get("X-Envd-Memory"))
	})

	// No cgroupfs exists here, so a header carrying these values can only have come from
	// the cached struct: the handler serialises and does not read.
	t.Run("serialises the cached value, in bytes", func(t *testing.T) {
		t.Parallel()

		api := newAPIWithCgroupManager(cgroups.NewNoopManager())
		api.memory = cgroups.MemoryProtection{Request: 134217728, Low: 268435456, Floor: 67108864, Partial: false}

		rec := postInit(t, api, []byte("{not json"))

		assert.JSONEq(t, `{"request":134217728,"low":268435456,"floor":67108864,"partial":false}`, rec.Header().Get("X-Envd-Memory"))
	})
}
