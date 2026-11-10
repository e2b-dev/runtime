package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

// An update with a DNS server that is not an IP address is refused before the
// sandbox is touched.
func TestPutSandboxNetwork_InvalidDnsServer(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/sandboxes/sbx-dns/network",
		strings.NewReader(`{"allowOut":["example.com"],"denyOut":["0.0.0.0/0"],"dnsServers":["dns.example.com"]}`))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &dbtypes.Team{ID: uuid.New(), Slug: "test-team"},
	})

	store := &APIStore{}
	store.PutSandboxesSandboxIDNetwork(ginCtx, "sbx-dns")
	ginCtx.Writer.WriteHeaderNow()

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "invalid DNS server dns.example.com: must be an IPv4 address")
}
