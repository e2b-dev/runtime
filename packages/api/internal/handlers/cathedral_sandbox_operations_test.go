package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestHashCathedralCreateRequestIsCanonicalAndBodyBound(t *testing.T) {
	t.Parallel()

	firstMetadata := api.SandboxMetadata{"b": "2", "a": "1"}
	secondMetadata := api.SandboxMetadata{"a": "1", "b": "2"}
	first, err := hashCathedralCreateRequest(api.PostSandboxesJSONRequestBody{
		TemplateID: "base",
		Metadata:   &firstMetadata,
	})
	require.NoError(t, err)
	second, err := hashCathedralCreateRequest(api.PostSandboxesJSONRequestBody{
		TemplateID: "base",
		Metadata:   &secondMetadata,
	})
	require.NoError(t, err)
	different, err := hashCathedralCreateRequest(api.PostSandboxesJSONRequestBody{
		TemplateID: "other",
		Metadata:   &secondMetadata,
	})
	require.NoError(t, err)

	assert.Len(t, first, 64)
	assert.Equal(t, first, second)
	assert.NotEqual(t, first, different)
}

func TestCathedralIdempotencyKeyValidation(t *testing.T) {
	t.Parallel()

	assert.True(t, cathedralIdempotencyKeyPattern.MatchString("box-op:create_1"))
	assert.False(t, cathedralIdempotencyKeyPattern.MatchString("short"))
	assert.False(t, cathedralIdempotencyKeyPattern.MatchString("contains space"))
	assert.False(t, cathedralIdempotencyKeyPattern.MatchString(strings.Repeat("x", 129)))
}

func TestReadyCathedralCreateReplayReturnsExactStoredResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	stored := `{"sandboxID":"i-bound","templateID":"base","clientID":"","envdVersion":"0.5.0"}`
	claim := &cathedralCreateClaim{
		key:           "cathedral-replay-1",
		requestSHA256: "a",
	}

	resumed, proceed := (&APIStore{}).resumeCathedralCreate(c, claim, queries.CathedralSandboxOperation{
		IdempotencyKey: claim.key,
		RequestSha256:  claim.requestSHA256,
		SandboxID:      "i-bound",
		State:          "ready",
		ResponseJson:   &stored,
	})

	assert.Nil(t, resumed)
	assert.False(t, proceed)
	assert.Equal(t, http.StatusCreated, recorder.Code)
	assert.Equal(t, claim.key, recorder.Header().Get(cathedralIdempotencyAckHeader))
	assert.Equal(t, stored, recorder.Body.String())
}

func TestCathedralCreateReplayRejectsDifferentBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	claim := &cathedralCreateClaim{
		key:           "cathedral-conflict-1",
		requestSHA256: "new",
	}

	resumed, proceed := (&APIStore{}).resumeCathedralCreate(c, claim, queries.CathedralSandboxOperation{
		IdempotencyKey: claim.key,
		RequestSha256:  "old",
		SandboxID:      "i-bound",
		State:          "creating",
	})

	assert.Nil(t, resumed)
	assert.False(t, proceed)
	assert.Equal(t, http.StatusConflict, recorder.Code)
}

func TestCathedralCapabilitiesFailClosedOnFork(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	(&APIStore{}).GetV1CathedralCapabilities(c)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `{
		"schema": 1,
		"durable_create_idempotency": true,
		"operation_lookup": true,
		"safe_fork": false
	}`, recorder.Body.String())
}
