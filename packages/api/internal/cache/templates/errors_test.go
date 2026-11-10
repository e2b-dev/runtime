package templatecache

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
)

func sentShouldRetry(t *testing.T, apiErr *api.APIError) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	apierrors.SendAPIError(c, apiErr)

	return recorder.Result().Header.Get(apierrors.ShouldRetryHeader)
}

func TestTemplateRef_APIError_TagNotFound(t *testing.T) {
	t.Parallel()

	tag := "v2"
	apiErr := TemplateRef{
		Identifier: "mytemplate",
		Visible:    true,
	}.APIError(templateTagNotFoundError{Tag: tag})

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "tag 'v2' does not exist for template 'mytemplate'", apiErr.ClientMsg)
}

func TestTemplateRef_APIError_DefaultTagNotFound(t *testing.T) {
	t.Parallel()

	apiErr := TemplateRef{
		Identifier: "mytemplate",
		Visible:    true,
	}.APIError(templateTagNotFoundError{Tag: "default"})

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "tag 'default' does not exist for template 'mytemplate'", apiErr.ClientMsg)
}

func TestTemplateRef_APIError_HiddenTemplate(t *testing.T) {
	t.Parallel()

	apiErr := TemplateRef{
		Identifier: "mytemplate",
		Visible:    false,
	}.APIError(ErrTemplateNotFound)

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "template 'mytemplate' not found", apiErr.ClientMsg)
}

func TestTemplateRef_APIError_HiddenAccessDenied(t *testing.T) {
	t.Parallel()

	apiErr := TemplateRef{
		Identifier: "mytemplate",
		Visible:    false,
	}.APIError(ErrAccessDenied)

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "template 'mytemplate' not found", apiErr.ClientMsg)
}

func TestErrorToAPIError_TemplateNotFound(t *testing.T) {
	t.Parallel()

	apiErr := ErrorToAPIError(ErrTemplateNotFound, "mytemplate")

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "template 'mytemplate' not found", apiErr.ClientMsg)
	assert.Equal(t, "false", sentShouldRetry(t, apiErr))
}

func TestErrorToAPIError_UsesIdentifierFromNotFoundError(t *testing.T) {
	t.Parallel()

	apiErr := ErrorToAPIError(templateNotFoundError{Identifier: "myteam/desktop"}, "desktop")

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "template 'myteam/desktop' not found", apiErr.ClientMsg)
	assert.Equal(t, "false", sentShouldRetry(t, apiErr))
}

func TestErrorToAPIError_TemplateTagNotFound(t *testing.T) {
	t.Parallel()

	apiErr := ErrorToAPIError(templateTagNotFoundError{Tag: "dev"}, "myteam/desktop")

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "tag 'dev' does not exist for template 'myteam/desktop'", apiErr.ClientMsg)
	assert.Equal(t, "false", sentShouldRetry(t, apiErr))
}

func TestToAPIError_UsesIdentifierFromNotFoundError(t *testing.T) {
	t.Parallel()

	apiErr := ToAPIError(templateNotFoundError{Identifier: "myteam/desktop"}, "desktop")

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "template 'myteam/desktop' not found", apiErr.ClientMsg)
}

func TestToAPIError_TemplateTagNotFound(t *testing.T) {
	t.Parallel()

	apiErr := ToAPIError(templateTagNotFoundError{Tag: "dev"}, "myteam/desktop")

	assert.Equal(t, http.StatusNotFound, apiErr.Code)
	assert.Equal(t, "tag 'dev' does not exist for template 'myteam/desktop'", apiErr.ClientMsg)
}

func TestTemplateRef_APIError_IdentifierEqualsTemplateID(t *testing.T) {
	t.Parallel()

	apiErr := TemplateRef{
		Identifier: "tmpl-abc",
		Visible:    true,
	}.APIError(templateTagNotFoundError{Tag: "default"})

	assert.Equal(t, "tag 'default' does not exist for template 'tmpl-abc'", apiErr.ClientMsg)
}

func TestErrorToAPIError_AccessDenied(t *testing.T) {
	t.Parallel()

	apiErr := ErrorToAPIError(ErrAccessDenied, "mytemplate")

	assert.Equal(t, http.StatusForbidden, apiErr.Code)
	assert.Equal(t, "you don't have access to template 'mytemplate'", apiErr.ClientMsg)
	assert.Equal(t, "false", sentShouldRetry(t, apiErr))
}

func TestErrorToAPIError_ClusterMismatch(t *testing.T) {
	t.Parallel()

	apiErr := ErrorToAPIError(ErrClusterMismatch, "mytemplate")

	assert.Equal(t, http.StatusBadRequest, apiErr.Code)
	assert.Equal(t, "template 'mytemplate' is not available in the requested cluster", apiErr.ClientMsg)
	assert.Equal(t, "false", sentShouldRetry(t, apiErr))
}

func TestErrorToAPIError_Unknown(t *testing.T) {
	t.Parallel()

	backend := errors.New("boom")
	apiErr := ErrorToAPIError(fmt.Errorf("fetching template by alias: %w", backend), "mytemplate")

	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
	assert.Equal(t, "error resolving template 'mytemplate'", apiErr.ClientMsg)
	require.ErrorIs(t, apiErr.Err, backend)
	assert.Equal(t, "true", sentShouldRetry(t, apiErr))
}
