//nolint:paralleltest // env-driven tests use t.Setenv
package cmdutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- resolveAPIURL ---

func TestResolveAPIURL_Default(t *testing.T) {
	t.Setenv("E2B_API_URL", "")
	t.Setenv("E2B_DOMAIN", "")
	assert.Equal(t, "https://api.e2b.dev", resolveAPIURL())
}

func TestResolveAPIURL_Domain(t *testing.T) {
	t.Setenv("E2B_API_URL", "")
	t.Setenv("E2B_DOMAIN", "my-e2b.internal")
	assert.Equal(t, "https://api.my-e2b.internal", resolveAPIURL())
}

func TestResolveAPIURL_FullURL(t *testing.T) {
	// E2B_API_URL accepts any base URL, including http:// for local dev.
	t.Setenv("E2B_API_URL", "http://localhost:3000")
	t.Setenv("E2B_DOMAIN", "ignored.example.com") // must be ignored
	assert.Equal(t, "http://localhost:3000", resolveAPIURL())
}

func TestResolveAPIURL_TrailingSlashStripped(t *testing.T) {
	t.Setenv("E2B_API_URL", "http://localhost:3000/")
	t.Setenv("E2B_DOMAIN", "")
	assert.Equal(t, "http://localhost:3000", resolveAPIURL())
}

// --- ResolveTemplateID ---

// mockTemplateServer starts an httptest server that returns the given templates
// and checks that each request carries the expected API key.
func mockTemplateServer(t *testing.T, apiKey string, templates []templateInfo) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != apiKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(templates)
	}))
}

func TestResolveTemplateID_ByTemplateID(t *testing.T) {
	srv := mockTemplateServer(t, "test-key", []templateInfo{
		{TemplateID: "tpl-abc", BuildID: "build-uuid-1", Aliases: []string{"my-alias"}},
	})
	defer srv.Close()

	t.Setenv("E2B_API_KEY", "test-key")
	t.Setenv("E2B_API_URL", srv.URL)

	id, err := ResolveTemplateID("tpl-abc")
	require.NoError(t, err)
	assert.Equal(t, "build-uuid-1", id)
}

func TestResolveTemplateID_ByAlias(t *testing.T) {
	srv := mockTemplateServer(t, "test-key", []templateInfo{
		{TemplateID: "tpl-abc", BuildID: "build-uuid-1", Aliases: []string{"my-alias", "other-alias"}},
	})
	defer srv.Close()

	t.Setenv("E2B_API_KEY", "test-key")
	t.Setenv("E2B_API_URL", srv.URL)

	id, err := ResolveTemplateID("other-alias")
	require.NoError(t, err)
	assert.Equal(t, "build-uuid-1", id)
}

func TestResolveTemplateID_ByName(t *testing.T) {
	srv := mockTemplateServer(t, "test-key", []templateInfo{
		{TemplateID: "tpl-abc", BuildID: "build-uuid-1", Names: []string{"org/my-image"}},
	})
	defer srv.Close()

	t.Setenv("E2B_API_KEY", "test-key")
	t.Setenv("E2B_API_URL", srv.URL)

	id, err := ResolveTemplateID("org/my-image")
	require.NoError(t, err)
	assert.Equal(t, "build-uuid-1", id)
}

func TestResolveTemplateID_NotFound(t *testing.T) {
	srv := mockTemplateServer(t, "test-key", []templateInfo{
		{TemplateID: "tpl-abc", BuildID: "build-uuid-1", Aliases: []string{"existing"}},
	})
	defer srv.Close()

	t.Setenv("E2B_API_KEY", "test-key")
	t.Setenv("E2B_API_URL", srv.URL)

	_, err := ResolveTemplateID("nonexistent")
	require.ErrorContains(t, err, "not found")
	// Error message lists available aliases to help users.
	require.ErrorContains(t, err, "existing")
}

func TestResolveTemplateID_NilUUID(t *testing.T) {
	// A template whose latest build failed has buildID == nilUUID.
	srv := mockTemplateServer(t, "test-key", []templateInfo{
		{TemplateID: "broken", BuildID: nilUUID, Aliases: []string{"broken-alias"}},
	})
	defer srv.Close()

	t.Setenv("E2B_API_KEY", "test-key")
	t.Setenv("E2B_API_URL", srv.URL)

	_, err := ResolveTemplateID("broken")
	require.ErrorContains(t, err, "no successful build")
}

func TestResolveTemplateID_EmptyBuildID(t *testing.T) {
	srv := mockTemplateServer(t, "test-key", []templateInfo{
		{TemplateID: "pending", BuildID: "", Aliases: []string{"pending-alias"}},
	})
	defer srv.Close()

	t.Setenv("E2B_API_KEY", "test-key")
	t.Setenv("E2B_API_URL", srv.URL)

	_, err := ResolveTemplateID("pending")
	require.ErrorContains(t, err, "no successful build")
}

func TestResolveTemplateID_MissingAPIKey(t *testing.T) {
	t.Setenv("E2B_API_KEY", "")

	_, err := ResolveTemplateID("any-template")
	require.ErrorContains(t, err, "E2B_API_KEY")
}

func TestResolveTemplateID_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Invalid host", http.StatusBadRequest)
	}))
	defer srv.Close()

	t.Setenv("E2B_API_KEY", "test-key")
	t.Setenv("E2B_API_URL", srv.URL)

	_, err := ResolveTemplateID("any-template")
	require.ErrorContains(t, err, "400")
	require.ErrorContains(t, err, "Invalid host")
}
