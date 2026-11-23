package featureflags

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjectedFlagCredentialsActivateAndRotate(t *testing.T) {
	t.Parallel()
	requests := make(chan string, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Header.Get("Authorization")
		switch r.Header.Get("Authorization") {
		case "Bearer accepted":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	file := filepath.Join(dir, "flags")
	require.NoError(t, os.WriteFile(file, []byte("first"), 0o600))
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/all", nil)
	require.NoError(t, err)
	tr := projectedTransport{file: file, host: req.URL.Host, base: server.Client().Transport}
	pending, err := tr.RoundTrip(req)
	if pending != nil {
		require.NoError(t, pending.Body.Close())
	}
	require.Error(t, err)
	require.Empty(t, requests)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "registered"), []byte("registered-cluster"), 0o600))
	for _, token := range []string{"first", "accepted", "rotated", "accepted"} {
		require.NoError(t, os.WriteFile(file, []byte(token), 0o600))
		res, err := tr.RoundTrip(req)
		require.Equal(t, "Bearer "+token, <-requests)
		if token == "rotated" {
			require.Error(t, err)
			require.Nil(t, res)

			continue
		}
		require.NoError(t, err)
		if token == "first" {
			assert.Equal(t, http.StatusUnauthorized, res.StatusCode)
		} else {
			assert.Equal(t, http.StatusOK, res.StatusCode)
		}
		require.NoError(t, res.Body.Close())
	}
}
