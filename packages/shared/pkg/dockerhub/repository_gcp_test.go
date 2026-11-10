package dockerhub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/stretchr/testify/require"
)

func TestGCPExplicitRegistryCredentials(t *testing.T) {
	t.Parallel()
	config := base64.URLEncoding.EncodeToString([]byte(`{"username":"explicit","password":"test-password"}`))
	auth, err := getAuthToken(t.Context(), config, "legacy-test-key")
	require.NoError(t, err)
	credentials, err := auth.Authorization()
	require.NoError(t, err)
	require.Equal(t, "explicit", credentials.Username)
	require.Equal(t, "test-password", credentials.Password)
	auth, err = getAuthToken(t.Context(), "", "legacy-test-key")
	require.NoError(t, err)
	credentials, err = auth.Authorization()
	require.NoError(t, err)
	require.Equal(t, "_json_key_base64", credentials.Username)
	require.Equal(t, "legacy-test-key", credentials.Password)
	_, err = getAuthToken(t.Context(), "invalid!", "legacy-test-key")
	require.Error(t, err, "invalid explicit credentials must not fall through to ADC")
}

//nolint:paralleltest // ADC reads process-global environment modified with t.Setenv.
func TestGCPRegistryUsesRefreshableADC(t *testing.T) {
	// Environment-backed ADC is process-global, so this test cannot run in parallel.
	var requests atomic.Int32
	var stall atomic.Bool
	var unavailable atomic.Bool
	unavailable.Store(true)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if stall.Load() {
			<-release

			return
		}
		count := requests.Add(1)
		if unavailable.Load() {
			http.Error(w, "token endpoint temporarily unavailable", http.StatusServiceUnavailable)

			return
		}
		expires := 3600
		if count == 2 {
			expires = 1
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"access_token":"test-token-%d","token_type":"Bearer","expires_in":%d,"issued_token_type":"urn:ietf:params:oauth:token-type:access_token"}`, count, expires)
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})
	dir := t.TempDir()
	subjectPath := filepath.Join(dir, "subject")
	require.NoError(t, os.WriteFile(subjectPath, []byte("test-subject"), 0o600))
	config, err := json.Marshal(map[string]any{
		"type": "external_account", "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test/providers/test",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt", "token_url": server.URL,
		"credential_source": map[string]string{"file": subjectPath},
	})
	require.NoError(t, err)
	credentialPath := filepath.Join(dir, "credentials.json")
	require.NoError(t, os.WriteFile(credentialPath, config, 0o600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialPath)
	setupCtx, cancelSetup := context.WithCancel(t.Context())
	auth, err := getAuthToken(setupCtx, "", "")
	cancelSetup()
	require.NoError(t, err)
	require.Zero(t, requests.Load(), "registry setup must discover ADC without fetching a token")
	require.Implements(t, (*authn.ContextAuthenticator)(nil), auth)
	_, err = auth.Authorization()
	require.ErrorContains(t, err, "503")
	unavailable.Store(false)
	credentials, err := auth.Authorization()
	require.NoError(t, err)
	require.Equal(t, "test-token-2", credentials.Password, "the same authenticator must recover after a token failure")
	credentials, err = auth.Authorization()
	require.NoError(t, err)
	require.NotEqual(t, "_json_key_base64", credentials.Username)
	require.Equal(t, "test-token-3", credentials.Password, "an expired token must not be frozen into basic auth")
	stall.Store(true)
	stalledAuth, err := getAuthToken(t.Context(), "", "")
	require.NoError(t, err, "a stalled token endpoint must not prevent repository setup")
	_, err = stalledAuth.Authorization()
	require.ErrorContains(t, err, "Client.Timeout", "a stalled token endpoint must not hang authentication")
	require.NoError(t, os.WriteFile(credentialPath, []byte("invalid JSON"), 0o600))
	_, err = getAuthToken(t.Context(), "", "")
	require.Error(t, err, "invalid ADC configuration must still fail setup")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(dir, "missing"))
	_, err = getAuthToken(t.Context(), "", "")
	require.Error(t, err, "ADC failures must not produce empty JSON-key credentials")
}

type registryAuthFunc func() (*authn.AuthConfig, error)

func (f registryAuthFunc) Authorization() (*authn.AuthConfig, error) { return f() }

type refreshWaitContext struct {
	context.Context //nolint:containedctx // Test context wrapper observes when cancellation becomes selectable.

	waiting chan<- struct{}
}

func (c refreshWaitContext) Done() <-chan struct{} {
	// AuthorizationContext checks Done only after joining the shared refresh.
	c.waiting <- struct{}{}

	return c.Context.Done()
}

func TestGCPRefreshCancellationDoesNotQueueRetries(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var attempts atomic.Int32
	refreshErr := errors.New("token endpoint unavailable")
	auth := &gcpRefreshAuthenticator{auth: registryAuthFunc(func() (*authn.AuthConfig, error) {
		if attempts.Add(1) == 1 {
			close(started)
			<-release

			return nil, refreshErr
		}

		return &authn.AuthConfig{Username: "oauth2accesstoken", Password: "refreshed"}, nil
	})}
	survivor := make(chan error, 1)
	go func() {
		_, err := authn.Authorization(t.Context(), auth)
		survivor <- err
	}()
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	ready, canceled := make(chan struct{}, 10), make(chan error, 10)
	waitCtx := refreshWaitContext{Context: ctx, waiting: ready}
	for range 10 {
		go func() {
			_, err := authn.Authorization(waitCtx, auth)
			canceled <- err
		}()
	}
	for range 10 {
		select {
		case <-ready:
		case <-time.After(2 * time.Second):
			t.Fatal("pull did not join the cancellable refresh wait")
		}
	}
	cancel()
	for range 10 {
		select {
		case err := <-canceled:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("canceled pull remained blocked by token refresh")
		}
	}
	require.Equal(t, int32(1), attempts.Load())
	unblock()
	require.ErrorIs(t, <-survivor, refreshErr)
	credentials, err := authn.Authorization(t.Context(), auth)
	require.NoError(t, err)
	require.Equal(t, "refreshed", credentials.Password)
	require.Equal(t, int32(2), attempts.Load(), "only a new caller should retry the failed refresh")
}

func TestGCPRepositoryAccessDenied(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/" {
					w.WriteHeader(http.StatusOK)

					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"errors":[{"code":"DENIED","message":"repository unavailable"}]}`))
			}))
			t.Cleanup(server.Close)
			repository := &GCPRemoteRepository{repositoryURL: strings.TrimPrefix(server.URL, "http://") + "/project/proxy", authToken: authn.Anonymous}
			_, err := repository.GetImage(t.Context(), "ubuntu:24.04", containerregistry.Platform{OS: "linux", Architecture: "amd64"})
			require.Error(t, err)
			require.Equal(t, status != http.StatusNotFound, errors.Is(err, ErrRepositoryAccessDenied))
			registryErr, ok := errors.AsType[*transport.Error](err)
			require.True(t, ok)
			require.Equal(t, status, registryErr.StatusCode)
		})
	}
}
