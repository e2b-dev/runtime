package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/metadata"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

type stubTokenSource struct {
	token *oauth2.Token
	err   error
}

func (s stubTokenSource) Token() (*oauth2.Token, error) {
	return s.token, s.err
}

// blockingTokenSource answers no Token call until release is closed.
type blockingTokenSource struct {
	release chan struct{}
}

func (s blockingTokenSource) Token() (*oauth2.Token, error) {
	<-s.release

	return &oauth2.Token{AccessToken: "late"}, nil
}

func TestNewGRPCResumeAuth(t *testing.T) {
	t.Parallel()

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()

		auth, err := newGrpcResumeAuth(t.Context(), GRPCOAuthConfig{})
		require.NoError(t, err)
		require.IsType(t, noopGrpcResumeAuth{}, auth)
	})

	t.Run("partial config", func(t *testing.T) {
		t.Parallel()

		auth, err := newGrpcResumeAuth(t.Context(), GRPCOAuthConfig{ClientID: "client-id"})
		require.Error(t, err)
		require.Nil(t, auth)
	})

	t.Run("enabled", func(t *testing.T) {
		t.Parallel()

		auth, err := newGrpcResumeAuth(t.Context(), GRPCOAuthConfig{
			ClientID:     " client-id ",
			ClientSecret: " secret ",
			TokenURL:     " https://tokens.example.com ",
		})
		require.NoError(t, err)
		require.IsType(t, &oauthGrpcResumeAuth{}, auth)
	})
}

func TestOAuthGrpcResumeAuthRequestsAutoresumeScope(t *testing.T) {
	t.Parallel()

	scopeCh := make(chan string, 1)
	errCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			errCh <- err
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}
		scopeCh <- r.Form.Get("scope")

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"access_token": "token",
			"token_type":   "bearer",
			"expires_in":   3600,
		}); err != nil {
			errCh <- err
		}
	}))
	t.Cleanup(server.Close)

	auth, err := newGrpcResumeAuth(t.Context(), GRPCOAuthConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		TokenURL:     server.URL,
	})
	require.NoError(t, err)

	ctx, err := auth.authorize(t.Context())
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{"Bearer token"}, md.Get(proxygrpc.MetadataAuthorization))
	require.Equal(t, grpcResumeAuthScope, <-scopeCh)
	require.Empty(t, errCh)
}

func TestNoopGrpcResumeAuthAuthorize(t *testing.T) {
	t.Parallel()

	ctx, err := (noopGrpcResumeAuth{}).authorize(t.Context())
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.False(t, ok)
	require.Empty(t, md.Get(proxygrpc.MetadataAuthorization))
}

func TestOAuthGrpcResumeAuthAuthorize(t *testing.T) {
	t.Parallel()

	ctx, err := (&oauthGrpcResumeAuth{fetch: stubTokenSource{token: &oauth2.Token{AccessToken: "token"}}.Token}).authorize(t.Context())
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{"Bearer token"}, md.Get(proxygrpc.MetadataAuthorization))
}

func TestOAuthGrpcResumeAuthAuthorizeError(t *testing.T) {
	t.Parallel()

	tokenErr := errors.New("token failed")
	ctx, err := (&oauthGrpcResumeAuth{fetch: stubTokenSource{err: tokenErr}.Token}).authorize(t.Context())
	require.ErrorIs(t, err, tokenErr)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.False(t, ok)
	require.Empty(t, md.Get(proxygrpc.MetadataAuthorization))
}

// An attempt whose context has already ended starts no fetch, and one that
// joined a fetch still running when its context ends stops waiting at once;
// the fetch carries on and adds no authorization to either.
func TestOAuthGrpcResumeAuthAuthorizeEndsWithTheContext(t *testing.T) {
	t.Parallel()

	source := blockingTokenSource{release: make(chan struct{})}
	t.Cleanup(func() { close(source.release) })
	auth := &oauthGrpcResumeAuth{fetch: source.Token}

	ended, cancel := context.WithCancel(t.Context())
	cancel()
	ctx, err := auth.authorize(ended)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, auth.current, "an attempt that had already ended started a fetch")

	md, ok := metadata.FromOutgoingContext(ctx)
	require.False(t, ok)
	require.Empty(t, md.Get(proxygrpc.MetadataAuthorization))

	joined, leave := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := auth.authorize(joined)
		result <- err
	}()
	require.Eventually(t, func() bool {
		auth.mu.Lock()
		defer auth.mu.Unlock()

		return auth.current != nil
	}, 5*time.Second, 5*time.Millisecond)

	start := time.Now()
	leave()
	require.ErrorIs(t, <-result, context.Canceled)
	require.Less(t, time.Since(start), 5*time.Second)
}

// sequenceFetch answers its calls with the listed results in order and
// counts them.
type sequenceFetch struct {
	results []tokenFetch
	calls   atomic.Int32
}

func (s *sequenceFetch) fetch() (*oauth2.Token, error) {
	call := int(s.calls.Add(1)) - 1
	if call >= len(s.results) {
		call = len(s.results) - 1
	}

	return s.results[call].token, s.results[call].err
}

// A valid token is handed out without a fetch; a failed fetch is not kept,
// so the next attempt fetches again.
func TestOAuthGrpcResumeAuthReusesAValidTokenAndNoFailure(t *testing.T) {
	t.Parallel()

	fetchErr := errors.New("token endpoint down")
	source := &sequenceFetch{results: []tokenFetch{
		{err: fetchErr},
		{token: &oauth2.Token{AccessToken: "first", Expiry: time.Now().Add(time.Hour)}},
		{token: &oauth2.Token{AccessToken: "unexpected"}},
	}}
	auth := &oauthGrpcResumeAuth{fetch: source.fetch}

	_, err := auth.authorize(t.Context())
	require.ErrorIs(t, err, fetchErr)
	require.ErrorIs(t, err, errResumeToken)

	for range 3 {
		ctx, err := auth.authorize(t.Context())
		require.NoError(t, err)
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"Bearer first"}, md.Get(proxygrpc.MetadataAuthorization))
	}
	require.EqualValues(t, 2, source.calls.Load())
}

// Concurrent attempts against a stalled token endpoint share one fetch: the
// endpoint sees one request, every attempt ends with its own context while
// the fetch is still blocked, and once the fetch is released it is cleared
// without a second request. The fetch is held by the test, not by a
// timeout, so the outcome does not depend on how fast the callers are
// scheduled.
func TestOAuthGrpcResumeAuthConcurrentAttemptsShareOneFetch(t *testing.T) {
	t.Parallel()

	const attempts = 50

	var requests atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	// A token once released: a bare answer would make the oauth2 client
	// probe the other auth style with a second request.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"token","token_type":"bearer","expires_in":3600}`))
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	auth, err := newGrpcResumeAuthWithTimeout(t.Context(), GRPCOAuthConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		TokenURL:     server.URL,
	}, time.Minute)
	require.NoError(t, err)
	oauth, ok := auth.(*oauthGrpcResumeAuth)
	require.True(t, ok)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Go(func() {
			_, err := auth.authorize(ctx)
			errs <- err
		})
	}
	require.Eventually(t, func() bool { return requests.Load() == 1 }, 5*time.Second, 5*time.Millisecond)

	cancel()
	ended := make(chan struct{})
	go func() {
		wg.Wait()
		close(ended)
	}()
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the attempts did not end with their context while the fetch was blocked")
	}
	close(errs)
	for err := range errs {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.EqualValues(t, 1, requests.Load())

	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		oauth.mu.Lock()
		defer oauth.mu.Unlock()

		return oauth.current == nil && oauth.token.Valid()
	}, 5*time.Second, 5*time.Millisecond, "the fetch was not published and cleared once it ended")
	require.EqualValues(t, 1, requests.Load(), "the fetch ended with another request")
}

// stalledTokenServer answers no token request until the test ends; the
// handlers are released before the server closes, since it does not notice
// a client that gave up on an unread request body.
func stalledTokenServer(t *testing.T) *httptest.Server {
	t.Helper()

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	return server
}

// A token endpoint that never answers fails the refresh within the timeout
// instead of holding the attempt.
func TestOAuthGrpcResumeAuthTokenRefreshIsBounded(t *testing.T) {
	t.Parallel()

	server := stalledTokenServer(t)

	auth, err := newGrpcResumeAuthWithTimeout(t.Context(), GRPCOAuthConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		TokenURL:     server.URL,
	}, 100*time.Millisecond)
	require.NoError(t, err)

	start := time.Now()
	_, err = auth.authorize(t.Context())
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
}
