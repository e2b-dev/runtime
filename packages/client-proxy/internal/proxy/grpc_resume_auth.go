package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc/metadata"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

const grpcResumeAuthScope = proxygrpc.ScopeSandboxLifecycle

// grpcResumeTokenTimeout bounds one token fetch. An attempt that stops
// waiting for the fetch leaves it running for the callers that still wait;
// this is how long that can last.
const grpcResumeTokenTimeout = 10 * time.Second

// errResumeToken marks an attempt that could not obtain its token; the
// flight answers it like its other local errors.
var errResumeToken = errors.New("get api grpc OAuth token")

type grpcResumeAuth interface {
	authorize(ctx context.Context) (context.Context, error)
}

type noopGrpcResumeAuth struct{}

// oauthGrpcResumeAuth hands every attempt the cached token while it is
// valid and otherwise runs one fetch at a time, shared by every attempt
// that arrives while it is in flight. A failed fetch is not cached, so the
// next attempt starts a new one; a stalled token endpoint therefore costs
// one goroutine per fetch, not one per attempt.
type oauthGrpcResumeAuth struct {
	fetch func() (*oauth2.Token, error)

	mu      sync.Mutex
	token   *oauth2.Token
	current *tokenFetch
}

// tokenFetch is one fetch in flight; done is closed once token and err are
// set.
type tokenFetch struct {
	done  chan struct{}
	token *oauth2.Token
	err   error
}

func (c GRPCOAuthConfig) Enabled() bool {
	return strings.TrimSpace(c.ClientID) != "" ||
		strings.TrimSpace(c.ClientSecret) != "" ||
		strings.TrimSpace(c.TokenURL) != ""
}

func newGrpcResumeAuth(ctx context.Context, c GRPCOAuthConfig) (grpcResumeAuth, error) {
	return newGrpcResumeAuthWithTimeout(ctx, c, grpcResumeTokenTimeout)
}

func newGrpcResumeAuthWithTimeout(ctx context.Context, c GRPCOAuthConfig, tokenTimeout time.Duration) (grpcResumeAuth, error) {
	if !c.Enabled() {
		return noopGrpcResumeAuth{}, nil
	}

	if strings.TrimSpace(c.ClientID) == "" ||
		strings.TrimSpace(c.ClientSecret) == "" ||
		strings.TrimSpace(c.TokenURL) == "" {
		return nil, errors.New("api grpc OAuth client ID, client secret, and token URL are required when OAuth is configured")
	}

	oauthConfig := clientcredentials.Config{
		ClientID:     strings.TrimSpace(c.ClientID),
		ClientSecret: strings.TrimSpace(c.ClientSecret),
		TokenURL:     strings.TrimSpace(c.TokenURL),
		Scopes:       []string{grpcResumeAuthScope},
	}

	return &oauthGrpcResumeAuth{
		fetch: func() (*oauth2.Token, error) {
			fetchCtx, cancel := context.WithTimeout(ctx, tokenTimeout)
			defer cancel()

			return oauthConfig.Token(fetchCtx)
		},
	}, nil
}

func (noopGrpcResumeAuth) authorize(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

// authorize waits for a token only as long as ctx lives; a fetch it stops
// waiting for carries on for the other attempts waiting on it.
func (a *oauthGrpcResumeAuth) authorize(ctx context.Context) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return ctx, err
	}

	token, err := a.getToken(ctx)
	if err != nil {
		return ctx, err
	}

	return metadata.AppendToOutgoingContext(ctx, proxygrpc.MetadataAuthorization, "Bearer "+token.AccessToken), nil
}

func (a *oauthGrpcResumeAuth) getToken(ctx context.Context) (*oauth2.Token, error) {
	a.mu.Lock()
	if a.token.Valid() {
		token := a.token
		a.mu.Unlock()

		return token, nil
	}

	fetch := a.current
	if fetch == nil {
		fetch = &tokenFetch{done: make(chan struct{})}
		a.current = fetch
		go a.run(fetch)
	}
	a.mu.Unlock()

	select {
	case <-fetch.done:
		if fetch.err != nil {
			return nil, fmt.Errorf("%w: %w", errResumeToken, fetch.err)
		}

		return fetch.token, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// run completes fetch and publishes it to every waiter. The fetch is bounded
// by the timeout its function applies, not by any waiter's context.
func (a *oauthGrpcResumeAuth) run(fetch *tokenFetch) {
	func() {
		// Off the request goroutine there is no net/http to recover a
		// panic; one here fails this fetch, not the process. The attempt
		// that waits on it logs the failure with its request.
		defer func() {
			if p := recover(); p != nil {
				fetch.token, fetch.err = nil, fmt.Errorf("token fetch panicked: %v", p)
			}
		}()
		fetch.token, fetch.err = a.fetch()
	}()

	a.mu.Lock()
	if fetch.err == nil {
		a.token = fetch.token
	}
	a.current = nil
	a.mu.Unlock()

	close(fetch.done)
}
