// Package acr implements Azure Container Registry (ACR) authentication shared
// by the artifacts-registry and dockerhub backends. ACR does not accept AAD
// access tokens directly for docker operations; they must first be exchanged
// for an ACR refresh token via the registry's /oauth2/exchange endpoint.
package acr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/containers/azcontainerregistry"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

const (
	// tokenUsername is the well-known username ACR expects with a refresh-token password.
	tokenUsername = "00000000-0000-0000-0000-000000000000"

	// aadScope is the public-cloud ACR audience; sovereign clouds differ.
	aadScope = "https://containerregistry.azure.net/.default"

	// walkTimeout bounds one detached walk: the AAD credential chain (which may probe IMDS) plus the exchange.
	walkTimeout = 45 * time.Second

	// On re-walk failure a cached token keeps serving until hardExpiry, under ACR's ~3h refresh-token lifetime.
	refreshTokenSoftTTL = time.Hour
	refreshTokenHardTTL = 150 * time.Minute
)

// Authenticator implements authn.Authenticator and authn.ContextAuthenticator
// for Azure Container Registry by exchanging an AAD access token for a cached
// ACR refresh token. Safe for concurrent use.
type Authenticator struct {
	service    string // login server, e.g. myregistry.azurecr.io
	credential azcore.TokenCredential
	authClient *azcontainerregistry.AuthenticationClient

	mu           sync.Mutex
	refreshToken string
	softExpiry   time.Time
	hardExpiry   time.Time
	pending      *inflight
}

// inflight is one shared token walk; done closes once token/err are set.
type inflight struct {
	done  chan struct{}
	token string
	err   error
}

var (
	_ authn.Authenticator        = (*Authenticator)(nil)
	_ authn.ContextAuthenticator = (*Authenticator)(nil)
)

// NewAuthenticator creates an ACR authenticator for the given login server
// (e.g. myregistry.azurecr.io) backed by the given AAD credential.
func NewAuthenticator(loginServer string, credential azcore.TokenCredential, opts *azcontainerregistry.AuthenticationClientOptions) (*Authenticator, error) {
	authClient, err := azcontainerregistry.NewAuthenticationClient(fmt.Sprintf("https://%s", loginServer), opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create acr authentication client: %w", err)
	}

	return &Authenticator{
		service:    loginServer,
		credential: credential,
		authClient: authClient,
	}, nil
}

// Authorization implements authn.Authenticator for callers without a context.
func (a *Authenticator) Authorization() (*authn.AuthConfig, error) {
	return a.AuthorizationContext(context.Background())
}

// AuthorizationContext returns docker credentials for the registry. The wait
// is bounded by the caller's context; the walk runs detached so one cancelled
// caller cannot poison the result every queued caller shares.
func (a *Authenticator) AuthorizationContext(ctx context.Context) (*authn.AuthConfig, error) {
	a.mu.Lock()
	if a.refreshToken != "" && time.Now().Before(a.softExpiry) {
		defer a.mu.Unlock()

		return &authn.AuthConfig{Username: tokenUsername, Password: a.refreshToken}, nil
	}

	if a.pending == nil {
		a.pending = &inflight{done: make(chan struct{})}
		//nolint:contextcheck // the walk is deliberately detached: one cancelled caller must not poison the shared result
		go a.lead(a.pending)
	}
	p := a.pending
	a.mu.Unlock()

	select {
	case <-p.done:
		if p.err != nil {
			return nil, p.err
		}

		return &authn.AuthConfig{Username: tokenUsername, Password: p.token}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Invalidate drops the cached token so the next call re-walks; call it on an
// authentication failure (see IsUnauthorized).
func (a *Authenticator) Invalidate() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.refreshToken = ""
	a.softExpiry = time.Time{}
	a.hardExpiry = time.Time{}
}

// lead performs one token walk and publishes the result to every waiter.
func (a *Authenticator) lead(p *inflight) {
	ctx, cancel := context.WithTimeout(context.Background(), walkTimeout)
	defer cancel()

	token, err := a.walk(ctx)

	a.mu.Lock()
	a.pending = nil
	now := time.Now()
	switch {
	case err == nil:
		a.refreshToken = token
		a.softExpiry = now.Add(refreshTokenSoftTTL)
		a.hardExpiry = now.Add(refreshTokenHardTTL)
		p.token = token
	case a.refreshToken != "" && now.Before(a.hardExpiry):
		// A transient re-walk failure must not fail pulls while the cached token still authenticates.
		p.token = a.refreshToken
	default:
		p.err = err
	}
	a.mu.Unlock()

	close(p.done)
}

// walk exchanges an AAD access token for an ACR refresh token; retries
// (429/5xx, honoring Retry-After) live in the SDK's azcore pipeline.
func (a *Authenticator) walk(ctx context.Context) (string, error) {
	token, err := a.credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{aadScope}})
	if err != nil {
		return "", fmt.Errorf("failed to get AAD access token: %w", err)
	}

	resp, err := a.authClient.ExchangeAADAccessTokenForACRRefreshToken(
		ctx,
		azcontainerregistry.PostContentSchemaGrantTypeAccessToken,
		a.service,
		&azcontainerregistry.AuthenticationClientExchangeAADAccessTokenForACRRefreshTokenOptions{
			AccessToken: &token.Token,
		},
	)
	if err != nil {
		return "", fmt.Errorf("failed to exchange AAD token for ACR refresh token: %w", err)
	}

	if resp.RefreshToken == nil || *resp.RefreshToken == "" {
		return "", errors.New("acr token exchange response did not contain a refresh token")
	}

	return *resp.RefreshToken, nil
}

// IsUnauthorized reports whether a registry operation failed authentication —
// the signal to Invalidate the cached token and retry once.
func IsUnauthorized(err error) bool {
	if terr, ok := errors.AsType[*transport.Error](err); ok {
		return terr.StatusCode == http.StatusUnauthorized || terr.StatusCode == http.StatusForbidden
	}

	return false
}
