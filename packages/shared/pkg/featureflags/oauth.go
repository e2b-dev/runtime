package featureflags

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	ldclient "github.com/launchdarkly/go-server-sdk/v7"
	"github.com/launchdarkly/go-server-sdk/v7/ldcomponents"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	oauthClientIDEnv     = "LAUNCH_DARKLY_OAUTH_CLIENT_ID"
	oauthClientSecretEnv = "LAUNCH_DARKLY_OAUTH_CLIENT_SECRET"
	oauthTokenURLEnv     = "LAUNCH_DARKLY_OAUTH_TOKEN_URL"
	oauthAudienceEnv     = "LAUNCH_DARKLY_OAUTH_AUDIENCE"
	baseURLEnv           = "LAUNCH_DARKLY_BASE_URL"

	// oauth2.Transport mints without a request context, so no deadline above it
	// reaches the token exchange and an unanswered token endpoint blocks forever.
	oauthTokenTimeout = 5 * time.Second

	// Sent by the SDK as Authorization and replaced by oauth2.Transport before
	// the request leaves the process.
	oauthPlaceholderSDKKey = "oauth"
)

var (
	errOAuthIncomplete      = errors.New("LaunchDarkly OAuth requires " + oauthClientIDEnv + ", " + oauthClientSecretEnv + ", and " + oauthTokenURLEnv)
	errOAuthRejected        = errors.New("LaunchDarkly OAuth identity was rejected by the token endpoint")
	errOAuthTokenFailed     = errors.New("LaunchDarkly OAuth token request failed")
	errOAuthGatewayRejected = errors.New("LaunchDarkly gateway rejected the OAuth token; it will be minted again")
)

type oauthConfig struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Audience     string
	BaseURL      string
}

func oauthConfigFromEnv() oauthConfig {
	return oauthConfig{
		ClientID:     os.Getenv(oauthClientIDEnv),
		ClientSecret: os.Getenv(oauthClientSecretEnv),
		TokenURL:     os.Getenv(oauthTokenURLEnv),
		Audience:     os.Getenv(oauthAudienceEnv),
		BaseURL:      os.Getenv(baseURLEnv),
	}
}

func (c oauthConfig) enabled() bool {
	return c.ClientID != "" || c.ClientSecret != "" || c.TokenURL != "" || c.Audience != ""
}

func (c oauthConfig) validate() error {
	if c.ClientID == "" || c.ClientSecret == "" || c.TokenURL == "" {
		return errOAuthIncomplete
	}
	if err := validateOAuthURL(baseURLEnv, c.BaseURL); err != nil {
		return err
	}

	return validateOAuthURL(oauthTokenURLEnv, c.TokenURL)
}

// identity mints the first token now. The SDK sees a failed mint as a network
// error and retries it forever, so an identity the issuer rejects has to fail
// here or a service opted into WithStartOnInitTimeout would run on fallbacks
// for good.
//
//nolint:contextcheck // the constructors take no context
func (c oauthConfig) identity() (*oauthIdentity, error) {
	credentials := clientcredentials.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		TokenURL:     c.TokenURL,
		AuthStyle:    oauth2.AuthStyleInParams,
	}
	if c.Audience != "" {
		credentials.EndpointParams = url.Values{"audience": {c.Audience}}
	}

	tokenClient := &http.Client{Timeout: oauthTokenTimeout, CheckRedirect: rejectOAuthRedirect}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, tokenClient)
	identity := newOAuthIdentity(func() (*oauth2.Token, error) { return credentials.Token(ctx) })

	if _, err := identity.cache.Token(); rejectedByIssuer(err) {
		return nil, errOAuthRejected
	}

	return identity, nil
}

func configureOAuth(cfg *ldclient.Config) (string, error) {
	oauth := oauthConfigFromEnv()
	if !oauth.enabled() {
		return "", nil
	}
	if err := oauth.validate(); err != nil {
		return "", err
	}

	identity, err := oauth.identity()
	if err != nil {
		return "", err
	}

	cfg.HTTP = ldcomponents.HTTPConfiguration().HTTPClientFactory(func() *http.Client {
		return &http.Client{
			Transport:     newOAuthTransport(identity),
			Timeout:       ldcomponents.DefaultConnectTimeout,
			CheckRedirect: rejectOAuthRedirect,
		}
	})

	return oauthPlaceholderSDKKey, nil
}

func validateOAuthURL(name, value string) error {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("%s must be an absolute HTTPS URL without credentials, query, or fragment", name)
	}

	return nil
}

func rejectOAuthRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

// rejectedByIssuer tells a token endpoint that answered and refused apart from
// one that is unreachable or failing, which the SDK's retries can outlast.
func rejectedByIssuer(err error) bool {
	var retrieveErr *oauth2.RetrieveError
	if !errors.As(err, &retrieveErr) || retrieveErr.Response == nil {
		return false
	}

	return retrieveErr.Response.StatusCode < http.StatusInternalServerError
}

type tokenSourceFunc func() (*oauth2.Token, error)

func (f tokenSourceFunc) Token() (*oauth2.Token, error) { return f() }

// oauthIdentity is the bearer token shared by every HTTP client the SDK
// creates: one cache, so the stream, events and diagnostics mint once per
// lifetime, and one forget, so a token the gateway stops accepting is replaced
// for all of them.
type oauthIdentity struct {
	mint oauth2.TokenSource

	mu    sync.Mutex
	cache oauth2.TokenSource

	// accepted is set once the gateway has served a request. Before that a
	// rejection is a misconfigured identity and must stop the SDK, as a wrong
	// SDK key does; after it a rejection is treated as transient.
	accepted atomic.Bool
}

func newOAuthIdentity(mint tokenSourceFunc) *oauthIdentity {
	identity := &oauthIdentity{mint: mint}
	identity.forget()

	return identity
}

// Token hides the issuer's error body from the SDK, which logs token errors
// verbatim.
func (i *oauthIdentity) Token() (*oauth2.Token, error) {
	i.mu.Lock()
	cache := i.cache
	i.mu.Unlock()

	token, err := cache.Token()
	if err != nil {
		return nil, errOAuthTokenFailed
	}

	return token, nil
}

func (i *oauthIdentity) forget() {
	i.mu.Lock()
	i.cache = oauth2.ReuseTokenSource(nil, i.mint)
	i.mu.Unlock()
}

// oauthTransport turns a gateway rejection of an established identity into an
// error the SDK retries. The SDK treats 401 and 403 as a revoked SDK key and
// stops for good, which is wrong for a token minted per lifetime: the gateway
// rejects every token while it cannot fetch the issuer's keys, and a token
// that went stale is fixed by minting a new one.
type oauthTransport struct {
	identity *oauthIdentity
	authed   oauth2.Transport
}

func newOAuthTransport(identity *oauthIdentity) *oauthTransport {
	return &oauthTransport{identity: identity, authed: oauth2.Transport{Source: identity}}
}

func (t *oauthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.authed.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	switch {
	case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
		if !t.identity.accepted.Load() {
			return res, nil
		}
		_ = res.Body.Close()
		t.identity.forget()

		return nil, errOAuthGatewayRejected
	case res.StatusCode < http.StatusMultipleChoices:
		t.identity.accepted.Store(true)
	}

	return res, nil
}
