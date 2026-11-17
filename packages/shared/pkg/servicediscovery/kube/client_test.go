package kube

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

const (
	testAPIEndpoint = "https://dns-endpoint.example"
	adcAccessToken  = "adc-access-token"
)

// redirectOnceTransport answers the first request with a redirect to `to` and
// records every hop, so a test can watch what the client sends after following
// one.
type redirectOnceTransport struct {
	to   string
	hops []*http.Request
}

func (rt *redirectOnceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.hops = append(rt.hops, r)
	if len(rt.hops) == 1 {
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{rt.to}},
			Body:       http.NoBody,
			Request:    r,
		}, nil
	}

	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
}

type recordingTransport struct {
	req *http.Request
}

func (rt *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.req = r

	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
}

// Points GOOGLE_APPLICATION_CREDENTIALS at a service account whose token
// endpoint is a local stub, so both the ADC lookup and the token exchange
// resolve without leaving the test.
func applicationDefaultCredentials(t *testing.T) *mintedToken {
	t.Helper()

	minted := &mintedToken{}
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		minted.record(t, r)
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]any{
			"access_token": adcAccessToken,
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
		assert.NoError(t, err, "encoding token response")
	}))
	t.Cleanup(tokens.Close)

	credentialsAt(t, tokens.URL)

	return minted
}

func credentialsAt(t *testing.T, tokenURI string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	credentials, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"client_email":   "discovery@example.iam.gserviceaccount.com",
		"private_key_id": "test-key-id",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"token_uri":      tokenURI,
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "adc.json")
	require.NoError(t, os.WriteFile(path, credentials, 0o600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
}

// The two-legged JWT flow carries the requested scopes in the signed
// assertion's scope claim rather than as a form field.
type mintedToken struct {
	scopes string
}

func (m *mintedToken) record(t *testing.T, r *http.Request) {
	t.Helper()

	if !assert.NoError(t, r.ParseForm(), "parsing token request") { //nolint:testifylint // called from the HTTP handler goroutine
		return
	}

	parts := strings.Split(r.PostForm.Get("assertion"), ".")
	if len(parts) != 3 {
		return
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if !assert.NoError(t, err, "decoding assertion payload") {
		return
	}

	var claims struct {
		Scope string `json:"scope"`
	}
	if !assert.NoError(t, json.Unmarshal(payload, &claims), "decoding assertion claims") {
		return
	}

	m.scopes = claims.Scope
}

func TestNewRESTConfig_NoEndpointKeepsTheInClusterPath(t *testing.T) {
	// Otherwise this passes only because the runner is not itself a pod.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	_, err := newRESTConfig(t.Context(), "")
	require.ErrorIs(t, err, rest.ErrNotInCluster)
}

func TestNewClient_PropagatesAValidationFailure(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	client, err := NewClient(t.Context(), "http://dns-endpoint.example")
	require.ErrorIs(t, err, ErrEndpointNotHTTPS)
	assert.Nil(t, client)
}

//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_EndpointIsReachedWithAnADCBearerToken(t *testing.T) {
	_ = applicationDefaultCredentials(t)

	config, err := newRESTConfig(t.Context(), testAPIEndpoint)
	require.NoError(t, err)
	assert.Equal(t, testAPIEndpoint, config.Host)
	assert.Empty(t, config.BearerToken, "the token is minted per request, never pinned on the config")
	assert.Empty(t, config.TLSClientConfig.CAData, "the DNS endpoint serves a publicly trusted certificate")
	require.NotNil(t, config.WrapTransport)

	recorder := &recordingTransport{}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, testAPIEndpoint+"/api/v1/pods", nil)
	require.NoError(t, err)

	//nolint:bodyclose // the recording transport returns http.NoBody
	_, err = config.WrapTransport(recorder).RoundTrip(req)
	require.NoError(t, err)

	require.NotNil(t, recorder.req)
	assert.Equal(t, "Bearer "+adcAccessToken, recorder.req.Header.Get("Authorization"))
}

// A scheme-less or http:// endpoint would send the bearer in clear, and the
// value an operator reaches for -- the cluster's DNS endpoint output -- is a
// bare hostname.
func TestNewRESTConfig_RejectsAnEndpointThatIsNotHTTPS(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{
		"abc123-456.example.goog",
		"http://abc123-456.example.goog",
		"https://",
		// A non-empty authority with no host in it: this otherwise surfaces
		// deep inside the TLS handshake, which the caching decorator swallows.
		"https://:6443",
	} {
		_, err := newRESTConfig(t.Context(), endpoint)
		require.ErrorIs(t, err, ErrEndpointNotHTTPS, "endpoint %q must be rejected", endpoint)
	}
}

//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_RefusesToLeaveTheConfiguredOrigin(t *testing.T) {
	_ = applicationDefaultCredentials(t)

	config, err := newRESTConfig(t.Context(), testAPIEndpoint)
	require.NoError(t, err)

	for name, target := range map[string]string{
		"another host":                      "https://elsewhere.example/api/v1/pods",
		"downgraded to http":                "http://dns-endpoint.example/api/v1/pods",
		"another host on http":              "http://elsewhere.example/api/v1/pods",
		"a different port on the same host": "https://dns-endpoint.example:8443/api/v1/pods",
	} {
		t.Run(name, func(t *testing.T) {
			recorder := &recordingTransport{}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
			require.NoError(t, err)

			//nolint:bodyclose // the boundary check returns before any response exists
			_, err = config.WrapTransport(recorder).RoundTrip(req)
			require.ErrorIs(t, err, ErrOffEndpointOrigin)
			assert.Nil(t, recorder.req, "the request must not be sent at all")
		})
	}
}

//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_MintsTheCredentialWithTheEmailScope(t *testing.T) {
	minted := applicationDefaultCredentials(t)

	config, err := newRESTConfig(t.Context(), testAPIEndpoint)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, testAPIEndpoint+"/api/v1/pods", nil)
	require.NoError(t, err)

	//nolint:bodyclose // the recording transport returns http.NoBody
	_, err = config.WrapTransport(&recordingTransport{}).RoundTrip(req)
	require.NoError(t, err)

	assert.Contains(t, minted.scopes, cloudPlatformScope)
	assert.Contains(t, minted.scopes, userinfoEmailScope)
}

// The endpoint validation refuses a non-https endpoint because the bearer would
// go on the wire in clear. That is a config-time check; a redirect back to the
// same host over http reaches the wire, so the transport has to refuse it too.
//
//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_DoesNotFollowTheCredentialToPlaintext(t *testing.T) {
	_ = applicationDefaultCredentials(t)

	config, err := newRESTConfig(t.Context(), testAPIEndpoint)
	require.NoError(t, err)

	base := &redirectOnceTransport{to: "http://dns-endpoint.example/downgraded"}
	client := &http.Client{Transport: config.WrapTransport(base)}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, testAPIEndpoint+"/api/v1/pods", nil)
	require.NoError(t, err)

	_, err = client.Do(req) //nolint:bodyclose // the redirect is refused, so there is no body
	require.ErrorIs(t, err, ErrOffEndpointOrigin)

	require.Len(t, base.hops, 1, "the downgraded hop must never be sent")
	assert.Equal(t, "Bearer "+adcAccessToken, base.hops[0].Header.Get("Authorization"))
}

// Userinfo in the authority dials the part after the "@" while reading as the
// part before it, so a reviewed config line can point the credential somewhere
// else in plain sight.
func TestNewRESTConfig_RejectsAnEndpointCarryingCredentials(t *testing.T) {
	t.Parallel()

	for name, endpoint := range map[string]string{
		"username only":         "https://real-endpoint.example.goog@elsewhere.example",
		"username and password": "https://user:hunter2@elsewhere.example",
		// url.Redacted masks a password and nothing else, so a token in the
		// username has to be caught before any formatting, on any scheme.
		"token as the username, on a scheme that is also wrong": "http://ya29.a0AfB-secret@elsewhere.example",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := newRESTConfig(t.Context(), endpoint)
			require.ErrorIs(t, err, ErrEndpointHasCredentials)
			assert.NotContains(t, err.Error(), "hunter2")
			assert.NotContains(t, err.Error(), "ya29.a0AfB-secret")
		})
	}
}

// A path is how an API server behind a reverse proxy is addressed; client-go
// keeps it as a request prefix, so validation must not reject it along with
// the authority forms that are unsafe.
//
//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_AcceptsAPathPrefix(t *testing.T) {
	_ = applicationDefaultCredentials(t)

	config, err := newRESTConfig(t.Context(), testAPIEndpoint+"/k8s-api")
	require.NoError(t, err)
	assert.Equal(t, testAPIEndpoint+"/k8s-api", config.Host)
}

//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_KeepsTheCredentialAcrossAuthoritySpellings(t *testing.T) {
	_ = applicationDefaultCredentials(t)

	for name, target := range map[string]string{
		"explicit default port": "https://dns-endpoint.example:443/normalized",
		"uppercased host":       "https://DNS-Endpoint.Example/normalized",
		"trailing root dot":     "https://dns-endpoint.example./normalized",
	} {
		t.Run(name, func(t *testing.T) {
			config, err := newRESTConfig(t.Context(), testAPIEndpoint)
			require.NoError(t, err)

			base := &redirectOnceTransport{to: target}
			client := &http.Client{Transport: config.WrapTransport(base)}

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, testAPIEndpoint+"/api/v1/pods", nil)
			require.NoError(t, err)

			resp, err := client.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			require.Len(t, base.hops, 2)
			assert.Equal(t, "Bearer "+adcAccessToken, base.hops[1].Header.Get("Authorization"),
				"the same authority spelled differently is still the same authority")
		})
	}
}

// The rejected endpoint is formatted into a startup-fatal error that reaches
// the logs, so nothing that could carry a secret may be echoed back.
func TestNewRESTConfig_DoesNotEchoTheQueryStringWhenRejecting(t *testing.T) {
	t.Parallel()

	_, err := newRESTConfig(t.Context(), "http://elsewhere.example/?token=s3cr3t")
	require.ErrorIs(t, err, ErrEndpointNotHTTPS)
	assert.NotContains(t, err.Error(), "s3cr3t")
}

func TestNewRESTConfig_RejectsAnUnparseableEndpointWithoutEchoingIt(t *testing.T) {
	t.Parallel()

	for name, endpoint := range map[string]string{
		"token in the port slot":  "https://ya29:a0AfB-secret-token",
		"token as a bracket host": "https://[ya29.a0AfB-secret-token]",
		"control character":       "https://dns-endpoint.example/\x7f",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := newRESTConfig(t.Context(), endpoint)
			require.ErrorIs(t, err, ErrEndpointUnparseable)
			assert.NotContains(t, err.Error(), "a0AfB-secret-token")
		})
	}
}

// Validation must reject the hosts the credential binding would reduce to
// nothing, or the two disagree: "." passes a Hostname() check and canonicalises
// to an empty host, which then matches a host-less redirect target.
func TestNewRESTConfig_RejectsAnAuthorityThatCanonicalisesToNoHost(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{"https://.", "https://.:6443", "https://:6443"} {
		_, err := newRESTConfig(t.Context(), endpoint)
		require.ErrorIs(t, err, ErrEndpointNotHTTPS, "endpoint %q must be rejected", endpoint)
	}
}

// net/http resolves a host through IDNA, not case folding, and the two
// disagree: strings.ToLower maps U+0130 to "i" while IDNA maps it to a
// different registrable domain. Pinning the folded name would send the
// project-scoped token to the domain the socket actually reaches.
//
//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_PinsTheNameThatWillBeDialled(t *testing.T) {
	_ = applicationDefaultCredentials(t)

	config, err := newRESTConfig(t.Context(), testAPIEndpoint)
	require.NoError(t, err)

	base := &redirectOnceTransport{to: "https://dns-endpo\u0130nt.example/api/v1/pods"}
	client := &http.Client{Transport: config.WrapTransport(base)}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, testAPIEndpoint+"/api/v1/pods", nil)
	require.NoError(t, err)

	_, err = client.Do(req) //nolint:bodyclose // the hop is refused, so there is no body
	require.ErrorIs(t, err, ErrOffEndpointOrigin)
	require.Len(t, base.hops, 1, "the homograph hop must never be sent")
}

// An endpoint whose host is not a resolvable domain name is rejected rather
// than pinned to something no redirect can match.
func TestNewRESTConfig_RejectsAHostThatIsNotADomainName(t *testing.T) {
	t.Parallel()

	_, err := newRESTConfig(t.Context(), "https://dns-endpoint\u0000.example")
	require.Error(t, err)
}

// oauth2.Transport mints with no request context, so neither the caller's
// deadline nor rest.Config.Timeout reaches the exchange. Unbounded, a silent
// token endpoint stalls the discovery refresh forever while the caching
// decorator keeps serving its last set with a nil error.
//
//nolint:paralleltest // cannot call t.Setenv and t.Parallel
func TestNewRESTConfig_BoundsTheCredentialExchange(t *testing.T) {
	blocked := make(chan struct{})
	silent := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-blocked
	}))
	t.Cleanup(func() { close(blocked); silent.Close() })
	credentialsAt(t, silent.URL)

	config, err := newRESTConfig(t.Context(), testAPIEndpoint)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, testAPIEndpoint+"/api/v1/pods", nil)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		//nolint:bodyclose // the mint fails, so no response exists
		_, roundTripErr := config.WrapTransport(&recordingTransport{}).RoundTrip(req)
		done <- roundTripErr
	}()

	select {
	case roundTripErr := <-done:
		require.Error(t, roundTripErr, "a silent token endpoint must fail the request, not satisfy it")
	case <-time.After(tokenMintTimeout + 20*time.Second):
		require.Fail(t, "the credential exchange is unbounded: RoundTrip never returned")
	}
}
