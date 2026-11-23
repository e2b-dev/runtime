package featureflags

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	ldclient "github.com/launchdarkly/go-server-sdk/v7"
	"github.com/launchdarkly/go-server-sdk/v7/ldcomponents"
)

const projectedTokenEnv = "LAUNCH_DARKLY_TOKEN_FILE"

type projectedTransport struct {
	accepted atomic.Bool
	file     string
	host     string
	base     http.RoundTripper
}

func (t *projectedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" || req.URL.Host != t.host {
		return nil, errors.New("flag gateway origin mismatch")
	}
	_, err := os.Stat(filepath.Join(filepath.Dir(t.file), "registered"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("flag gateway registration is pending")
	}
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(t.file)
	if err != nil || len(strings.TrimSpace(string(raw))) == 0 {
		return nil, errors.New("flag gateway credential is unavailable")
	}
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(raw)))

	res, err := t.base.RoundTrip(out)
	if err != nil {
		return nil, err
	}
	switch {
	case res.StatusCode < http.StatusMultipleChoices:
		t.accepted.Store(true)
	case t.accepted.Load() && (res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden):
		_ = res.Body.Close()

		return nil, errors.New("flag gateway rejected the projected credential")
	}

	return res, nil
}

func configureProjected(cfg *ldclient.Config, path string) (string, error) {
	oauth := oauthConfigFromEnv()
	if oauth.enabled() || launchDarklyApiKey != "" {
		return "", errors.New("projected flag credentials require a single authentication mode")
	}
	if err := validateOAuthURL(baseURLEnv, oauth.BaseURL); err != nil {
		return "", err
	}
	origin, err := url.Parse(oauth.BaseURL)
	if err != nil {
		return "", err
	}
	transport := &projectedTransport{file: path, host: origin.Host, base: http.DefaultTransport}
	cfg.HTTP = ldcomponents.HTTPConfiguration().HTTPClientFactory(func() *http.Client {
		return &http.Client{Transport: transport, Timeout: ldcomponents.DefaultConnectTimeout, CheckRedirect: rejectOAuthRedirect}
	})

	return "projected", nil
}
