package jwks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// httpTimeout is the timeout used for discovery and JWKS HTTP requests.
const httpTimeout = 10 * time.Second

// optionalRefreshInterval is how often an issuer that is allowed to start
// without keys retries the JWKS fetch. The configured cache duration would
// leave peer tokens rejected for that whole interval after the peer is up.
const optionalRefreshInterval = 2 * time.Second

var errNoJWKSKeys = errors.New("JWKS contains no supported keys")

// Option customizes a Verifier beyond the issuer configuration.
type Option func(*Verifier)

// WithParserOptions appends jwt parser options to the verifier's defaults.
func WithParserOptions(options ...jwt.ParserOption) Option {
	return func(v *Verifier) {
		v.parserOptions = append(v.parserOptions, options...)
	}
}

// Verifier verifies JWTs against the JWKS of a single OIDC issuer.
type Verifier struct {
	keyfunc       keyfunc.Keyfunc
	storage       jwkset.Storage
	audiences     []string
	parserOptions []jwt.ParserOption
}

// discoveryDocument is a minimal subset of the OIDC discovery document
// (https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata).
type discoveryDocument struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

// NewVerifier constructs a Verifier from the supplied Config. OIDC discovery
// is fetched synchronously and fails on configuration or network errors. The
// JWKS fetch fails startup unless RequiredAtStartup is false.
func NewVerifier(ctx context.Context, entry Config, httpClient *http.Client, options ...Option) (*Verifier, error) {
	entry, err := validateConfig(entry, httpClient)
	if err != nil {
		return nil, err
	}

	discoveryURL := entry.discoveryURL()
	if err := validateHTTPSURL(discoveryURL, "discoveryURL"); err != nil {
		return nil, err
	}

	doc, err := fetchDiscoveryDocument(ctx, httpClient, discoveryURL)
	if err != nil {
		return nil, fmt.Errorf("fetch OIDC discovery document at %s: %w", discoveryURL, err)
	}

	if doc.Issuer != entry.Issuer.URL {
		return nil, fmt.Errorf("discovery document issuer %q does not match configured issuer %q", doc.Issuer, entry.Issuer.URL)
	}

	if err := validateHTTPSURL(doc.JWKSURI, "discovery jwks_uri"); err != nil {
		return nil, err
	}

	return newVerifier(ctx, entry, doc.JWKSURI, httpClient, options...)
}

func NewVerifierFromIssuerJWKS(ctx context.Context, entry Config, httpClient *http.Client, options ...Option) (*Verifier, error) {
	entry.Issuer.DiscoveryURL = ""
	entry, err := validateConfig(entry, httpClient)
	if err != nil {
		return nil, err
	}

	jwksURL := strings.TrimRight(entry.Issuer.URL, "/") + defaultJWKSPath
	if err := validateHTTPSURL(jwksURL, "jwksURL"); err != nil {
		return nil, err
	}

	return newVerifier(ctx, entry, jwksURL, httpClient, options...)
}

func validateConfig(entry Config, httpClient *http.Client) (Config, error) {
	if httpClient == nil {
		return Config{}, errors.New("JWKS HTTP client is required")
	}

	entry = entry.Normalized()
	if err := entry.Validate(); err != nil {
		return Config{}, err
	}

	return entry, nil
}

func newVerifier(ctx context.Context, entry Config, jwksURL string, httpClient *http.Client, options ...Option) (*Verifier, error) {
	refreshInterval := entry.CacheDuration
	if !entry.requiredAtStartup() {
		refreshInterval = optionalRefreshInterval
	}
	storage, err := jwkset.NewStorageFromHTTP(jwksURL, jwkset.HTTPClientStorageOptions{
		Client:                    httpClient,
		Ctx:                       ctx,
		HTTPTimeout:               httpTimeout,
		RefreshInterval:           refreshInterval,
		NoErrorReturnFirstHTTPReq: !entry.requiredAtStartup(),
	})
	if err != nil {
		return nil, fmt.Errorf("create JWKS storage: %w", err)
	}

	if _, err := validMethodsFromStorage(ctx, storage); err != nil {
		if entry.requiredAtStartup() || !errors.Is(err, errNoJWKSKeys) {
			return nil, fmt.Errorf("validate JWKS signing algorithms: %w", err)
		}
	}

	keyFunc, err := keyfunc.New(keyfunc.Options{
		Ctx:     ctx,
		Storage: storage,
	})
	if err != nil {
		return nil, fmt.Errorf("create JWKS keyfunc: %w", err)
	}

	parserOptions := []jwt.ParserOption{
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(entry.Issuer.URL),
	}

	verifier := &Verifier{
		keyfunc:       keyFunc,
		storage:       storage,
		audiences:     entry.Issuer.Audiences,
		parserOptions: parserOptions,
	}
	for _, option := range options {
		option(verifier)
	}

	return verifier, nil
}

func validMethodsFromStorage(ctx context.Context, storage jwkset.Storage) ([]string, error) {
	keys, err := storage.KeyReadAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("read JWKS keys: %w", err)
	}
	if len(keys) == 0 {
		return nil, errNoJWKSKeys
	}

	methods := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		metadata := key.Marshal()
		algorithm := metadata.ALG.String()
		if algorithm == "" {
			return nil, fmt.Errorf("JWKS key %q is missing alg", metadata.KID)
		}
		if _, ok := seen[algorithm]; ok {
			continue
		}

		method := jwt.GetSigningMethod(algorithm)
		switch method.(type) {
		case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS, *jwt.SigningMethodECDSA, *jwt.SigningMethodEd25519:
		default:
			return nil, fmt.Errorf("JWKS key %q uses unsupported signing algorithm %q", metadata.KID, algorithm)
		}

		seen[algorithm] = struct{}{}
		methods = append(methods, algorithm)
	}

	return methods, nil
}

// Verify parses and validates the supplied token string and returns its
// claims.
func (v *Verifier) Verify(ctx context.Context, tokenString string) (jwt.MapClaims, error) {
	if v == nil || v.keyfunc == nil || v.storage == nil {
		return nil, errors.New("JWKS verifier is not configured")
	}

	validMethods, err := validMethodsFromStorage(ctx, v.storage)
	if err != nil {
		return nil, fmt.Errorf("validate JWKS signing algorithms: %w", err)
	}
	parserOptions := append(slices.Clone(v.parserOptions), jwt.WithValidMethods(validMethods))

	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
		return v.keyfunc.KeyfuncCtx(ctx)(token)
	}, parserOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to verify token: %w", err)
	}
	if !token.Valid {
		return nil, errors.New("token is invalid")
	}

	if err := validateAudience(claims, v.audiences); err != nil {
		return nil, fmt.Errorf("failed to verify token: %w", err)
	}

	return claims, nil
}

func fetchDiscoveryDocument(ctx context.Context, httpClient *http.Client, discoveryURL string) (*discoveryDocument, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create discovery request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute discovery request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))

		return nil, fmt.Errorf("discovery endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var doc discoveryDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode discovery document: %w", err)
	}

	if doc.Issuer == "" {
		return nil, errors.New("discovery document is missing issuer")
	}

	if doc.JWKSURI == "" {
		return nil, errors.New("discovery document is missing jwks_uri")
	}

	return &doc, nil
}

// validateHTTPSURL applies the same checks as validateURL but returns at the
// first failure. It is intended for runtime validation of URLs derived from
// the OIDC discovery document where one error is enough to fail fast.
func validateHTTPSURL(rawURL string, field string) error {
	if errs := validateURL(rawURL, field); len(errs) > 0 {
		return errs[0]
	}

	return nil
}

// validateURL enforces the same constraints Kubernetes requires of issuer
// URLs:
//   - parseable via url.Parse
//   - https scheme (with one exception, see isLoopbackHost below)
//   - no userinfo (username/password)
//   - no query string
//   - no fragment
//
// Loopback exception: an http:// URL is accepted when its host resolves to
// a loopback address (`localhost`, `127.0.0.1`, `[::1]`). Any non-loopback host
// still requires https.
//
// The field name is included in error messages to help operators locate the
// offending value. All applicable errors are returned together.
func validateURL(rawURL string, field string) []error {
	var errs []error

	u, err := url.Parse(rawURL)
	if err != nil {
		return []error{fmt.Errorf("invalid %s: %w", field, err)}
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !isLoopbackHost(u.Hostname())) {
		errs = append(errs, fmt.Errorf("invalid %s scheme %q (must be https, or http for loopback hosts)", field, u.Scheme))
	}
	if u.User != nil {
		errs = append(errs, fmt.Errorf("invalid %s: must not contain a username or password", field))
	}
	if len(u.RawQuery) > 0 {
		errs = append(errs, fmt.Errorf("invalid %s: must not contain a query", field))
	}
	if len(u.Fragment) > 0 {
		errs = append(errs, fmt.Errorf("invalid %s: must not contain a fragment", field))
	}

	return errs
}

// isLoopbackHost reports whether the given URL host is a loopback address.
// It accepts:
//   - the literal name "localhost" (case-insensitive)
//   - any IPv4 address in 127.0.0.0/8
//   - the IPv6 loopback ::1 (Hostname() strips the brackets, so we receive "::1")
//
// We deliberately *do not* resolve the name via DNS: that would make
// validation depend on host resolver state and turn this into a TOCTOU
// surface. Matching string-and-literal-IP only is enough for the local-dev
// use case and keeps the check pure.
func isLoopbackHost(host string) bool {
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

// validateIssuerURL enforces presence + URL constraints on the issuer URL.
func validateIssuerURL(issuerURL string) []error {
	if issuerURL == "" {
		return []error{errors.New("issuer.url is required")}
	}

	return validateURL(issuerURL, "issuer.url")
}

// validateDiscoveryURL enforces URL constraints on the optional discovery
// URL plus the requirement that it differ from the issuer URL when set.
func validateDiscoveryURL(issuerURL, discoveryURL string) []error {
	if discoveryURL == "" {
		return nil
	}

	var errs []error
	if issuerURL != "" && strings.TrimRight(issuerURL, "/") == strings.TrimRight(discoveryURL, "/") {
		errs = append(errs, errors.New("issuer.discoveryURL must be different from issuer.url"))
	}
	errs = append(errs, validateURL(discoveryURL, "issuer.discoveryURL")...)

	return errs
}
