package jwks

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// defaultCacheDuration is the default JWKS cache duration applied
	// when a Config does not specify one.
	defaultCacheDuration = 5 * time.Minute
	// defaultDiscoveryPath is the relative path appended to the issuer URL
	// to derive the discovery URL when one is not explicitly configured.
	defaultDiscoveryPath = "/.well-known/openid-configuration"
	defaultJWKSPath      = "/.well-known/jwks.json"
)

// Config describes a single OIDC issuer.
type Config struct {
	Issuer            Issuer        `json:"issuer"`
	CacheDuration     time.Duration `json:"cacheDuration"`
	RequiredAtStartup *bool         `json:"requiredAtStartup"`
}

// Issuer describes an OIDC issuer endpoint plus audience policy.
type Issuer struct {
	URL                 string              `json:"url"`
	DiscoveryURL        string              `json:"discoveryURL"`
	Audiences           []string            `json:"audiences"`
	AudienceMatchPolicy AudienceMatchPolicy `json:"audienceMatchPolicy"`
}

// UnmarshalJSON parses `cacheDuration` from a Go duration string.
func (e *Config) UnmarshalJSON(data []byte) error {
	type entryJSON struct {
		Issuer            Issuer `json:"issuer"`
		CacheDuration     string `json:"cacheDuration"`
		RequiredAtStartup *bool  `json:"requiredAtStartup"`
	}

	var raw entryJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	e.Issuer = raw.Issuer
	e.RequiredAtStartup = raw.RequiredAtStartup

	if raw.CacheDuration != "" {
		duration, err := time.ParseDuration(raw.CacheDuration)
		if err != nil {
			return fmt.Errorf("parse cacheDuration: %w", err)
		}

		e.CacheDuration = duration
	}

	return nil
}

// Normalized returns a copy with defaults applied.
func (e *Config) Normalized() Config {
	out := *e
	out.Issuer.URL = strings.TrimSpace(out.Issuer.URL)
	out.Issuer.DiscoveryURL = strings.TrimSpace(out.Issuer.DiscoveryURL)

	if out.CacheDuration <= 0 {
		out.CacheDuration = defaultCacheDuration
	}

	return out
}

// Validate returns an error if the config contains invalid configuration.
//
// All issues found are joined into a single error to surface as much useful
// feedback as possible in one pass.
func (e *Config) Validate() error {
	var errs []error

	errs = append(errs, validateIssuerURL(e.Issuer.URL)...)
	errs = append(errs, validateDiscoveryURL(e.Issuer.URL, e.Issuer.DiscoveryURL)...)

	if err := validateAudienceMatchPolicy(e.Issuer.AudienceMatchPolicy, e.Issuer.Audiences); err != nil {
		errs = append(errs, fmt.Errorf("issuer: %w", err))
	}

	return errors.Join(errs...)
}

// discoveryURL returns the configured discoveryURL or the default derived
// from the issuer URL.
func (e *Config) discoveryURL() string {
	if e.Issuer.DiscoveryURL != "" {
		return e.Issuer.DiscoveryURL
	}

	return strings.TrimRight(e.Issuer.URL, "/") + defaultDiscoveryPath
}

func (e *Config) requiredAtStartup() bool {
	return e.RequiredAtStartup == nil || *e.RequiredAtStartup
}
