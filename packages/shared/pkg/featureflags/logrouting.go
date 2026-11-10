package featureflags

import (
	"context"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// LogsWriteConfigFlag controls where sandbox/external logs are written, so
// operators can retarget log destinations from LaunchDarkly without a redeploy.
//
// Shape (every key optional):
//
//	{
//	  "mode": "primary_only" | "primary_and_shadow",
//	  "primary_url": "http://localhost:30006",
//	  "shadow_urls": ["http://localhost:4321/logs"],
//	  "timeout_ms": 2000,
//	  "max_inflight_shadow_writes": 1024
//	}
//
// A key present in the flag overrides the deployment default for that field;
// an absent (or null) key falls through to it:
//
//	mode                        LOGS_WRITE_MODE, else "primary_only"
//	primary_url                 LOGS_COLLECTOR_ADDRESS
//	shadow_urls                 LOGS_WRITE_SHADOW_URLS (comma-separated)
//	timeout_ms                  LOGS_WRITE_TIMEOUT_MS, else 2000
//	max_inflight_shadow_writes  LOGS_WRITE_MAX_INFLIGHT_SHADOW_WRITES, else 1024
//
// Semantics:
//   - null/missing/non-object -> the deployment defaults alone.
//   - "primary_only"        -> write to the primary URL only.
//   - "primary_and_shadow"  -> write to the primary URL; fire-and-forget the
//     shadow URLs (shadow failures never affect the primary result).
//   - An empty primary_url falls through like an absent one.
//   - timeout_ms or max_inflight_shadow_writes <= 0 falls through; a timeout
//     above the maximum is clamped.
//   - A present but invalid value (unknown mode, a non-string or unsafe URL,
//     shadow_urls that is not an array of <= maxLogWriteShadowURLs strings)
//     invalidates the whole flag -> the deployment defaults alone.
//   - URLs from the flag must be http URLs pointing at local/private hosts or
//     allowed internal DNS suffixes. URLs from the environment are operator
//     configuration and are not checked.
//
// With the flag null and LOGS_WRITE_TIMEOUT_MS unset the timeout is 0: writes
// rely on the HTTP client's own timeout, as they did before this flag existed.
//
// The defaults are runtime env values the flag cannot know, so the default is
// Null() and the resolver substitutes them (see LogWriteDefaultsFromEnv).
var LogsWriteConfigFlag = NewJSONFlag("logs-write-config", ldvalue.Null())

// LogsReadConfigFlag selects the backend used to read sandbox/build logs.
// false reads from Loki (unchanged behavior); true reads from the ClickHouse
// sandbox_logs table. The fallback comes from LOGS_READ_CONFIG and defaults to
// false, so a deployment without LaunchDarkly, or one whose LaunchDarkly has
// no value for the flag, reads where the variable says; a LaunchDarkly value
// wins when there is one.
var LogsReadConfigFlag = NewBoolFlag("logs-read-config", logsReadConfigFallback())

// logsReadConfigFallback reads LOGS_READ_CONFIG. Unset or unparseable means
// false, the way envdTimeoutFallbackMs treats ENVD_TIMEOUT.
func logsReadConfigFallback() bool {
	return parseLogsReadConfig(os.Getenv("LOGS_READ_CONFIG"))
}

func parseLogsReadConfig(raw string) bool {
	value, err := strconv.ParseBool(strings.TrimSpace(raw))

	return err == nil && value
}

// Log write routing modes for LogsWriteConfigFlag.
const (
	LogsWriteModePrimaryOnly      = "primary_only"
	LogsWriteModePrimaryAndShadow = "primary_and_shadow"
)

const (
	// defaultLogWriteTimeout is used when timeout_ms is missing/invalid.
	defaultLogWriteTimeout = 2000 * time.Millisecond
	// maxLogWriteTimeout caps operator-provided timeouts.
	maxLogWriteTimeout = 10000 * time.Millisecond
	// maxLogWriteShadowURLs caps fanout configured from LaunchDarkly.
	maxLogWriteShadowURLs = 4
	// defaultMaxInflightShadowWrites is used when max_inflight_shadow_writes is missing/invalid.
	defaultMaxInflightShadowWrites = 1024
)

var (
	logRoutingMeter                = otel.Meter("github.com/e2b-dev/infra/packages/shared/pkg/featureflags")
	logWriteConfigResolutionMetric = mustLogRoutingCounter(
		"log_write_config_resolution_count",
		"Number of logs-write-config resolutions by outcome and fallback reason",
	)
)

func mustLogRoutingCounter(name, description string) metric.Int64Counter {
	counter, err := logRoutingMeter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

// LogWriteConfig is the resolved, validated log write routing configuration.
type LogWriteConfig struct {
	// PrimaryURL is the synchronous, success-controlling destination.
	PrimaryURL string
	// ShadowURLs are best-effort, fire-and-forget destinations.
	ShadowURLs []string
	// Timeout bounds each individual log write request.
	Timeout time.Duration
	// MaxInflightShadowWrites bounds concurrent best-effort shadow writes.
	MaxInflightShadowWrites int64
}

// LogWriteDefaults is the deployment's log write routing. Each field applies
// when LogsWriteConfigFlag leaves the matching key out; a zero field is unset.
type LogWriteDefaults struct {
	Mode                    string
	PrimaryURL              string
	ShadowURLs              []string
	Timeout                 time.Duration
	MaxInflightShadowWrites int64
}

// LogWriteDefaultsFromEnv reads the LOGS_WRITE_* variables. primaryURL is the
// collector address (LOGS_COLLECTOR_ADDRESS). Unparseable or non-positive
// values are ignored, leaving that field unset.
func LogWriteDefaultsFromEnv(primaryURL string) LogWriteDefaults {
	return parseLogWriteDefaults(primaryURL, os.Getenv)
}

func parseLogWriteDefaults(primaryURL string, getenv func(string) string) LogWriteDefaults {
	defaults := LogWriteDefaults{PrimaryURL: strings.TrimSpace(primaryURL)}

	if mode := strings.TrimSpace(getenv("LOGS_WRITE_MODE")); isLogWriteMode(mode) {
		defaults.Mode = mode
	}

	for u := range strings.SplitSeq(getenv("LOGS_WRITE_SHADOW_URLS"), ",") {
		if u = strings.TrimSpace(u); u != "" {
			defaults.ShadowURLs = append(defaults.ShadowURLs, u)
		}
	}

	if ms, err := strconv.Atoi(strings.TrimSpace(getenv("LOGS_WRITE_TIMEOUT_MS"))); err == nil && ms > 0 {
		defaults.Timeout = cappedLogWriteTimeout(ms)
	}

	if n, err := strconv.ParseInt(strings.TrimSpace(getenv("LOGS_WRITE_MAX_INFLIGHT_SHADOW_WRITES")), 10, 64); err == nil && n > 0 {
		defaults.MaxInflightShadowWrites = n
	}

	return defaults
}

func (d LogWriteDefaults) mode() string {
	if d.Mode == "" {
		return LogsWriteModePrimaryOnly
	}

	return d.Mode
}

func (d LogWriteDefaults) maxInflightShadowWrites() int64 {
	if d.MaxInflightShadowWrites <= 0 {
		return defaultMaxInflightShadowWrites
	}

	return d.MaxInflightShadowWrites
}

// config is the routing used when the flag is null or invalid. Its Timeout
// stays 0 unless LOGS_WRITE_TIMEOUT_MS is set, so callers skip the
// per-request WithTimeout and rely on the HTTP client's own timeout, exactly as
// before this flag existed. defaultLogWriteTimeout only applies once the flag
// is configured.
func (d LogWriteDefaults) config() LogWriteConfig {
	primary := strings.TrimSpace(d.PrimaryURL)

	cfg := LogWriteConfig{
		PrimaryURL:              primary,
		Timeout:                 d.Timeout,
		MaxInflightShadowWrites: d.maxInflightShadowWrites(),
	}
	if d.mode() == LogsWriteModePrimaryAndShadow {
		cfg.ShadowURLs = dedupeShadowURLs(primary, d.ShadowURLs)
	}

	return cfg
}

func isLogWriteMode(mode string) bool {
	return mode == LogsWriteModePrimaryOnly || mode == LogsWriteModePrimaryAndShadow
}

// ResolveLogWriteConfig reads LogsWriteConfigFlag and merges it over defaults:
// each key the flag sets wins, each key it leaves out takes the default. On a
// missing/malformed/unsafe flag it returns the defaults alone.
func ResolveLogWriteConfig(ctx context.Context, ff *Client, defaults LogWriteDefaults, contexts ...ldcontext.Context) LogWriteConfig {
	fallback := defaults.config()

	if ff == nil {
		recordLogWriteConfigResolution(ctx, "legacy", "nil_client", "")

		return fallback
	}

	value := ff.JSONFlag(ctx, LogsWriteConfigFlag, contexts...)
	if value.IsNull() {
		recordLogWriteConfigResolution(ctx, "legacy", "null", "")

		return fallback
	}
	if value.Type() != ldvalue.ObjectType {
		recordLogWriteConfigResolution(ctx, "legacy", "non_object", "")

		return fallback
	}

	mode := defaults.mode()
	if modeValue := value.GetByKey("mode"); !modeValue.IsNull() {
		if modeValue.Type() != ldvalue.StringType {
			recordLogWriteConfigResolution(ctx, "legacy", "mode_not_string", "")

			return fallback
		}
		mode = strings.TrimSpace(modeValue.StringValue())
		if !isLogWriteMode(mode) {
			recordLogWriteConfigResolution(ctx, "legacy", "unknown_mode", mode)

			return fallback
		}
	}

	primary := strings.TrimSpace(defaults.PrimaryURL)
	if primaryValue := value.GetByKey("primary_url"); !primaryValue.IsNull() {
		if primaryValue.Type() != ldvalue.StringType {
			recordLogWriteConfigResolution(ctx, "legacy", "primary_not_string", mode)

			return fallback
		}
		if u := strings.TrimSpace(primaryValue.StringValue()); u != "" {
			if !isSafeLogURL(u) {
				recordLogWriteConfigResolution(ctx, "legacy", "unsafe_primary", mode)

				return fallback
			}
			primary = u
		}
	}
	if primary == "" {
		recordLogWriteConfigResolution(ctx, "legacy", "no_primary", mode)

		return fallback
	}

	var shadows []string
	if mode == LogsWriteModePrimaryAndShadow {
		candidates := defaults.ShadowURLs
		if raw := value.GetByKey("shadow_urls"); !raw.IsNull() {
			var reason string
			candidates, reason = flagShadowURLs(raw)
			if reason != "" {
				recordLogWriteConfigResolution(ctx, "legacy", reason, mode)

				return fallback
			}
		}
		shadows = dedupeShadowURLs(primary, candidates)
	}

	recordLogWriteConfigResolution(ctx, "configured", "", mode)

	return LogWriteConfig{
		PrimaryURL:              primary,
		ShadowURLs:              shadows,
		Timeout:                 logWriteTimeout(value, defaults.Timeout),
		MaxInflightShadowWrites: maxInflightShadowWrites(value, defaults.maxInflightShadowWrites()),
	}
}

// flagShadowURLs validates shadow_urls from the flag. A non-empty reason means
// the value is invalid. An unsafe shadow URL invalidates the whole config:
// fail safe to the defaults rather than silently exfiltrating to an external
// host.
func flagShadowURLs(raw ldvalue.Value) ([]string, string) {
	if raw.Type() != ldvalue.ArrayType {
		return nil, "shadow_not_array"
	}
	if raw.Count() > maxLogWriteShadowURLs {
		return nil, "too_many_shadows"
	}

	urls := make([]string, 0, raw.Count())
	for i := range raw.Count() {
		item := raw.GetByIndex(i)
		if item.Type() != ldvalue.StringType {
			return nil, "shadow_not_string"
		}
		u := strings.TrimSpace(item.StringValue())
		if !isSafeLogURL(u) {
			return nil, "unsafe_shadow"
		}
		urls = append(urls, u)
	}

	return urls, ""
}

// dedupeShadowURLs drops shadows that repeat the primary or each other.
func dedupeShadowURLs(primary string, candidates []string) []string {
	var shadows []string
	seen := map[string]struct{}{primary: {}}
	for _, u := range candidates {
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		shadows = append(shadows, u)
	}

	return shadows
}

func recordLogWriteConfigResolution(ctx context.Context, outcome, reason, mode string) {
	if logWriteConfigResolutionMetric == nil {
		return
	}

	logWriteConfigResolutionMetric.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
		attribute.String("mode", mode),
	))
}

// logWriteTimeout reads timeout_ms and clamps it to a safe range. A missing or
// non-positive value falls through to fallback, then defaultLogWriteTimeout.
func logWriteTimeout(value ldvalue.Value, fallback time.Duration) time.Duration {
	ms := value.GetByKey("timeout_ms").IntValue()
	if ms <= 0 {
		if fallback > 0 {
			return fallback
		}

		return defaultLogWriteTimeout
	}

	return cappedLogWriteTimeout(ms)
}

// cappedLogWriteTimeout converts a positive millisecond count to a duration,
// capping it before the conversion so a huge value cannot overflow.
func cappedLogWriteTimeout(ms int) time.Duration {
	return time.Duration(min(ms, int(maxLogWriteTimeout/time.Millisecond))) * time.Millisecond
}

// maxInflightShadowWrites reads max_inflight_shadow_writes. A missing or
// non-positive value falls through to fallback.
func maxInflightShadowWrites(value ldvalue.Value, fallback int64) int64 {
	maxInflight := value.GetByKey("max_inflight_shadow_writes").IntValue()
	if maxInflight <= 0 {
		return fallback
	}

	return int64(maxInflight)
}

var allowedLogHostSuffixes = []string{
	".service.consul",
	".consul",
	".svc.cluster.local",
	".svc",
	".local",
	".internal",
}

// isSafeLogURL allows only http URLs pointing at loopback, link-local, private
// IPs, or internal service-discovery DNS suffixes. This keeps log routing on
// local/private infrastructure and prevents exfiltration to arbitrary external
// endpoints via the flag.
func isSafeLogURL(raw string) bool {
	if raw == "" {
		return false
	}

	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return false
	}

	host := u.Hostname()
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	}

	hostLower := strings.ToLower(host)
	for _, suffix := range allowedLogHostSuffixes {
		if strings.HasSuffix(hostLower, suffix) {
			return true
		}
	}

	ip := net.ParseIP(host)
	if ip == nil {
		// Non-IP host without an explicitly allowed internal suffix -> reject.
		return false
	}

	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
}
