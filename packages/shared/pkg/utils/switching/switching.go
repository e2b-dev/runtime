// Package switching provides a generic, LaunchDarkly-gated resource switcher.
//
// A Switcher holds one default Resource and N alternates, all built up-front
// from DSNs via a caller-supplied factory. Each call to Resolve reads a string
// flag and routes to the default (empty value) or one of the alternates
// (numeric index "0", "1", ...). Invalid values or out-of-range / unavailable
// indexes fall back to the default and log a rate-limited warning, so a
// misconfigured flag never takes traffic down.
//
// Intended for shifting read traffic between endpoints per-query without
// restarting the service.
package switching

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Resource represents a component that can be gracefully closed.
type Resource interface {
	Close(ctx context.Context) error
}

// Factory builds a Resource from a DSN.
type Factory[T Resource] func(dsn string) (T, error)

type Option[T Resource] func(*options[T])

type options[T Resource] struct {
	allowNoopDefault bool
	defaultClient    T
	defaultClientSet bool
	factory          Factory[T]
	noopFactory      func() (T, error)
	warnCap          int64
	meter            metric.Meter
}

// WithAllowNoopDefault enables falling back to a noop client if the default DSN
// is empty. Requires WithNoopFactory to be set.
func WithAllowNoopDefault[T Resource](allow bool) Option[T] {
	return func(o *options[T]) {
		o.allowNoopDefault = allow
	}
}

// WithNoopFactory provides a constructor for a noop client used when the
// default DSN is empty and noop is allowed.
func WithNoopFactory[T Resource](factory func() (T, error)) Option[T] {
	return func(o *options[T]) {
		o.noopFactory = factory
	}
}

// WithDefaultClient provides an explicitly constructed default client.
// Note: Passing a client here transfers ownership to the Switcher; the
// switcher will close this client when Switcher.Close() is called.
func WithDefaultClient[T Resource](defaultClient T) Option[T] {
	return func(o *options[T]) {
		o.defaultClient = defaultClient
		o.defaultClientSet = true
	}
}

// WithWarnCap sets the maximum number of unique invalid flag value warnings
// logged before suppression. Defaults to 16.
func WithWarnCap[T Resource](warnCap int64) Option[T] {
	return func(o *options[T]) {
		o.warnCap = warnCap
	}
}

// WithMeter enables observability metrics for the switcher.
func WithMeter[T Resource](meter metric.Meter) Option[T] {
	return func(o *options[T]) {
		o.meter = meter
	}
}

type Switcher[T Resource] struct {
	defaultClient T
	alternates    []T
	// alternateNonNil caches the results of utils.IsNil(alternates[i]) to avoid
	// reflection on the hot path (Resolve).
	alternateNonNil []bool
	closed          atomic.Bool
	ff              *featureflags.Client
	flag            featureflags.StringFlag
	warnSeen        sync.Map
	warnCount       atomic.Int64
	warnCap         int64

	resolveCounter metric.Int64Counter
	// flagKeyAttr is pre-computed to avoid per-call allocations in Resolve.
	flagKeyAttr attribute.KeyValue
}

// New creates a new Switcher that routes queries between a default client
// and a list of alternates based on a LaunchDarkly string flag.
//
// The flag value "" (empty) selects the default client.
// Numeric values "0", "1", ... select the corresponding index in alternateDSNs.
// Any other value or an out-of-range index falls back to the default client and
// logs a rate-limited warning.
func New[T Resource](
	ctx context.Context,
	ff *featureflags.Client,
	flag featureflags.StringFlag,
	defaultDSN string,
	alternateDSNs []string,
	factory Factory[T],
	opts ...Option[T],
) (*Switcher[T], error) {
	options := options[T]{
		factory: factory,
		warnCap: 16,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	if ff == nil {
		return nil, errors.New("switcher requires a feature flags client")
	}
	if options.factory == nil {
		return nil, errors.New("switcher requires a client factory")
	}
	if options.warnCap < 0 {
		return nil, errors.New("switcher warn cap must not be negative")
	}

	var defaultClient T
	if options.defaultClientSet {
		if utils.IsNil(options.defaultClient) {
			return nil, errors.New("provided default client must not be nil")
		}
		defaultClient = options.defaultClient
	} else {
		switch {
		case strings.TrimSpace(defaultDSN) != "":
			client, err := options.factory(strings.TrimSpace(defaultDSN))
			if err != nil {
				return nil, fmt.Errorf("failed to create default client: %w", err)
			}
			if utils.IsNil(client) {
				return nil, errors.New("factory returned nil default client")
			}
			defaultClient = client
		case options.allowNoopDefault:
			if options.noopFactory == nil {
				return nil, errors.New("allowNoopDefault is true but no noopFactory was provided")
			}
			client, err := options.noopFactory()
			if err != nil {
				return nil, fmt.Errorf("failed to create noop client: %w", err)
			}
			if utils.IsNil(client) {
				return nil, errors.New("noopFactory returned nil default client")
			}
			defaultClient = client
		default:
			return nil, errors.New("default DSN is required (or enable allowNoopDefault)")
		}
	}

	clonedDSNs := slices.Clone(alternateDSNs)
	alternates := make([]T, len(clonedDSNs))
	alternateNonNil := make([]bool, len(clonedDSNs))
	alternateHosts := make([]string, 0, len(clonedDSNs))
	var nonBlankAlternates, initializedAlternates int
	var failedIndexes []int

	for i := range clonedDSNs {
		dsn := strings.TrimSpace(clonedDSNs[i])
		if dsn == "" {
			continue
		}

		nonBlankAlternates++
		host := sanitizeHost(dsn)
		alternateHosts = append(alternateHosts, fmt.Sprintf("%d=%s", i, host))

		client, err := options.factory(dsn)
		if err != nil {
			logger.L().Error(ctx, "failed to create alternate client, skipping entry",
				zap.Int("index", i),
				zap.String("host", host),
				zap.Error(sanitizeDriverErr(err)),
			)
			failedIndexes = append(failedIndexes, i)

			continue
		}
		if utils.IsNil(client) {
			logger.L().Error(ctx, "factory returned nil alternate client, skipping entry",
				zap.Int("index", i),
				zap.String("host", host),
			)
			failedIndexes = append(failedIndexes, i)

			continue
		}

		alternates[i] = client
		alternateNonNil[i] = true
		initializedAlternates++
	}

	s := &Switcher[T]{
		defaultClient:   defaultClient,
		alternates:      alternates,
		alternateNonNil: alternateNonNil,
		ff:              ff,
		flag:            flag,
		warnCap:         options.warnCap,
		flagKeyAttr:     attribute.String("flag_key", flag.Key()),
	}

	if options.meter != nil {
		counter, err := options.meter.Int64Counter(
			"switcher.resolve_count",
			metric.WithDescription("Number of times the switcher resolved a client"),
		)
		if err != nil {
			logger.L().Error(ctx, "failed to create switcher metrics", zap.Error(err))
		} else {
			s.resolveCounter = counter
		}
	}

	logger.L().Info(ctx, "initialized switching client",
		zap.String("flag_key", flag.Key()),
		zap.Int("alternate_count_configured", nonBlankAlternates),
		zap.Int("alternate_count_initialized", initializedAlternates),
		zap.Ints("alternate_failed_indexes", failedIndexes),
		zap.Strings("alternate_hosts", alternateHosts),
	)

	return s, nil
}

func (s *Switcher[T]) Resolve(ctx context.Context) T {
	v := strings.TrimSpace(s.ff.StringFlag(ctx, s.flag))
	if v == "" {
		s.recordMetric(ctx, "default")

		return s.defaultClient
	}

	idx, err := strconv.Atoi(v)
	if err != nil || idx < 0 || idx >= len(s.alternates) || !s.alternateNonNil[idx] {
		s.warnInvalid(ctx, v)
		s.recordMetric(ctx, "fallback_default")

		return s.defaultClient
	}

	// Cardinality bound for "target" label: len(alternateDSNs) + 2
	// (default + fallback_default + indices).
	s.recordMetric(ctx, v)

	return s.alternates[idx]
}

func (s *Switcher[T]) recordMetric(ctx context.Context, target string) {
	if s.resolveCounter != nil {
		s.resolveCounter.Add(ctx, 1, metric.WithAttributes(
			s.flagKeyAttr,
			attribute.String("target", target),
		))
	}
}

func (s *Switcher[T]) warnInvalid(ctx context.Context, v string) {
	h := hashFlagValue(v)
	if _, ok := s.warnSeen.Load(h); ok {
		return
	}

	for {
		count := s.warnCount.Load()
		if count >= s.warnCap {
			return
		}
		if _, ok := s.warnSeen.Load(h); ok {
			return
		}
		if !s.warnCount.CompareAndSwap(count, count+1) {
			continue
		}
		if _, loaded := s.warnSeen.LoadOrStore(h, struct{}{}); loaded {
			s.warnCount.Add(-1)

			return
		}

		logger.L().Warn(ctx, "invalid read-endpoint flag value, falling back to default",
			zap.String("flag_key", s.flag.Key()),
			zap.String("value_hash", h),
			zap.Int("value_len", len(v)),
		)

		return
	}
}

func (s *Switcher[T]) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	var errs []error
	if !utils.IsNil(s.defaultClient) {
		errs = append(errs, s.defaultClient.Close(ctx))
	}
	for _, client := range s.alternates {
		if !utils.IsNil(client) {
			errs = append(errs, client.Close(ctx))
		}
	}

	return errors.Join(errs...)
}

func hashFlagValue(v string) string {
	sum := sha256.Sum256([]byte(v))

	return hex.EncodeToString(sum[:])[:8]
}

func sanitizeHost(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "unparseable-dsn-" + hashFlagValue(dsn)
	}

	host := u.Hostname()
	if host == "" {
		return "unparseable-dsn-" + hashFlagValue(dsn)
	}
	// url.Hostname strips brackets from IPv6 literals; re-add them so the
	// host:port form stays unambiguous (e.g. [::1]:9000, not ::1:9000).
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port := u.Port(); port != "" {
		return host + ":" + port
	}

	return host
}

var (
	dsnRE      = regexp.MustCompile(`(?i)\b(clickhouses?|https?|tcp)://[^\s]+`)
	passwordRE = regexp.MustCompile(`(?i)(password|pass|pwd)=([^&\s]+)`)
	userinfoRE = regexp.MustCompile(`(?i)://([^/@\s:]+):([^/@\s]+)@`)
)

func sanitizeDriverErr(err error) error {
	if err == nil {
		return nil
	}

	msg := err.Error()
	msg = userinfoRE.ReplaceAllString(msg, "://<redacted>@")
	msg = passwordRE.ReplaceAllString(msg, "$1=<redacted>")
	msg = dsnRE.ReplaceAllStringFunc(msg, func(raw string) string {
		if idx := strings.Index(raw, "://"); idx >= 0 {
			return raw[:idx+3] + "<redacted>"
		}

		return "<redacted-dsn>"
	})

	return errors.New(msg)
}
