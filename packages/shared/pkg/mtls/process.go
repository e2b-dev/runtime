package mtls

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// ErrNoCertificateConfigured refuses a mode other than off in a process
// whose certificate files are not configured.
var ErrNoCertificateConfigured = errors.New("mtls: a mode other than off needs certificate files, and " + CertFileEnv + " is unset")

// Process is one process's mutual TLS: its certificate files, its
// instruments, its flag reader, and whether certificate files are
// configured at all. Build it once, after the logger and the meter
// provider, and derive every listener and client hop from it.
//
// The chart sets E2B_TLS_CERT_FILE only where it mounts a certificate.
// Where it is unset the process loads nothing, reads no flag and records
// nothing, and every listener and hop it builds is off: the wire and the
// process behave as before this package.
//
// A mode that comes from a flag is also re-read every watch interval, so
// its gauge follows the flag without traffic, and while a listener is
// required, each re-read closes, on every Listener and ServerCredentials
// built on its configuration, the connections required would refuse.
type Process struct {
	files   *Files
	metrics *Metrics
	reader  FlagReader
	log     logger.Logger
	watch   *watcher
}

// ProcessOption configures NewProcess.
type ProcessOption func(*processOptions)

type processOptions struct {
	files         *FileConfig
	watchInterval time.Duration
}

// WithFileConfig reads the certificate files from cfg instead of the
// environment, and counts them as configured whether or not they exist yet.
func WithFileConfig(cfg FileConfig) ProcessOption {
	return func(o *processOptions) { o.files = &cfg }
}

// WithWatchInterval replaces DefaultWatchInterval.
func WithWatchInterval(interval time.Duration) ProcessOption {
	return func(o *processOptions) { o.watchInterval = interval }
}

// NewProcess loads the certificate files and re-reads them every minute
// until ctx ends, and creates the instruments on provider. reader is the
// service's flag client, or nil for a service whose modes come from the
// environment alone; a nil log means logger.L().
func NewProcess(ctx context.Context, provider metric.MeterProvider, reader FlagReader, log logger.Logger, opts ...ProcessOption) (*Process, error) {
	var options processOptions
	for _, o := range opts {
		o(&options)
	}
	if log == nil {
		log = logger.L()
	}
	if options.files == nil && strings.TrimSpace(os.Getenv(CertFileEnv)) != "" {
		fromEnv := FileConfigFromEnv()
		options.files = &fromEnv
	}
	if options.files == nil {
		// Not configured: record into nothing and read no flag.
		provider, reader = noop.NewMeterProvider(), nil
	}

	metrics, err := NewMetrics(provider)
	if err != nil {
		return nil, fmt.Errorf("mtls: instruments: %w", err)
	}
	p := &Process{metrics: metrics, reader: reader, log: log}
	if options.files != nil {
		p.files = NewFiles(ctx, *options.files, WithFilesLogger(log), WithFilesMetrics(metrics))
		p.files.Start(ctx)
	}
	if p.reader != nil {
		p.watch = newWatcher(ctx, options.watchInterval, log)
	}

	return p, nil
}

// Enabled reports whether certificate files are configured.
func (p *Process) Enabled() bool {
	return p.files != nil
}

// Listener builds the configuration of the listener a chart renders under
// key, named key in metrics and logs. Its mode comes from the flag flagKey,
// with settings.Mode as the fallback, when the process has a flag reader
// and flagKey is set, and from settings.Mode alone otherwise; blank is off.
// A malformed mode or allow-list is an error naming its variable; opts
// apply to the allow-list.
func (p *Process) Listener(key, flagKey string, settings ListenerSettings, opts ...AllowListOption) (ServerConfig, error) {
	prefix := EnvPrefix(key)
	fallback := ModeOff
	if strings.TrimSpace(settings.Mode) != "" {
		var err error
		if fallback, err = ParseMode(settings.Mode); err != nil {
			return ServerConfig{}, fmt.Errorf("%sMODE: %w", prefix, err)
		}
	}
	allow, err := ParseAllowList(settings.Allow, opts...)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("%sALLOW: %w", prefix, err)
	}
	if !p.Enabled() && fallback != ModeOff {
		return ServerConfig{}, fmt.Errorf("%sMODE: %w", prefix, ErrNoCertificateConfigured)
	}

	cfg := ServerConfig{Name: key, Files: p.files, Mode: StaticMode(fallback), Allow: allow, Logger: p.log, Metrics: p.metrics}
	if p.reader != nil && flagKey != "" {
		cfg.Mode = NewFlagModeSource(p.reader, flagKey, fallback, allow, WithModeLogger(p.log))
		cfg.watched = p.watch.listener(cfg)
	}

	return cfg, nil
}

// Hop builds the configuration of the client hop a chart renders under
// key, named key in metrics and logs; its mode comes from flagKey as for
// Listener. The expected server names refuse a wildcard, as the chart does.
// A hop on from the environment needs at least one; a flag-backed hop
// without one builds, and its flag cannot turn it on.
func (p *Process) Hop(key, flagKey string, settings HopSettings) (ClientConfig, error) {
	prefix := EnvPrefix(key)
	fallback := ClientOff
	if strings.TrimSpace(settings.Mode) != "" {
		var err error
		if fallback, err = ParseClientMode(settings.Mode); err != nil {
			return ClientConfig{}, fmt.Errorf("%sCLIENT_MODE: %w", prefix, err)
		}
	}
	expected, err := ParseAllowList(settings.Expect, WithoutWildcards())
	if err != nil {
		return ClientConfig{}, fmt.Errorf("%sEXPECT: %w", prefix, err)
	}
	if !p.Enabled() && fallback != ClientOff {
		return ClientConfig{}, fmt.Errorf("%sCLIENT_MODE: %w", prefix, ErrNoCertificateConfigured)
	}
	if fallback == ClientOn && expected.Len() == 0 {
		// An empty set refuses every server: fail here, not at every dial.
		return ClientConfig{}, fmt.Errorf("%sEXPECT: a hop that is on needs an expected server name", prefix)
	}

	cfg := ClientConfig{
		Name:              key,
		Files:             p.files,
		Mode:              StaticClientMode(fallback),
		ServerName:        strings.TrimSpace(settings.ServerName),
		ExpectedServerIDs: expected,
		Logger:            p.log,
		Metrics:           p.metrics,
	}
	if p.reader != nil && flagKey != "" {
		cfg.Mode = NewFlagClientModeSource(p.reader, flagKey, fallback, WithModeLogger(p.log)).refuseOnWithout(expected)
		cfg.watched = p.watch.hop(cfg)
	}

	return cfg, nil
}
