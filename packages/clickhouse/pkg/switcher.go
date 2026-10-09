package clickhouse

import (
	"context"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils/switching"
)

// SwitchingClient is a Clickhouse client that routes each read to one of
// several DSNs based on the clickhouse-read-endpoint LaunchDarkly flag.
// Each call delegates to switcher.Resolve(ctx), so the active endpoint can
// change between calls without restarting. See
// packages/shared/pkg/utils/switching for the underlying mechanism.
type SwitchingClient struct {
	switcher *switching.Switcher[Clickhouse]
}

var _ Clickhouse = (*SwitchingClient)(nil)

// NewSwitchingClient builds N+1 clients (one for defaultDSN, one per alternate)
// and selects between them per-call using ClickhouseReadEndpointFlag. An empty
// flag value (the LD default) selects the default client; "0", "1", … select
// alternateDSNs[i]. Invalid values fall back to default + rate-limited warning.
func NewSwitchingClient(
	ctx context.Context,
	ff *featureflags.Client,
	defaultDSN string,
	alternateDSNs []string,
	opts ...Option,
) (*SwitchingClient, error) {
	var sOpts []switching.Option[Clickhouse]
	for _, opt := range opts {
		if opt != nil {
			sOpts = append(sOpts, switching.Option[Clickhouse](opt))
		}
	}

	s, err := switching.New[Clickhouse](
		ctx,
		ff,
		featureflags.ClickhouseReadEndpointFlag,
		defaultDSN,
		alternateDSNs,
		func(dsn string) (Clickhouse, error) { return New(dsn) },
		append(sOpts, switching.WithNoopFactory(func() (Clickhouse, error) {
			return NewNoopClient(), nil
		}))...,
	)
	if err != nil {
		return nil, err
	}

	return &SwitchingClient{switcher: s}, nil
}

// Option mirrors switching.Option for caller convenience.
type Option switching.Option[Clickhouse]

// WithAllowNoopDefault enables falling back to a noop client when the
// default DSN is empty.
func WithAllowNoopDefault(allow bool) Option {
	return Option(switching.WithAllowNoopDefault[Clickhouse](allow))
}

func (s *SwitchingClient) Close(ctx context.Context) error {
	return s.switcher.Close(ctx)
}

func (s *SwitchingClient) QuerySandboxTimeRange(ctx context.Context, sandboxID, teamID string) (time.Time, time.Time, error) {
	return s.switcher.Resolve(ctx).QuerySandboxTimeRange(ctx, sandboxID, teamID)
}

func (s *SwitchingClient) QuerySandboxMetrics(ctx context.Context, sandboxID, teamID string, start, end time.Time, step time.Duration) ([]Metrics, error) {
	return s.switcher.Resolve(ctx).QuerySandboxMetrics(ctx, sandboxID, teamID, start, end, step)
}

func (s *SwitchingClient) QueryLatestMetrics(ctx context.Context, sandboxIDs []string, teamID string) ([]Metrics, error) {
	return s.switcher.Resolve(ctx).QueryLatestMetrics(ctx, sandboxIDs, teamID)
}

func (s *SwitchingClient) QueryTeamMetrics(ctx context.Context, teamID string, start, end time.Time, step time.Duration) ([]TeamMetrics, error) {
	return s.switcher.Resolve(ctx).QueryTeamMetrics(ctx, teamID, start, end, step)
}

func (s *SwitchingClient) QueryMaxStartRateTeamMetrics(ctx context.Context, teamID string, start, end time.Time, step time.Duration) (MaxTeamMetric, error) {
	return s.switcher.Resolve(ctx).QueryMaxStartRateTeamMetrics(ctx, teamID, start, end, step)
}

func (s *SwitchingClient) QueryMaxConcurrentTeamMetrics(ctx context.Context, teamID string, start, end time.Time) (MaxTeamMetric, error) {
	return s.switcher.Resolve(ctx).QueryMaxConcurrentTeamMetrics(ctx, teamID, start, end)
}

func (s *SwitchingClient) QuerySandboxLifecycle(ctx context.Context, sandboxID string) (SandboxLifecycle, error) {
	return s.switcher.Resolve(ctx).QuerySandboxLifecycle(ctx, sandboxID)
}
