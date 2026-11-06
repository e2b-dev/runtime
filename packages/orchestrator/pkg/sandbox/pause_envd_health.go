//go:build linux

package sandbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// AwaitEnvdAdmission is the envd half of the pause pre-flight: it asks the
// guest agent for /health before a snapshot. A memory snapshot of a hung
// agent replays hung on every resume; a filesystem-only capture needs that
// agent to freeze the rootfs and loses the sandbox when it cannot.
//
// A timeout of zero or less disables the probe (a zero deadline would expire
// before the request is sent and refuse every pause); a disabled probe decides
// nothing and returns the empty outcome. A timeout longer than the sandbox HTTP
// client's own limit is capped to it, since the client aborts there anyway.
// The timeout is passed in rather than read from a flag here, like the
// durable-header wait, so the method stays callable from a Sandbox built
// without a flag client. A nil error means the pause may proceed.
func (s *Sandbox) AwaitEnvdAdmission(ctx context.Context, timeout time.Duration) (SnapshotAdmissionOutcome, time.Duration, error) {
	if timeout <= 0 {
		return "", 0, nil
	}

	ctx, span := tracer.Start(ctx, "envd-admission")
	defer span.End()

	return s.awaitEnvdHealthy(ctx, capEnvdProbeTimeout(timeout, sandboxHttpClient.Timeout))
}

// SetEnvdServerURLForTest points every envd call of this sandbox at url, so a
// test outside this package serves envd itself; the client, its transport and
// its timeout stay the production ones.
func (s *Sandbox) SetEnvdServerURLForTest(url string) {
	s.internalConfig.envdServerURLOverride = url
}

// MarkExitedForTest ends this sandbox's execution with err, as a Firecracker
// exit would, without any cleanup: the live-map entry stays where it is.
func (s *Sandbox) MarkExitedForTest(err error) {
	if s.exit == nil {
		s.exit = utils.NewErrorOnce()
	}
	_ = s.exit.SetError(err)
}

// capEnvdProbeTimeout bounds the probe by the HTTP client's own Timeout, which
// aborts the request regardless of a longer context deadline. A zero limit
// means the client sets none.
func capEnvdProbeTimeout(timeout, limit time.Duration) time.Duration {
	if limit > 0 && timeout > limit {
		return limit
	}

	return timeout
}

func (s *Sandbox) awaitEnvdHealthy(ctx context.Context, timeout time.Duration) (SnapshotAdmissionOutcome, time.Duration, error) {
	// A sandbox torn down concurrently can leave the checker nil: missing
	// evidence is not evidence of a hung envd, so nothing is decided.
	checks := s.Checks()
	if checks == nil {
		return "", 0, nil
	}

	start := time.Now()
	healthy, err := checks.getHealth(ctx, timeout)
	waited := time.Since(start)

	trace.SpanFromContext(ctx).SetAttributes(
		attribute.Bool("admission.envd_healthy", healthy),
		attribute.Int64("admission.envd_probe_ms", waited.Milliseconds()),
		attribute.Int64("admission.envd_probe_timeout_ms", timeout.Milliseconds()),
	)

	if healthy {
		return SnapshotAdmissionEnvdHealthy, waited, nil
	}

	// The caller's context ending is not evidence about envd: nothing was
	// decided and the sandbox is untouched.
	if ctx.Err() != nil {
		return "", waited, ctx.Err()
	}

	s.log().Warn(ctx, "envd did not answer the pause admission probe",
		zap.Error(err), zap.Duration("probe", waited))

	return SnapshotAdmissionEnvdUnhealthy, waited, ErrSnapshotAdmissionEnvdUnhealthy
}
