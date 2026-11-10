//go:build linux

package sandbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Post-start fault-in: what the guest still had to pull into memory after it
// was usable. The uffd.startup.* histograms stop at the first successful envd
// init, but a resumed guest keeps faulting in the pages its own processes
// touch, and every first touch of a page that was not prefetched waits on
// the source. A resume can return quickly while the customer's first calls
// stall on exactly that, so these windows measure it per team.
var (
	uffdPostStartPagesHistogram           = utils.Must(telemetry.GetHistogram(meter, telemetry.UffdPostStartPagesHistogramName))
	uffdPostStartSourcePagesHistogram     = utils.Must(telemetry.GetHistogram(meter, telemetry.UffdPostStartSourcePagesHistogramName))
	uffdPostStartServeTimeHistogram       = utils.Must(telemetry.GetHistogram(meter, telemetry.UffdPostStartServeTimeHistogramName))
	uffdPostStartSourceServeTimeHistogram = utils.Must(telemetry.GetHistogram(meter, telemetry.UffdPostStartSourceServeTimeHistogramName))
)

// postStartWindow is a point after the first successful envd init at which
// the faults served since then are recorded. label is the `window` attribute
// value, fixed so queries read window="1m" rather than time.Duration's "1m0s".
type postStartWindow struct {
	after time.Duration
	label string
}

// postStartWindows are cumulative from that init. Short enough to catch a
// guest still loading when its first calls arrive, long enough to see when it
// settles.
var postStartWindows = []postStartWindow{
	{10 * time.Second, "10s"},
	{time.Minute, "1m"},
	{5 * time.Minute, "5m"},
}

// postStartBase is the fault-in state at the first successful envd init of a
// customer start: the baseline the post-start windows are measured from.
type postStartBase struct {
	at        time.Time
	stats     userfaultfd.ServeSnapshot
	startType StartType
}

// recordsPostStartFaultIn reports whether a start of startType gets post-start
// windows. Only a memory resume serves demand faults: a create or a reboot cold
// boots on no-op memory and would record zeros. A build's resumes run build
// steps, not customer code, and stay out of the team's series.
func (s *Sandbox) recordsPostStartFaultIn(startType StartType) bool {
	return startType == StartTypeResume && s.Runtime.SandboxType != sandboxtypes.SandboxTypeBuild
}

// recordPostStartFaultIn records, at each post-start window, the demand faults
// the guest needed since its first successful envd init. WaitForEnvd starts it
// once, at that init, for a customer memory resume only (a throwaway resume,
// a create, a reboot and a build record nothing). It stops when the
// Firecracker process exits, so a sandbox paused or killed early records only
// the windows it reached. Across an in-place checkpoint the process keeps
// running and so does the sampling.
func (s *Sandbox) recordPostStartFaultIn(parent trace.SpanContext, base *postStartBase) {
	// Keep the trace identity without retaining the completed request or span.
	ctx := trace.ContextWithSpanContext(context.Background(), parent)
	ctx, cancel := contextUntil(ctx, s.process.Exit.Done())
	defer cancel()

	runPostStartWindows(ctx, base.at, postStartWindows, time.Now, time.After, s.memory.ServeStats,
		func(window postStartWindow, now userfaultfd.ServeSnapshot) {
			s.emitPostStartWindow(ctx, base, window, now.Sub(base.stats))
		})
}

// contextUntil returns a context that is cancelled when done closes, or when
// the returned cancel is called.
func contextUntil(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-done:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}

// runPostStartWindows waits for each window after start and hands emit the
// snapshot taken then. It returns when ctx ends or after the last window.
// The clock, timer and snapshot source are parameters so the schedule can be
// tested without real time.
func runPostStartWindows(
	ctx context.Context,
	start time.Time,
	windows []postStartWindow,
	now func() time.Time,
	after func(time.Duration) <-chan time.Time,
	snapshot func() userfaultfd.ServeSnapshot,
	emit func(window postStartWindow, snap userfaultfd.ServeSnapshot),
) {
	for _, window := range windows {
		if wait := start.Add(window.after).Sub(now()); wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-after(wait):
			}
		}
		// Checked again so a window that came due as the sandbox stopped is
		// not recorded from a guest that is already going away.
		if ctx.Err() != nil {
			return
		}

		emit(window, snapshot())
	}
}

func (s *Sandbox) emitPostStartWindow(ctx context.Context, base *postStartBase, window postStartWindow, d userfaultfd.ServeSnapshot) {
	// The parent wait span has ended, but its context keeps this point-in-time
	// summary on the same trace as the resume. No new per-page work is needed.
	_, span := tracer.Start(ctx, "post-start-fault-window")
	span.SetAttributes(
		attribute.String("window", window.label),
		attribute.Int64("post_start.pages", d.Pages),
		attribute.Int64("post_start.source_pages", d.SourcePages),
		attribute.Float64("post_start.serve_sum_ms", ms(d.ServeTime)),
		attribute.Float64("post_start.source_serve_sum_ms", ms(d.SourceServeTime)),
	)
	span.SetAttributes(sourceAttrs("post_start", d)...)
	span.End()

	attrs := metric.WithAttributes(
		attribute.String("window", window.label),
		attribute.String("start_type", string(base.startType)),
		s.teamAttr(),
	)
	uffdPostStartPagesHistogram.Record(ctx, d.Pages, attrs)
	uffdPostStartSourcePagesHistogram.Record(ctx, d.SourcePages, attrs)
	uffdPostStartServeTimeHistogram.Record(ctx, d.ServeTime.Milliseconds(), attrs)
	uffdPostStartSourceServeTimeHistogram.Record(ctx, d.SourceServeTime.Milliseconds(), attrs)

	s.log().Debug(ctx, "post-start fault-in",
		zap.String("window", window.label),
		zap.String("start_type", string(base.startType)),
		zap.Int64("pages", d.Pages),
		zap.Int64("source_pages", d.SourcePages),
		zap.Int64("bytes", d.Bytes),
		zap.Int64("deferred", d.Deferred),
		zap.Int64("serve_ms", d.ServeTime.Milliseconds()),
		zap.Int64("source_serve_ms", d.SourceServeTime.Milliseconds()),
	)
}
