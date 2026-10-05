//go:build linux

package chrooted

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// The three counters are the security signal of the package: each one counts
// an input the kernel or the package refused to act on, which a tenant
// probing the boundary produces and normal traffic does not.
//
// They are plain atomics read by observable instruments. The filesystem
// methods implement go-billy's interface, which carries no context, so a
// synchronous instrument would have nothing to record against; an atomic add
// also keeps the hot path free of the instrument's own cost.
var (
	resolveRetriesExhausted atomic.Int64
	pathsRefused            atomic.Int64
	resolveLoops            atomic.Int64
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted")

	_ = utils.Must(meter.Int64ObservableCounter(
		"orchestrator.chroot.resolve.retries_exhausted",
		metric.WithDescription("Path resolutions abandoned because the kernel kept detecting a rename or mount race"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(observe(&resolveRetriesExhausted)),
	))
	_ = utils.Must(meter.Int64ObservableCounter(
		"orchestrator.chroot.path.refused",
		metric.WithDescription("Entry names refused before reaching a syscall: empty, \".\", \"..\" or containing a slash"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(observe(&pathsRefused)),
	))
	_ = utils.Must(meter.Int64ObservableCounter(
		"orchestrator.chroot.resolve.eloop",
		metric.WithDescription("Symlink resolutions abandoned at the hop limit"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(observe(&resolveLoops)),
	))
)

func observe(counter *atomic.Int64) metric.Int64Callback {
	return func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(counter.Load())

		return nil
	}
}
