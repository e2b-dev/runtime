//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"math"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// envdMemoryHeader carries the memory protection configured on the guest envd's cgroup
// chain, on every /init response. Same JSON-header convention as the audit and the
// defaults above it in initEnvd.
const envdMemoryHeader = "X-Envd-Memory"

// EnvdMemoryProtection is what the running envd reports about the memory protection on
// its cgroup chain, from the X-Envd-Memory header on /init. Its JSON tags match envd's
// cgroups.MemoryProtection, which carries the wire form, so the header unmarshals
// directly. Values are bytes, as the cgroup files hold them.
//
// It is a report of what the guest's init system configured, never an input to a decision
// on the host: the orchestrator records it and labels the init instruments with it, and
// acts on nothing in it. Absent from an envd that predates the header, which is not an
// error and records nothing.
type EnvdMemoryProtection struct {
	// Request is memory.min of envd's own cgroup.
	Request uint64 `json:"request"`
	// Low is memory.low of envd's own cgroup.
	Low uint64 `json:"low"`
	// Floor is the minimum of memory.min over every level of the chain below the root,
	// envd's own included. Protection is granted top-down, so a Floor of 0 with Partial
	// false means either some level carries none or there is no level at all, envd being
	// in the root cgroup, and envd is unprotected however large its own request. With
	// Partial set the same 0 is a value that could not be read rather than one that was
	// measured, and the cohort is unknown instead. A level requesting protection without
	// a bound reports MaxInt64 and leaves the floor to the rest of the chain.
	Floor uint64 `json:"floor"`
	// Partial is true when envd could not read a value and reported it as 0.
	Partial bool `json:"partial"`
}

// envdProtectionCohort is what a start's init instruments are labelled with, so that the
// attempt count and the duration distribution of protected starts can be read against
// unprotected ones. It is three-valued rather than a boolean because a report envd could
// not fully read measures nothing, and the cohort that means "the chain was read and
// carries no protection" must not absorb it.
type envdProtectionCohort string

const (
	// envdProtectionProtected: every level of the chain below the root carries a request.
	envdProtectionProtected envdProtectionCohort = "protected"
	// envdProtectionUnprotected: the chain was read in full and either some level carries
	// no request or there is no level at all, envd being in the root cgroup. Either way it
	// is granted nothing however large its own request.
	envdProtectionUnprotected envdProtectionCohort = "unprotected"
	// envdProtectionUnknown: envd answered but could not read part of its chain, so the
	// zeros in its report are absences rather than measurements.
	envdProtectionUnknown envdProtectionCohort = "unknown"
)

// protection is the cohort key derived for the init instruments.
//
// A positive floor decides the cohort before Partial is consulted, and the order is what
// makes the two conditions independent rather than overlapping: a memory.min that could
// not be read contributes 0 to the minimum and takes the floor down with it, so a floor
// above 0 proves every level's request was read, and the only field a partial can then
// concern is memory.low, which the cohort does not rest on. A floor of 0 with Partial
// set, by contrast, is a chain nobody could see, and reporting it as unprotected would
// claim a measurement that was never taken.
func (p EnvdMemoryProtection) protection() envdProtectionCohort {
	switch {
	case p.Floor > 0:
		return envdProtectionProtected
	case p.Partial:
		return envdProtectionUnknown
	default:
		return envdProtectionUnprotected
	}
}

// envdMemoryHeaderMaxBytes bounds the guest-written header before it is decoded. The
// three integers fit in 20 digits each and the envelope in a few dozen bytes, so a
// legitimate header is about a hundred bytes; 1 KiB leaves room for fields a newer envd
// adds while keeping what a hostile guest can push through the decoder small.
const envdMemoryHeaderMaxBytes = 1 << 10

// decodeEnvdMemoryProtection turns the X-Envd-Memory header into a report, refusing
// anything oversized, non-numeric, negative, or too large to carry as an int64 span
// attribute.
func decodeEnvdMemoryProtection(header string) (EnvdMemoryProtection, error) {
	p, err := decodeEnvdHeader[EnvdMemoryProtection](header, envdMemoryHeaderMaxBytes)
	if err != nil {
		return EnvdMemoryProtection{}, err
	}

	for name, v := range map[string]uint64{"request": p.Request, "low": p.Low, "floor": p.Floor} {
		if v > math.MaxInt64 {
			return EnvdMemoryProtection{}, fmt.Errorf("%s is %d, over the int64 range", name, v)
		}
	}

	return p, nil
}

// recordEnvdMemoryProtection decodes the X-Envd-Memory header and records it.
//
// The span attributes are set on every /init whose header decodes, so a checkpoint
// re-init's report stays visible in its own trace. The protection histogram is recorded
// only under recordMetrics, the predicate that already gates envd.init.calls: the first
// WaitForEnvd of a start, whatever that /init's status, the prefetch harvest's throwaway
// resume included, so that the histogram has one sample per kind for every start that
// init.calls counts under exit_type=success (one per responded start), less those whose
// header did not decode (an envd predating it, a malformed one), and the two can be
// divided. Recording on every /init would count an upgraded resume twice, once
// from the old envd and once from the new; an upgraded resume therefore
// contributes its new envd's report to the span alone until it is next paused
// with that envd inside.
//
// The decoded report is retained on the Sandbox so envdProtectionAttrs can label the
// init instruments. A header that will not decode is logged, once per occurrence, and
// stores nothing, so a start whose first /init carried one has no report and carries no
// cohort attribute rather than a guessed one, the same as an envd that predates the
// header; a report an earlier /init on the same Sandbox stored stands.
func (s *Sandbox) recordEnvdMemoryProtection(ctx context.Context, header string, startType StartType, recordMetrics bool) {
	p, err := decodeEnvdMemoryProtection(header)
	if err != nil {
		s.log().Warn(ctx, "could not decode the envd memory protection header",
			zap.Error(err),
		)

		return
	}

	s.envdMemory.Store(&p)

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.Int64("envd.memory.request", int64(p.Request)),
			attribute.Int64("envd.memory.low", int64(p.Low)),
			attribute.Int64("envd.memory.floor", int64(p.Floor)),
			attribute.Bool("envd.memory.partial", p.Partial),
		)
	}

	if !recordMetrics {
		return
	}

	attrs := []attribute.KeyValue{
		telemetry.WithEnvdVersion(s.Config.Envd.Version),
		attribute.String("start_type", string(startType)),
	}
	ramMB := s.Config.RamMB
	envdMemoryProtectionHistogram.Record(ctx, envdMemoryProtectionMiB(p.Request, ramMB),
		metric.WithAttributes(append(attrs, attribute.String("kind", "request"))...))
	envdMemoryProtectionHistogram.Record(ctx, envdMemoryProtectionMiB(p.Floor, ramMB),
		metric.WithAttributes(append(attrs, attribute.String("kind", "floor"))...))
}

// envdMemoryProtectionMiB is the histogram value for one reported byte count: MiB,
// truncated, capped at the sandbox's own RAM.
//
// The cap costs no reading, because a request above the guest's RAM cannot protect more
// memory than the guest has, and it is what keeps the series legible. The exporter
// aggregates every histogram as base-2 exponential and lowers the scale until the buckets
// span the range it observes, so a single unbounded request -- memory.min = max, reported
// as MaxInt64, which is 8.8e12 MiB -- would widen every bucket in its series to 18.9%,
// where 50 MiB and 52 MiB stop being distinguishable. Capped, the series spans the
// fleet's sandbox sizes instead: 4.4% buckets against a multi-GiB largest sample, 2.2%
// where the samples sit inside five octaves. The raw byte count stays on the envd-init
// span, which is where an unbounded chain is told apart from one that asked for exactly
// the guest's RAM.
//
// A RamMB of 0 is a size this call does not know rather than a guest with no memory, and
// capping at it would file an unprotected magnitude beside a protected cohort; such a
// start records the value as it came instead.
func envdMemoryProtectionMiB(bytes uint64, ramMB int64) int64 {
	mib := int64(bytes >> 20)
	if ramMB > 0 && mib > ramMB {
		return ramMB
	}

	return mib
}

// envdProtectionAttrs is the cohort attribute for the init instruments, derived from the
// most recent memory report. Empty when no report has decoded: an envd that predates the
// header, a header that would not decode, or a start that got no /init response at all
// (a timeout, an exited guest). Such a start carries no attribute rather than a guessed
// one, so its cohort is recovered offline from the template it was built from, never read
// off this label.
//
// No attribute at all and protection="unknown" are different facts and are deliberately
// not merged: the first is a start this signal never reached, and it has no sample in the
// protection histogram either, while the second is a start envd answered and reported its
// own blindness on.
func (s *Sandbox) envdProtectionAttrs() []attribute.KeyValue {
	p := s.envdMemory.Load()
	if p == nil {
		return nil
	}

	return []attribute.KeyValue{attribute.String("protection", string(p.protection()))}
}
