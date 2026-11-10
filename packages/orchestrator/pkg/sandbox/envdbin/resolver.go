package envdbin

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	binaryCacheReads      = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorEnvdBinaryCacheReads))
	binaryCacheDeliveries = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorEnvdBinaryCacheDeliveries))
)

// Op names the upgrade path a Resolver serves, for metrics.
type Op string

const (
	// OpLive is the resume-time live upgrade of a running envd.
	OpLive Op = "live"
	// OpOffline is the cold-boot rootfs swap of a filesystem-only snapshot.
	OpOffline Op = "offline"
)

// ErrNotCached reports that the binary is not on local disk, so the upgrade must
// be deferred to a later resume rather than read from the mount.
//
// It is not a failure. The warm it triggers usually lands within seconds, and the
// upgrade is idempotent and re-fires on every resume, so the cost is one cycle.
var ErrNotCached = errors.New("envd binary not cached on this node")

// ReasonNotCached is the gated-reason label for an upgrade ErrNotCached deferred.
// It is deliberately distinct from getversion_failed: that means a target that
// cannot be read, which is a misconfiguration worth chasing, while this is the
// cache working as designed on a node that has not warmed yet.
const ReasonNotCached = "binary_not_cached"

// ReasonCopyVanished is the gated reason for a resume that resolved a version and
// then found the copy carrying it gone -- retired by the cap, or replaced by a
// warm that saw a promotion.
//
// A deferral and a refusal are the same event to an operator -- this resume did
// not upgrade, and here is why -- so they share one vocabulary instead of two
// series that have to be summed. On the live path that vocabulary is the gated
// series, not upgrade.attempts, because nothing reached the guest. The offline
// path has no separate gated series, so it reports this on
// offline_upgrade.attempts beside not_quiesced and not_staged. Which of the two
// causes retired the copy is deliberately not distinguished: the caller's
// situation is identical, and separating them would suggest one needed acting on.
const ReasonCopyVanished = "copy_vanished"

// GatedReason relabels the upgrade resolver's reason when the deferral came from
// this cache rather than from the upgrade's own probe of the target.
//
// The resolver reports any probe error as getversion_failed, which is the right
// label for a missing or unreadable target -- an operator error worth chasing and
// logging per occurrence. A deferral is neither: it is the expected state of a node
// that has not warmed yet, it clears itself within a resume, and on a ramp it needs
// its own count so the deferred population is visible. Folded together, a real
// misconfiguration would hide behind an expected state on every freshly booted node.
//
// outcome is the caller's Resolver.DeferralOutcome, not its Outcome: a miss whose
// source stat is what failed is a miss to the reads counter but not a deferral, so
// it arrives here as "" and keeps getversion_failed. "" is also what a caller with
// no cache reports (the flag is off). Either way the reason passes through untouched.
func GatedReason(reason string, outcome Outcome) string {
	if reason == "getversion_failed" && outcome == OutcomeMiss {
		return ReasonNotCached
	}

	return reason
}

// Resolver adapts a Cache to the version-probe function the envd-upgrade
// resolver injects, and remembers what it resolved.
//
// It exists for one property: the upgrade decides on a version, and then — as a
// separate step — reads the bytes to deliver. Handing the caller back the same
// entry the decision was made from makes those the same file. Re-resolving by
// path instead would reopen the window this cache closes, since a promotion
// landing between the two steps would deliver a binary whose version was never
// the one recorded.
//
// On a miss it neither waits for the cache to fill nor reads the source: it warms
// in the background and returns ErrNotCached, which defers the upgrade to a later
// resume. Reading the source here would put the mount back on the resume path —
// an exec that costs seconds on a cold node and is bounded only by the resume's
// own deadline — which is the cost this cache exists to remove, not to make
// rarer.
//
// A Resolver is single-use and not safe for concurrent use: one per upgrade
// attempt.
type Resolver struct {
	cache *Cache
	op    Op

	entry   *Entry
	outcome Outcome

	// sourceUnreadable records that the miss was the source's own stat failing,
	// not an unwarmed cache. Both return ErrNotCached and both count as a miss --
	// this resume does not upgrade either way -- but only one of them is a
	// deferral that clears itself, so only one may be relabeled as one.
	sourceUnreadable bool
}

// NewResolver returns a Resolver backed by cache, reporting its reads as op.
func NewResolver(cache *Cache, op Op) *Resolver {
	return &Resolver{cache: cache, op: op}
}

// Version reports the version baked into the binary at path, matching the
// signature the upgrade resolver injects.
func (r *Resolver) Version(ctx context.Context, path string) (string, error) {
	ctx, span := tracer.Start(ctx, "envd-binary-cache-lookup", trace.WithAttributes(
		attribute.String("envd.binary_cache.op", string(r.op)),
		attribute.String("envd.binary_cache.path", path),
	))
	defer span.End()

	entry, fi, err := r.cache.lookupSource(ctx, path)
	if err == nil && entry != nil {
		r.entry, r.outcome = entry, OutcomeHit
		r.count(ctx, OutcomeHit, "")
		span.SetAttributes(attribute.String("envd.binary_cache.outcome", string(OutcomeHit)))

		return entry.Version, nil
	}

	// Nothing cached. Fill it for the resumes after this one and defer this one:
	// the caller skips the upgrade rather than paying for the mount on a path a
	// customer is waiting on. The identity the stat above resolved is handed to the
	// warm, so its slot is keyed on these bytes and a warm wedged on an earlier
	// generation cannot hold this one out.
	//
	// Unless that stat is what failed, in which case a warm has nothing to add: it
	// would open by taking the same stat, and with no identity in hand its slot
	// could only be claimed after that -- one goroutine per resume against a mount
	// that has already answered no.
	retErr, cause := ErrNotCached, missNotWarmed
	switch {
	case err != nil && (ctx.Err() != nil || errors.Is(err, errSourceStatStalled)):
		// A mount that has not answered yet, or a caller that stopped waiting for
		// it. No warm from here, with no identity to key it on -- the stat starts
		// one when it lands -- and no schedule, since the source has not failed.
		// The cause is wrapped so the upgrade resolver reports source_stalled, not
		// a cold cache.
		cause = missStalled
		if ctx.Err() != nil {
			cause = missCancelled
		} else {
			r.cache.noteSourceStalled(ctx, path, err)
		}
		retErr = fmt.Errorf("%w: %w", ErrNotCached, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	case errors.Is(err, errNoCache):
		cause = missNoCache
	case err != nil:
		cause = missUnreadable
		r.cache.noteSourceUnreadable(ctx, path, err)
		r.sourceUnreadable = true
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	default:
		r.cache.warmAsyncFor(ctx, path, fi)
	}
	r.entry, r.outcome = nil, OutcomeMiss
	r.count(ctx, OutcomeMiss, cause)
	span.SetAttributes(
		attribute.String("envd.binary_cache.outcome", string(OutcomeMiss)),
		attribute.String("envd.binary_cache.miss_cause", cause),
	)

	return "", retErr
}

// Stat checks an upgrade candidate through the cache's shared, bounded stat, for
// the upgrade resolver's existence checks, which run before Version. A stat that
// lands after this caller gave up starts the warm, as one behind Version does.
//
// A stall here is where a stalled mount almost always surfaces: the check stats
// the path Version would look up next, and the resolver returns source_stalled
// without calling Version. So the stall is counted as this resolution's one read
// here, or the reads counter would never see it.
func (r *Resolver) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	if r == nil || r.cache == nil {
		return (*Cache)(nil).BoundedStat(ctx, path)
	}

	fi, err := r.cache.statSourceForLookup(ctx, path, true)
	if err != nil && (ctx.Err() != nil || errors.Is(err, errSourceStatStalled)) {
		cause := missStalled
		if ctx.Err() != nil {
			cause = missCancelled
		} else {
			r.cache.noteSourceStalled(ctx, path, err)
		}
		r.entry, r.outcome = nil, OutcomeMiss
		r.count(ctx, OutcomeMiss, cause)
	}

	return fi, err
}

// The causes a miss is counted with on the reads counter.
const (
	missNotWarmed  = "not_warmed"
	missStalled    = "stalled"
	missCancelled  = "cancelled"
	missUnreadable = "unreadable"
	missNoCache    = "no_cache"
)

// count records what a lookup found: exactly one per resolution. cause is set on
// a miss only, so the hit/total ratio reads the same with or without it.
func (r *Resolver) count(ctx context.Context, outcome Outcome, cause string) {
	attrs := []attribute.KeyValue{
		attribute.String("outcome", string(outcome)),
		attribute.String("op", string(r.op)),
	}
	if cause != "" {
		attrs = append(attrs, attribute.String("cause", cause))
	}
	binaryCacheReads.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// countDelivery records what a caller actually read. Its own series, because its
// denominator is not a resolution: only the resolutions that go on to read the
// bytes reach it, and mixing the two would make hit/total exceed the number of
// resolutions.
func (r *Resolver) countDelivery(ctx context.Context, outcome Outcome) {
	binaryCacheDeliveries.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", string(outcome)),
		attribute.String("op", string(r.op)),
	))
}

// EntryFor returns the entry this Resolver probed, if it was for path.
// It returns nil when nothing was resolved, or when the resolver settled on a
// different path than the one probed. Delivering from an unrelated entry is the
// hazard it guards; a nil return is not licence to read the source, which only a
// nil Resolver may do — see SourcePath.
func (r *Resolver) EntryFor(path string) *Entry {
	if r == nil || r.entry == nil || r.entry.SourcePath != path {
		return nil
	}

	return r.entry
}

// Outcome reports how the last resolution was served, for metrics -- a miss
// included, which is the value GatedReason keys on. It is "" when no resolution
// has been attempted.
func (r *Resolver) Outcome() Outcome {
	if r == nil {
		return ""
	}

	return r.outcome
}

// DeferralOutcome is Outcome as far as relabeling the upgrade's reason goes: the
// read outcome, except that a miss whose source stat is what failed reports "" --
// that miss did defer this resume, but must not be labeled as a deferral.
//
// It differs from Outcome for one case, and that case is why it exists: a miss
// whose source stat is what failed. That resume did not upgrade, and it is a miss
// to the reads counter, but it is not the cache working as designed on a node that
// has not warmed. The plain miss is answered by a warm, which clears it as soon as
// one is allowed to run and the bytes are usable; this one starts no warm at all, so
// nothing this resume sets in motion will clear it -- only the target becoming
// readable again does. Relabeling it as a deferral would put that behind
// the self-clearing label, which is the fold GatedReason exists to prevent, so the
// upgrade keeps getversion_failed and logs it per occurrence.
func (r *Resolver) DeferralOutcome() Outcome {
	if r == nil || r.sourceUnreadable {
		return ""
	}

	return r.outcome
}

// SourcePath returns the path to read the binary at srcPath from, and whether it
// is safe to read at all.
//
// The copy is re-checked rather than assumed, because a caller reads the bytes as
// a separate step from resolving them, and in between the copy can be evicted or
// deleted as superseded by a warm that saw a promotion.
//
// ok is false when the copy that carried the resolved version is gone. There is
// deliberately no fall back to the source: that would be a 13 MB read off the
// mount on the resume path, and it would deliver bytes whose version was never
// verified against them. A skipped upgrade is better than either, and the next
// resume retries.
func (r *Resolver) SourcePath(ctx context.Context, srcPath string) (string, bool) {
	if r == nil {
		// No resolver at all: the flag is off, so the caller reads the source
		// exactly as it always did. This is the ONLY case in which this function
		// hands back the source.
		return srcPath, true
	}

	e := r.EntryFor(srcPath)
	if e == nil {
		// An engaged resolver holding no entry for this path must not fall back to
		// the source. Doing so would read the mount on the resume path and deliver
		// bytes whose version was never verified -- the two things this package
		// exists to prevent -- and it would do so silently, since the caller cannot
		// tell a cached path from a source path by looking at it.
		//
		// It is unreachable while the upgrade resolves the version exactly once, on
		// the string it later delivers: a lookup that hit at resolve time cannot
		// miss here. A test pins that invariant, because nothing else does.
		r.countDelivery(ctx, OutcomeStale)

		return "", false
	}
	if _, err := os.Stat(e.LocalPath); err == nil {
		r.countDelivery(ctx, OutcomeCopy)

		return e.LocalPath, true
	}

	r.countDelivery(ctx, OutcomeStale)

	return "", false
}
