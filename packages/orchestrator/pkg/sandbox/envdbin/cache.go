// Package envdbin keeps a node-local copy of the host envd binary so the
// resume-time envd upgrade paths neither fork `envd -version` nor stream 13 MB
// off the gcsfuse mount on every attempt.
//
// Nothing on the resume path ever waits for that copy, and nothing on it reads
// BYTES off the mount. Lookup is at most two stats and a map read; a miss warms
// the cache in the BACKGROUND and tells the caller to DEFER the upgrade to a
// later resume. The one miss that starts no warm is the one whose own source stat
// failed: a warm would open by re-taking it, so that failure is reported and
// scheduled where it happened.
//
// Deferring is the one thing the cache-off path never does, and it is deliberate:
// reading the source instead would put back the cost this package exists to
// remove, on a path a customer is waiting on. The upgrade is idempotent and
// re-fires on every resume, so a deferral costs one cycle. That is the whole
// reason the design is shaped this way: a copy on the critical path would mean a
// cold, slow or wedged mount delaying a resume, and no budget can fix that — a
// read already blocked in the kernel is not interruptible from userspace, so a
// deadline bounds a copy that is progressing slowly and not one that is stuck.
// Moving the copy off the path removes the class instead of bounding it.
//
// Lookup's source stat is on the mount, and deliberately so: the cache key
// is the source's identity, which is also how a promotion becomes visible, so a
// resume necessarily stats it. That is one metadata round trip against a
// TTL-cached mount, against the demand-paged exec it replaces — and it is not
// bounded either, for the same reason as above, so a wedged mount can still hold
// a resume's lookup. What moving the copy off the path removes is the unbounded
// work: the exec and the 13 MB read.
//
// The source (/fc-envd/envd, or envd.<sha> beside it) lives on a read-only
// gcsfuse mount. Promotion overwrites the object in place (gcloud storage cp
// envd.<sha> -> envd), so the path never changes and the identity of the bytes
// has to come from size and mtime — and since two builds can be byte-identical
// in length, mtime is the load-bearing half of that key.
//
// What stat-based invalidation guarantees, precisely: this mount is the only one
// of the four created WITHOUT the shared gcsfuse config, and that config sets
// metadata-cache ttl-secs: -1 (infinite). So its metadata cache is gcsfuse's
// bounded default rather than an infinite one, and a promotion becomes visible
// here within that TTL — not instantly. The delay is benign: resumes in that
// window resolve the pre-promotion binary and skip the upgrade as same_version,
// and the next resume after it picks the new one up, because the upgrade is
// idempotent and re-fires per resume.
//
// The dependency worth stating: if /fc-envd is ever normalised onto the shared
// config, stat would stop observing promotions altogether and this cache would
// serve the pre-promotion binary indefinitely. Do not point it at a mount
// configured with an infinite metadata TTL.
package envdbin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// meter is the package's own, so the cache and the resolver report through one
// instrumentation scope.
var meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envdbin")

// warms counts how a warm of the source ended, including the ones that declined
// to copy anything and the miss that is answered without a warm at all, when the
// source could not be stat-ed (noteSourceUnreadable). Those belong on the same
// series because they are the same question -- is this node getting the binary
// local -- and because that miss is the one a warm used to report itself. The
// warmer is off every critical path,
// so without this its health is invisible: a node whose cache directory is
// broken would serve every resume from the mount and look no different from one
// whose cache is working.
var warms = utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorEnvdBinaryCacheWarms))

const (
	// maxEntries bounds the cache to the paths that can be in play at once: the
	// live and offline upgrade targets resolve from independent flags, so two is
	// the expected working set during a ramp. The rest is headroom for a
	// transitional flip, and caps the directory at ~4 x 13 MB permanently.
	// Deliberately not configurable — nothing plausible wants to tune it, and a
	// knob is easier to add later than to remove.
	maxEntries = 4

	// tempPrefix marks in-progress copies, which the sweep must leave alone.
	tempPrefix = ".tmp-"

	// warmBudget bounds one background warm: the copy, the structural check, the
	// probe and the freshness re-check — plus the source stat in the boot-warm
	// shape, which has to take its own, where a resume's warm is handed the stat
	// its caller already took. Nothing waits on any of it, so this is not a latency
	// guarantee — it exists so a slow mount cannot retain a warm goroutine
	// indefinitely.
	//
	// One budget covers them all rather than one each, so a copy that finishes near
	// the edge leaves the stages after it to be cut short. That is deliberate and
	// costs only a cycle: a stage the deadline stopped is scheduled and never
	// pinned, because being stopped is not a verdict about the bytes (see
	// probeVerdictIsAboutTheBytes). The structural check is the one stage the
	// deadline cannot interrupt, since it takes no context, so it spends budget the
	// stages after it then go without.
	//
	// No stage needs a number of its own. The structural check and the probe work
	// on the local copy, and the freshness re-check — the one that stats the mount,
	// and so the one that could otherwise stall for the life of the process — is
	// already released on this shared deadline by statSourceBounded.
	warmBudget = 30 * time.Second

	// backoffBase and backoffCeiling schedule retries of a failure that may clear
	// on its own -- an unwritable directory, a spent copy budget, a read error from
	// the mount. A miss recurs on every resume, so without a schedule a node whose
	// cache directory is broken re-reads the whole source in the background for
	// every resume it serves.
	//
	// The ceiling is what bounds that: one minute turns a per-resume retry into one
	// attempt per node per minute, which is where the background volume stops
	// competing with the reads a resume waits on. The base is what keeps a
	// transient fault from costing an upgrade cycle -- short enough to be
	// invisible at the rate one sandbox is resumed, long enough that many resumes
	// pass between attempts. A success clears the record, so a node that recovers
	// is immediately back to normal rather than serving out a window.
	//
	// A target that cannot RUN is not scheduled at all: see markBadIdentity.
	backoffBase    = time.Second
	backoffCeiling = time.Minute

	// copyChunk is the granularity at which a copy checks its deadline.
	copyChunk = 1 << 20
)

// errCacheUnusable labels a failure of the cache directory itself, as opposed to
// a failure of the source or of the deadline, so a log line says which side was
// at fault. It does not change the retry policy: every failure that might clear
// on its own arms the same schedule, source-side included (noteFailure). Only a
// verdict about the bytes pins an identity, and this is not one.
var errCacheUnusable = errors.New("envd binary cache directory is unusable")

// errWarmSuppressed marks a warm that did not attempt a copy because the backoff
// from an earlier failure is still in force. It is not a failure: the
// backoff exists so a node whose cache directory is broken does not re-read the
// source on every resume, and a miss recurs on every resume, so reporting these
// as failures would flood the warm metric with the backoff working as intended --
// indistinguishable from the warmer actually being broken.
var errWarmSuppressed = errors.New("envd binary cache is in its post-failure backoff")

// errBadTarget marks a target this node will not serve, on the strength of the
// bytes themselves rather than anything about this node or this moment. It has two
// entry points, and they judge different things:
//
//   - the copy is not a complete image -- errBadImage, reached WITHOUT executing
//     anything, so an incomplete artifact never runs on the host;
//   - the copy is a complete image that the loader refused, or that faulted on its
//     own instructions. Evidence from OUTSIDE the image, both of them: an ordinary
//     nonzero exit is deliberately not one -- see probeVerdictIsAboutTheBytes.
//
// Either way the target is not usable, which is what this value means. Do not read
// it as "the image ran and failed": on the first path it is never exec'd, and on
// the second a loader refusal means execution never began.
//
// It is separated from a generic failure for two reasons. It must stop being
// retried, since re-copying an unusable binary wastes 13 MB of mount reads per
// resume forever. And it must be loud: with the cache on, the resolver never probes
// the source, so this is the ONLY place a broken promotion is detectable. Reported
// as binary_not_cached at the call sites, it is indistinguishable from a node that
// merely has not warmed yet.
var errBadTarget = errors.New("envd binary target is not usable")

// errBadIdentityKnown marks a warm that declined to copy because this exact
// binary -- same path, same size, same mtime -- has already been judged unusable.
// It is a refusal rather than a failure: the first encounter reported errBadTarget
// and logged, and re-reporting the same verdict on every subsequent resume would
// turn one broken promotion into a permanent stream of failures.
//
// It reports its own result, warmPinned, and not the schedule's warmSuppressed,
// because the two are opposites: a suppression lapses within the minute, while
// this lasts until a promotion changes the identity. Reported as one value, the
// condition that does not clear on its own would read as the one that clears
// fastest.
var errBadIdentityKnown = errors.New("envd binary target is already known to be unusable")

// errNoCache is what a nil Cache reports instead of a stat error: there is no
// cache to look in, which is a caller built without one rather than anything
// about the source.
var errNoCache = errors.New("no envd binary cache")

// errWarmInFlight marks a warm that did not start because another warm of the
// same bytes is already in flight. Nothing has been judged and nothing is wrong:
// the copy the holder is making is the copy this warm would have made.
//
// It is counted rather than dropped because it is the only symptom of a source
// that stopped answering mid-copy. The slot is held until the read returns, and a
// read blocked in the kernel does not return, so warmAlreadyWarming rising while
// warmOK stays flat says the slot is occupied by a copy that is not finishing --
// which no other series can distinguish from a node that has simply not warmed
// yet.
var errWarmInFlight = errors.New("a warm of this envd binary is already in flight")

// errWarmPanicked marks a warm that panicked. Contained rather than fatal: the
// warm runs on its own goroutine, debug/elf is documented as not hardened against
// malformed input, and the structural check parses every copy -- so an
// unrecovered panic there would take the orchestrator down with every sandbox on
// the node, and the boot warm would then parse the same bytes at the same path
// and die again.
//
// Scheduled, never pinned. A panic is not evidence about the artifact: a valid
// image whose shape the parser mishandles, or a regression in the parser itself,
// produces one from bytes that would exec perfectly -- and a pin would condemn
// that artifact on every node that deploys the parser.
var errWarmPanicked = errors.New("envd binary warm panicked")

// errSuperseded marks a copy that was made and probed successfully but describes
// an identity the source has already moved past. Not a failure: nothing is wrong
// with this node, and the next miss carries the new identity.
var errSuperseded = errors.New("envd binary copy describes a superseded generation")

// errBadImage marks a copy that is not a complete ELF image. It owns exactly one
// layer of the verdict: structure, judged without executing anything.
//
// Two things about its scope are worth knowing, because both are easy to overstate.
// It is STRICTER than the loader, not equivalent to it: envd's shipped binary ends
// in .shstrtab, the section-name strings, which elf.NewFile reads eagerly and which
// sit in the ~2 KB past the last PT_LOAD that the loader never maps. So a copy one
// byte short of the source fails here and would have run to completion -- measured,
// the whole tolerated window is that tail rounded up to a page. That is still the
// right answer for an artifact that arrived incomplete, but it means this value
// says "incomplete", never "will not run". And it catches only incompleteness:
// corruption in the middle of an otherwise whole file parses cleanly here, and
// whether anything catches it depends on how it dies -- measured on a 2.4 MB Go
// binary, 4 KB of 0xff mid-file parses and then faults, which the probe reads as a
// verdict, while 4 KB of zeros parses and then exits nonzero, which is merely
// scheduled. A corrupt-but-complete image that exits cleanly is caught by nothing
// here; the version it reports is what the upgrade then acts on.
var errBadImage = errors.New("envd binary copy is not a complete image")

// The results a warm can report. The first two are the successful ones, and they
// come back from the warm itself rather than being re-derived with a Lookup,
// whose source stat would run inside the warming slot.
const (
	warmOK             = "ok"
	warmSuperseded     = "superseded"
	warmSuppressed     = "suppressed"
	warmPinned         = "pinned"
	warmAlreadyWarming = "already_warming"
	warmBadTarget      = "bad_target"
	warmFailed         = "failed"
)

// Outcome is how a Lookup was served, for the caller's metrics.
type Outcome string

const (
	// OutcomeHit served an existing local copy: no read of the source beyond the
	// stat that validated it.
	OutcomeHit Outcome = "hit"
	// OutcomeCopy is a caller reading the cached copy, which is the point.
	OutcomeCopy Outcome = "copy"
	// OutcomeMiss found nothing cached. The upgrade is deferred to a later resume
	// and a warm runs in the background; the source is deliberately not read, since
	// that is the mount cost this cache exists to keep off the resume path.
	OutcomeMiss Outcome = "miss"
	// OutcomeStale is a caller refused: the copy that carried the resolved version
	// is gone, and the source is not an alternative -- reading it would be both a
	// mount read on the resume path and bytes whose version was never verified.
	OutcomeStale Outcome = "stale"
)

// readOutcomes and deliveryOutcomes are the vocabularies the two counters emit,
// and warmResults those of the third. They exist so a test can hold the published
// metric descriptions to them: a description that no longer names what the code
// emits fails nothing at runtime -- the counter still records, and the dashboard
// reader is simply told the wrong vocabulary. Add a value here when you add one
// to the code, and the test says which description has not caught up.
var (
	readOutcomes     = []Outcome{OutcomeHit, OutcomeMiss}
	deliveryOutcomes = []Outcome{OutcomeCopy, OutcomeStale}
	warmResults      = []string{
		warmOK, warmSuperseded, warmSuppressed, warmPinned,
		warmAlreadyWarming, warmBadTarget, warmFailed,
	}
)

// Entry is an immutable resolution of one source binary: a path to read the
// bytes from, and the version baked into those same bytes. Version is probed
// from LocalPath, not from the source, so the two can never describe different
// files.
type Entry struct {
	// SourcePath is the host path the entry was resolved from.
	SourcePath string
	// LocalPath is where to read the binary: always the cached copy, since an
	// unavailable cache answers with a miss rather than a source-backed entry.
	LocalPath string
	// Version is the version baked into the binary at LocalPath.
	Version string

	size    int64
	modTime time.Time
}

// Cached reports whether the entry is backed by a local copy rather than the
// source itself. Every published entry is, so this exists to keep a future
// non-copying entry from being revalidated as though its file were local.
func (e *Entry) Cached() bool { return e.LocalPath != e.SourcePath }

// Cache resolves host envd binaries to local copies. It is safe for concurrent
// use and is sized for a handful of entries: at full ramp the resume path calls
// Lookup on every attempt, so the hit path must not READ the source -- one stat
// aside, which is the key (see the package comment).
type Cache struct {
	dir string
	// probe reads the version baked into the binary at a path. Injected so tests
	// can count invocations without exec'ing a real binary.
	probe func(context.Context, string) (string, error)
	// budget bounds one background copy-and-probe. Zero means warmBudget, so a
	// Cache built any other way behaves as production does; tests set it short
	// because the wedge cases this package cares about are only observable by
	// waiting one out, and 30s of waiting per case is not a unit test.
	budget time.Duration
	// statSrc stats a source binary. It is os.Stat in production and injectable in
	// tests, which is the only way to model a mount that stalls in metadata. Read
	// through statSource, which falls back to os.Stat, so the field's existence
	// cannot change how a Cache behaves in production.
	statSrc func(string) (os.FileInfo, error)

	// reapTemps clears temporary files left by a previous process, once per
	// cache. It cannot run as part of the regular sweep: that would race a
	// concurrent copy's own temporary file.
	reapTemps sync.Once

	mu      sync.Mutex
	entries map[string]*Entry
	// order is insertion order, for eviction. Recency is deliberately not
	// tracked: with an expected working set of two, an LRU's ordering would
	// never be exercised.
	order []string
	// inFlight holds destination paths that a copy has renamed into place but
	// not yet published as an entry. The sweep must spare them: a file in that
	// window belongs to no entry, so an unrelated store's sweep would otherwise
	// delete it out from under the probe about to exec it, failing a resolution
	// that has nothing to do with the store.
	inFlight map[string]int
	// warming holds one slot per binary identity with a background warm in flight,
	// against the time it was claimed. A miss recurs on every resume until a warm
	// lands, so without this every one of them spawns a goroutine that then blocks
	// inside the flight -- unbounded in the resume rate -- and every one of them
	// reports the shared read as its own warm, inflating the counter by the number
	// of followers rather than counting reads.
	//
	// Keyed on the identity, which is the key the flight uses, and not on the path.
	// A read blocked in the kernel cannot be interrupted from userspace, so a warm
	// of a wedged source never returns to release its slot: keyed on the path, that
	// one stuck copy would hold the path for the life of the process and no
	// promotion of it could ever warm. Keyed on the identity, the promotion -- the
	// event that makes warming urgent -- presents a new key and proceeds.
	warming map[string]time.Time
	// backoff holds, per source path, the defence against repeating a failure
	// pointlessly. Keyed on the source path because every other structure here is:
	// one Cache serves both upgrade targets, which resolve from independent flags,
	// so a defence held cache-wide would let an unrunnable binary named by one flag
	// suppress warms of a perfectly good binary named by the other -- and both
	// states report binary_not_cached at the call sites, making the starvation
	// indistinguishable from a cold node.
	backoff map[string]*pathBackoff

	// load deduplicates concurrent misses. A busy node can see many resumes at
	// once, so an undeduplicated miss is a thundering herd of reads of the same
	// file.
	load singleflight.Group
}

// NewCache returns a cache that stores copies in dir, probing versions with
// probe. It touches no filesystem: dir is created by the first Warm that
// needs it, so constructing a cache that is never used (an unconfigured
// directory in a unit test, a node with the feature off) has no side effects.
func NewCache(dir string, probe func(context.Context, string) (string, error)) *Cache {
	return &Cache{
		dir:      dir,
		probe:    probe,
		statSrc:  os.Stat,
		entries:  make(map[string]*Entry),
		inFlight: make(map[string]int),
		warming:  make(map[string]time.Time),
		backoff:  make(map[string]*pathBackoff),
	}
}

// pathBackoff is one source path's defence against repeating a failure. The two
// halves answer failures of opposite character, which is why they are not one
// mechanism: a schedule for what may clear, an identity for what cannot.
type pathBackoff struct {
	// failures counts consecutive schedulable failures, and until is when the next
	// attempt is allowed. Cleared by a success.
	failures int
	until    time.Time

	// badSize and badModTime identify a binary this node has judged unusable: its
	// copy was not a complete image, or it landed intact and its probe returned a
	// verdict about the bytes -- see probeVerdictIsAboutTheBytes, which is what
	// keeps a failure of this NODE from reaching here at all. Either way the
	// verdict is about the bytes and not about this moment, so no duration is the
	// right answer -- the identity is simply not retried. Recovery needs no window:
	// a promotion changes mtime, which is a different identity, which is warmed on
	// the next miss.
	badSize    int64
	badModTime time.Time
	hasBad     bool

	// warnedAt rate-limits the log line for a path that keeps failing, so a fault
	// that recurs on every resume costs one line per window rather than a flood.
	warnedAt time.Time
}

// statSourceBounded stats the source without letting a wedged mount retain the
// caller. os.Stat takes no context, and a metadata lookup blocked in the kernel
// is not interruptible from userspace, so the call is made on its own goroutine
// and abandoned when the deadline fires. Abandoning it leaks that goroutine for
// as long as the kernel takes, which on a wedged mount is the life of the
// process. What is bounded is how many can exist: at most one per stat a warm
// takes, and at most one warm per identity is in flight. That is the trade -- the
// caller is released either way, so the node keeps warming instead of becoming a
// silent permanent miss.
func (c *Cache) statSourceBounded(ctx context.Context, path string) (os.FileInfo, error) {
	type result struct {
		fi  os.FileInfo
		err error
	}
	// Buffered, so the abandoned goroutine can always finish and exit.
	done := make(chan result, 1)
	go func() {
		fi, err := c.statSource(path)
		done <- result{fi: fi, err: err}
	}()

	select {
	case r := <-done:
		return r.fi, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// validateImage reports whether the copy at path is a complete ELF image of
// exactly size bytes.
//
// This is the structural layer, and its scope is exactly incompleteness. A parse
// failure is a verdict about the bytes reached without executing them, which is
// what makes a TRUNCATED promotion deterministic rather than inferred from how a
// process died -- and it means a truncated artifact is never exec'd on the host.
// That absolute does not extend further: corruption inside an otherwise whole file
// parses cleanly here and reaches the probe, which is the layer that judges it.
//
// Reaching the bytes is separated from judging them, and reading the whole copy
// before parsing it is what makes that separation structural rather than a
// taxonomy of error types. Any failure to open or read the copy is about THIS
// NODE -- fd exhaustion, a directory that went away, a bad block on the device
// holding the cache -- and must not be read as a verdict about a source that is
// intact everywhere else. The parser is then handed a complete buffer, so every
// error it returns is a statement about the artifact. size comes from the
// identity being warmed and the copy was verified against it, so a short read is
// local damage rather than a truncated source, and is attributed accordingly.
func validateImage(path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open envd binary copy %q: %w", path, err)
	}
	defer f.Close()

	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return fmt.Errorf("read envd binary copy %q: %w", path, err)
	}

	if _, err := elf.NewFile(bytes.NewReader(buf)); err != nil {
		return fmt.Errorf("%w: %q: %w", errBadImage, path, err)
	}

	return nil
}

// warmBudget is the deadline one background warm runs under.
func (c *Cache) warmBudget() time.Duration {
	if c.budget > 0 {
		return c.budget
	}

	return warmBudget
}

func (c *Cache) statSource(path string) (os.FileInfo, error) {
	if c.statSrc == nil {
		return os.Stat(path)
	}

	return c.statSrc(path)
}

// Lookup returns the cached entry for srcPath, if a live one exists.
//
// Deliberately cheap: at most two stats and a map read -- the copy's own stat is
// taken only when an entry matched, so a miss is one -- never a copy and never a
// probe, so the unbounded work this cache exists to remove -- exec'ing the binary off
// the mount, and streaming it -- is not reachable from here. The source stat is
// the one thing that does touch the mount, by design (see the package comment);
// a wedged mount can still hold it.
func (c *Cache) Lookup(srcPath string) (*Entry, bool) {
	// A stat that failed yields no entry, so the error needs no separate test.
	e, _, _ := c.lookupSource(srcPath)

	return e, e != nil
}

// lookupSource is Lookup, additionally returning the source stat it took and the
// error if it could not take one.
//
// A caller that misses and then triggers a warm needs the identity of the bytes
// to key that warm on, and this is the stat that establishes it -- so handing it
// down is what keeps the resume path at one metadata op instead of two. The error
// is handed back for the same reason: a caller whose own stat has just failed
// knows something no warm could discover more cheaply, since a warm would start
// by taking that same stat.
func (c *Cache) lookupSource(srcPath string) (*Entry, os.FileInfo, error) {
	// A nil cache is a caller built without one; a miss is the honest answer.
	if c == nil {
		return nil, nil, errNoCache
	}

	fi, err := c.statSource(srcPath)
	if err != nil {
		return nil, nil, fmt.Errorf("stat envd binary %q: %w", srcPath, err)
	}

	return c.lookup(srcPath, fi), fi, nil
}

// noteSourceUnreadable records a source a caller could not stat on its own
// lookup, and is the answer to that miss instead of a warm.
//
// A warm would begin by taking the stat that has just failed, so spawning one
// re-asks a mount that has already answered -- once per resume, since a miss
// recurs on every resume, and each of those goroutines would be spawned before
// any identity existed to key a slot on. Reporting it here instead costs no
// goroutine and no second metadata op, and arms the same schedule a failed warm
// would, so the next resumes stop asking at all.
func (c *Cache) noteSourceUnreadable(ctx context.Context, srcPath string, cause error) {
	if c == nil || c.dir == "" || srcPath == "" {
		return
	}

	if c.scheduleSuppressed(srcPath) {
		c.reportWarm(ctx, srcPath, "", errWarmSuppressed)

		return
	}

	c.noteFailure(srcPath)
	c.reportWarm(ctx, srcPath, "", cause)
}

// WarmAsync makes sure srcPath is cached, in the background, and returns
// immediately. Concurrent calls for the same binary collapse into one read.
//
// ctx contributes its values -- trace and resource attributes, so the warm's
// telemetry is attributable -- but not its cancellation: the work is shared and
// outlives whichever resume happened to notice the miss.
func (c *Cache) WarmAsync(ctx context.Context, srcPath string) {
	c.warmAsyncFor(ctx, srcPath, nil)
}

// warmAsyncFor is WarmAsync for a caller that has already stat-ed the source, so
// the identity of the bytes to warm is known before the goroutine starts.
//
// fi may be nil, and the two shapes differ in one thing only: where the warming
// slot is claimed. With an identity in hand the slot is claimed BEFORE the
// goroutine, which is what stops a recurring miss spawning one goroutine per
// resume. Without one the stat has to happen inside the goroutine -- os.Stat
// honours no deadline, so it cannot run on a caller's path -- and the slot can
// only be claimed after it.
//
// That second shape is the boot warm and nothing else, so it cannot pile up: it
// runs once per path per process. A resume never reaches it, even when its own
// stat failed -- that is noteSourceUnreadable's case, precisely because a warm
// with no identity could not be collapsed against the resumes behind it.
func (c *Cache) warmAsyncFor(ctx context.Context, srcPath string, fi os.FileInfo) {
	if c == nil || c.dir == "" || srcPath == "" {
		return
	}

	// Stripped of cancellation here, before the goroutine, so the deadline below
	// is the only thing that can stop it.
	ctx = context.WithoutCancel(ctx)

	// The schedule before anything else, and before a goroutine exists to do it:
	// a path inside its schedule is not going to be copied, so it must cost
	// neither a metadata lookup on the mount the schedule exists to protect nor a
	// goroutine per resume. The check is identity-free precisely so it can run
	// here, ahead of the stat this shape would otherwise take to establish one.
	// It does not spare the caller's own lookup, which has already stat-ed the
	// source by the time a miss gets here; what it spares is every stat after
	// that. warmWith checks it again for the blocking caller, and for a schedule
	// armed in between.
	if c.scheduleSuppressed(srcPath) {
		c.reportWarm(ctx, srcPath, "", errWarmSuppressed)

		return
	}

	if fi != nil {
		// At most one warm of these bytes in flight. The singleflight inside
		// warmWith already collapses the READ, but not the goroutines waiting on
		// it, nor their reports of it.
		if ok, held := c.beginWarm(srcPath, fi); !ok {
			c.reportWarm(ctx, srcPath, "", fmt.Errorf("%w: held for %s", errWarmInFlight, held.Round(time.Millisecond)))

			return
		}
	}

	go func() {
		// Bounded only so a slow mount cannot retain this goroutine forever;
		// nothing is waiting on it, so this is not a latency guarantee.
		ctx, cancel := context.WithTimeout(ctx, c.warmBudget())
		defer cancel()

		id := fi
		if id == nil {
			var err error
			if id, err = c.statSourceBounded(ctx, srcPath); err != nil {
				// Scheduled like any other failure that may clear -- the same answer
				// warm gives for its own stat, since this is that stat, moved earlier
				// so the slot below can be keyed on what it returns.
				c.noteFailure(srcPath)
				c.reportWarm(ctx, srcPath, "", fmt.Errorf("stat envd binary %q: %w", srcPath, err))

				return
			}

			if ok, held := c.beginWarm(srcPath, id); !ok {
				c.reportWarm(ctx, srcPath, "", fmt.Errorf("%w: held for %s", errWarmInFlight, held.Round(time.Millisecond)))

				return
			}
		}
		defer c.endWarm(srcPath, id)

		result, err := c.warmWith(ctx, srcPath, id)
		c.reportWarm(ctx, srcPath, result, err)
	}()
}

// reportWarm records how a background warm ended, on the counter and -- for the
// states an operator has to be able to see -- in the log.
//
// "ok" has to mean the binary is cached now, not merely that nothing went wrong: a
// promotion during the warm leaves the copy describing a superseded generation,
// which publication correctly drops, and reporting that as ok would make the
// metric say the cache is populated when it is not. That verdict comes back from
// the warm itself rather than from a Lookup here, whose source stat would run
// inside the warming slot.
func (c *Cache) reportWarm(ctx context.Context, srcPath, result string, err error) {
	if err != nil {
		result = warmResult(err)
	}
	warms.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))

	if err == nil {
		return
	}

	// A node that cannot warm keeps serving resumes and quietly stops upgrading
	// envd, which is the one state where this cache is worse than no cache -- so
	// every cause earns a line, not just the ones that look diagnosable. What makes
	// that affordable is the rate limit: the failure recurs on every resume, so
	// shouldWarn allows one line per path per window. A warm that declined to try
	// is not a failure and stays at Debug; a newly discovered bad target bypasses
	// the limit, because it is reported once per identity in the first place.
	switch {
	case errors.Is(err, errWarmSuppressed), errors.Is(err, errBadIdentityKnown), errors.Is(err, errWarmInFlight):
		logger.L().Debug(ctx, "envd binary cache: warm declined to try",
			zap.String("path", srcPath), zap.String("result", result), zap.Error(err))
	case errors.Is(err, errBadTarget) || c.shouldWarn(srcPath):
		logger.L().Warn(ctx, "envd binary cache: cannot warm; this node will not upgrade envd until it succeeds",
			zap.String("path", srcPath), zap.String("result", result), zap.Error(err))
	default:
		logger.L().Debug(ctx, "envd binary cache: warm did not complete",
			zap.String("path", srcPath), zap.String("result", result), zap.Error(err))
	}
}

// Warm copies srcPath into the cache and probes the copy's version, so a later
// Lookup hits. It blocks; production always goes through WarmAsync, so the only
// caller that waits is a test. Concurrent warms of the same binary collapse into
// one read.
//
// A failed warm publishes no entry, so nothing wrong is ever served. It is
// remembered, though: a failure that may clear arms the retry schedule, and a
// verdict about the bytes pins the identity -- see pathBackoff.
func (c *Cache) Warm(ctx context.Context, srcPath string) error {
	// A nil cache is a caller built without one, as Lookup and WarmAsync also
	// accept: nothing to warm, and no error to report for not having tried.
	if c == nil {
		return nil
	}

	_, err := c.warm(ctx, srcPath)

	return err
}

// warm is Warm, additionally reporting how it left the cache: "ok" when the
// binary is cached when it returns, "superseded" when the copy it made described a
// generation the source had already moved past. WarmAsync needs that distinction
// for its metric and must not ask the mount for it -- see the comment there.
func (c *Cache) warm(ctx context.Context, srcPath string) (string, error) {
	// Before the stat, not after: a path inside its schedule is not going to be
	// copied, and it should not cost a metadata lookup on the mount this schedule
	// exists to protect. The check is identity-free precisely so it can run here.
	if c.scheduleSuppressed(srcPath) {
		return "", errWarmSuppressed
	}

	// Bounded because os.Stat honours no deadline: an unbounded call here would
	// hold this caller until the kernel answered, which on a wedged mount is
	// never. The boot warm takes this stat before its slot exists, so what a wedge
	// costs there is one goroutine, not a path that can never warm; a resume's
	// warm never reaches it, since its caller hands the stat down.
	fi, err := c.statSourceBounded(ctx, srcPath)
	if err != nil {
		// Scheduled like any other failure that may clear. A miss re-triggers a warm
		// on every resume, so a mount that cannot answer its metadata would otherwise
		// spawn one per resume and flood warms{failed} -- the flood the schedule
		// exists to stop, arriving through the one path that was not arming it.
		c.noteFailure(srcPath)

		return "", fmt.Errorf("stat envd binary %q: %w", srcPath, err)
	}

	return c.warmWith(ctx, srcPath, fi)
}

// warmWith is warm for one already-resolved identity of srcPath: the caller has
// stat-ed the source and hands the result down, so nothing here asks the mount
// for metadata a resume has just paid for.
//
// A stale fi needs no special handling. The copy re-checks the source when it
// comes up short, and publication re-checks it again, so a generation the source
// has already moved past is abandoned as superseded rather than published or
// judged.
func (c *Cache) warmWith(ctx context.Context, srcPath string, fi os.FileInfo) (result string, err error) {
	// A panic is contained here rather than being allowed to reach the goroutine
	// this runs on, where it would take the orchestrator down with every sandbox on
	// the node -- and, since the boot warm parses the same bytes at the same path,
	// take the restarted process down too. debug/elf is documented as not hardened
	// against malformed input and the structural check parses every copy, so the
	// containment is not hypothetical. This also catches the re-panic singleflight
	// raises in a flight's followers.
	//
	// Scheduled, not pinned: see errWarmPanicked.
	defer func() {
		if r := recover(); r != nil {
			c.noteFailure(srcPath)
			logger.L().Error(ctx, "envd binary cache: warm panicked",
				zap.String("path", srcPath), zap.Any("panic", r), zap.Stack("stack"))

			result, err = "", fmt.Errorf("%w: %q: %v", errWarmPanicked, srcPath, r)
		}
	}()

	if c.scheduleSuppressed(srcPath) {
		return "", errWarmSuppressed
	}

	// Already cached: the binary is present when this returns, which is what "ok"
	// claims, so there is nothing to copy and nothing to report as superseded.
	if e := c.lookup(srcPath, fi); e != nil {
		return warmOK, nil
	}

	// These exact bytes have already been judged unusable. Copying them again cannot
	// change that; a promotion would present a different identity and fall through
	// to the copy below.
	if c.identityKnownBad(srcPath, fi) {
		return "", errBadIdentityKnown
	}

	// Keyed on the identity of the bytes, not just the path: a promotion landing
	// mid-flight must start its own warm rather than join the one carrying the
	// pre-promotion binary.
	res, err, _ := c.load.Do(identityKey(srcPath, fi), func() (any, error) {
		// Re-check inside the flight: a caller that missed can arrive here just
		// after the previous flight retired, and would otherwise re-read a source
		// the flight before it already cached.
		if e := c.lookup(srcPath, fi); e != nil {
			return warmOK, nil
		}

		return c.copyAndProbe(ctx, srcPath, fi)
	})
	if err != nil {
		return "", err
	}

	// Every follower of a shared flight reports what the flight achieved, which is
	// the same for all of them.
	result, _ = res.(string)

	return result, nil
}

// copyAndProbe copies the source in and probes the copy, publishing an entry. It
// reports whether the cache holds the binary when it returns (warmOK) or whether
// the copy described a generation the source had already moved past.
func (c *Cache) copyAndProbe(ctx context.Context, srcPath string, fi os.FileInfo) (string, error) {
	localPath, err := c.copyIn(ctx, srcPath, fi)
	if err != nil {
		// A copy the source outran is exactly what publication would have declined,
		// reported the same way and without having judged the target on the strength
		// of a partial image.
		if errors.Is(err, errSuperseded) {
			c.noteSuccess(srcPath)

			return warmSuperseded, nil
		}

		return "", err
	}
	// Published below; until then the sweep must not treat it as an orphan.
	defer c.releaseInFlight(localPath)

	// Before executing it. A structural verdict is deterministic where the probe's
	// is inferred from how the process died, so this catches the fault the probe
	// can only guess at -- and catches it without running the bytes.
	if err := validateImage(localPath, fi.Size()); err != nil {
		os.Remove(localPath)

		if errors.Is(err, errBadImage) {
			c.markBadIdentity(srcPath, fi)

			return "", fmt.Errorf("%w: %q: %w", errBadTarget, srcPath, err)
		}

		c.noteFailure(srcPath)

		return "", err
	}

	// Probe the copy, never the source: it proves the copy is complete and
	// executable, and it makes the reported version and the delivered bytes the
	// same file by construction. Probing the source would leave a window where a
	// promotion between probe and delivery ships a binary whose version was never
	// the one recorded.
	version, err := c.probe(ctx, localPath)
	if err == nil && version == "" {
		// A copy that exits 0 and names no version is not usable, and publishing it
		// would pin "no version" to this generation: lookup revalidates size, mtime
		// and existence only, so every later resume would hit, resolve to "", and
		// report getversion_failed with no probe and no way to clear it short of a
		// promotion. envd prints its version unconditionally, so this means the bytes
		// at the envd path are not envd.
		err = fmt.Errorf("probe of %q exited 0 without naming a version", localPath)
	}
	if err != nil {
		// An unprobeable copy must never be published, whatever killed the probe.
		os.Remove(localPath)

		// Only a verdict about the BYTES may stop this identity being retried, and
		// that is asked as an allowlist rather than as a list of exceptions. The two
		// mistakes are not symmetric: scheduling a genuinely dead target costs a
		// bounded retry loop that the ceiling below already caps, while pinning a
		// good binary stops this node upgrading envd on BOTH paths until the next
		// promotion or the next deploy -- and reports it as a corrupt artifact. So
		// the default is to schedule, and pinning requires positive evidence.
		if !probeVerdictIsAboutTheBytes(ctx, err) {
			c.noteFailure(srcPath)

			return "", fmt.Errorf("probe envd binary copy %q: %w", localPath, err)
		}

		c.markBadIdentity(srcPath, fi)

		return "", fmt.Errorf("%w: %q: %w", errBadTarget, srcPath, err)
	}

	// Publication can decline, and the two reasons it declines are not the same
	// event, so its verdict is not assumed here.
	switch err := c.store(ctx, &Entry{
		SourcePath: srcPath,
		LocalPath:  localPath,
		Version:    version,
		size:       fi.Size(),
		modTime:    fi.ModTime(),
	}); {
	case err == nil, errors.Is(err, errSuperseded):
		// Either the entry is published, or the source moved on while this copy was
		// being made and the copy described the older identity. Both mean the copy
		// and the probe of it succeeded, which is what the schedule and the
		// bad-identity record were about, so both clear them. A superseded warm is
		// not an error and needs no retry: the next miss carries the new identity.
		c.noteSuccess(srcPath)

		if errors.Is(err, errSuperseded) {
			return warmSuperseded, nil
		}

		return warmOK, nil
	default:
		// Freshness could not be confirmed -- a spent budget, or a mount that would
		// not answer. The copy is discarded, so this is a failed warm and must be
		// scheduled like any other: reporting it as a success would clear the
		// schedule, leave nothing cached, and let the next resume try again
		// immediately, forever.
		c.noteFailure(srcPath)

		return "", fmt.Errorf("publish envd binary copy %q: %w", srcPath, err)
	}
}

// lookup returns a live entry for srcPath, or nil when there is none, the source
// has changed underneath it, or its copy is gone.
func (c *Cache) lookup(srcPath string, fi os.FileInfo) *Entry {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[srcPath]
	if !ok {
		return nil
	}
	// Both halves matter: a promotion advances mtime, and a re-pin to a
	// different staged binary usually also changes size.
	if e.size != fi.Size() || !e.modTime.Equal(fi.ModTime()) {
		return nil
	}
	// The copy itself must still be there, and this stat is not the cheap redundancy
	// it looks like. Without it an entry whose file was removed underneath it stays
	// a permanent phantom hit: the key still matches, so it is never re-copied,
	// Version keeps answering with the hit, and SourcePath then finds nothing to
	// deliver -- records deliveries{outcome="stale"} and refuses. The node stops
	// upgrading envd until the source's mtime changes. Before the upgrade was gated
	// on a hit, the same removal merely cost a mount read per resume; now it costs
	// the upgrade, so do not drop it as dead weight.
	if e.Cached() {
		if _, err := os.Stat(e.LocalPath); err != nil {
			delete(c.entries, srcPath)
			c.dropOrderLocked(srcPath)

			return nil
		}
	}

	return e
}

// copyIn copies srcPath into the cache directory under a name that encodes the
// key, so a file left behind by an earlier process is never mistaken for a
// current one. The copy is written to a temporary file and renamed, so a torn or
// truncated copy is never observable under the final name.
//
// On success the destination is registered as in-flight and the caller must
// release it once the entry is published.
func (c *Cache) copyIn(ctx context.Context, srcPath string, fi os.FileInfo) (string, error) {
	if c.dir == "" {
		return "", errors.New("envd binary cache directory is not configured")
	}
	dest, err := c.copyInUnchecked(ctx, srcPath, fi)
	if err != nil {
		// A copy the source outran is not a failure of anything: the generation it
		// described was replaced while it was being made, and the next miss carries
		// the new one.
		if errors.Is(err, errSuperseded) {
			return "", errSuperseded
		}

		// Every other copy failure is schedulable: a directory can become writable,
		// a mount can come back, a budget can be met next time. What is not
		// schedulable is a binary that will not run, and that is not detectable
		// here -- it takes the structural check or a probe of the finished copy.
		c.noteFailure(srcPath)

		return "", err
	}

	return dest, nil
}

func (c *Cache) copyInUnchecked(ctx context.Context, srcPath string, fi os.FileInfo) (string, error) {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return "", fmt.Errorf("%w: create dir %q: %w", errCacheUnusable, c.dir, err)
	}
	c.reapTemps.Do(func() { reapTempFiles(c.dir) })

	dest := filepath.Join(c.dir, localName(srcPath, fi))
	// Reserved before the rename, so there is no instant in which the final name
	// exists and is neither in-flight nor an entry.
	c.reserveInFlight(dest)

	src, err := os.Open(srcPath)
	if err != nil {
		c.releaseInFlight(dest)

		return "", fmt.Errorf("open envd binary %q: %w", srcPath, err)
	}
	defer src.Close()

	tmp, err := os.CreateTemp(c.dir, tempPrefix+"*")
	if err != nil {
		c.releaseInFlight(dest)

		return "", fmt.Errorf("%w: create temp file: %w", errCacheUnusable, err)
	}
	tmpName := tmp.Name()
	renamed := false
	defer func() {
		tmp.Close()
		if !renamed {
			os.Remove(tmpName)
			c.releaseInFlight(dest)
		}
	}()

	written, err := copyContext(ctx, tmp, src)
	if err == nil && written != fi.Size() {
		// A clean EOF is not proof of a complete copy, and the shortfall has two
		// causes that must not share a verdict. A promotion landing mid-copy is
		// benign -- the copy simply describes the generation that has just been
		// replaced, and is abandoned here as superseded, before the probe. A source
		// that ended
		// the read early while STILL matching the key is a truncated copy of an
		// intact binary, and that one matters: validateImage rejects it as an
		// incomplete image, which is a verdict about the bytes, so an intact
		// promoted artifact would be pinned. Only the source can tell
		// the two apart.
		if cur, serr := c.statSourceBounded(ctx, srcPath); serr != nil ||
			(cur.Size() == fi.Size() && cur.ModTime().Equal(fi.ModTime())) {
			err = fmt.Errorf("short read of %q: copied %d of %d bytes", srcPath, written, fi.Size())
		} else {
			// The source moved on mid-copy, so this copy describes a generation that
			// no longer exists. It must be abandoned HERE rather than carried
			// forward: a partial image fails validation, and a failed validation is a
			// verdict about the bytes -- so letting it continue would report a benign
			// promotion as a corrupt target, with the loud warn and warms{bad_target}
			// that carries, and pin an identity the source has already left behind.
			// Publication would never get to call it superseded.
			err = errSuperseded
		}
	}
	if err != nil {
		return "", fmt.Errorf("copy envd binary %q: %w", srcPath, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("%w: close temp file: %w", errCacheUnusable, err)
	}
	// The copy is exec'd for the version probe and, on the offline path, read by
	// an unprivileged DynamicUser, so it needs its own mode rather than whatever
	// the umask left.
	if err := os.Chmod(tmpName, 0o755); err != nil {
		return "", fmt.Errorf("%w: chmod: %w", errCacheUnusable, err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return "", fmt.Errorf("%w: rename: %w", errCacheUnusable, err)
	}
	renamed = true

	return dest, nil
}

// copyContext copies src to dst a chunk at a time, checking the deadline between
// chunks, and returns the number of bytes written so a caller can tell a complete
// copy from one the source ended early. io.Copy would run to completion
// regardless, and the source is a network filesystem that can stall indefinitely —
// so the deadline is what stops a warm from retaining its goroutine forever.
// Nothing waits on it: this bounds the background copy, not any caller.
//
// It bounds a copy that is progressing too slowly, not one that is wedged: a read
// already blocked in the kernel cannot be interrupted from here. That limit is
// why the copy is off the resume path in the first place rather than merely
// bounded on it.
func copyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, copyChunk)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				// The write side is the cache's own: no space, read-only mount.
				return written, fmt.Errorf("%w: write: %w", errCacheUnusable, err)
			}
			written += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}

// localName is the cache-directory name for one (path, size, mtime). It encodes
// the whole key, plus a digest of the full source path: two sources sharing a
// basename, size and mtime would otherwise map onto one file and serve each
// other's bytes. The basename stays first so the directory is readable on a node.
func localName(srcPath string, fi os.FileInfo) string {
	sum := sha256.Sum256([]byte(srcPath))

	return fmt.Sprintf("%s.%d.%d.%s",
		filepath.Base(srcPath), fi.Size(), fi.ModTime().UnixNano(),
		hex.EncodeToString(sum[:4]))
}

func (c *Cache) reserveInFlight(dest string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.inFlight[dest]++
}

func (c *Cache) releaseInFlight(dest string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.inFlight[dest] <= 1 {
		delete(c.inFlight, dest)

		return
	}
	c.inFlight[dest]--
}

// warmResult labels a warm that returned an error. A warm that returns nil is
// labelled by whether the binary ended up cached, which the warm itself reports
// back -- see reportWarm.
//
// A suppressed one is the backoff doing its job, not a failure: a miss recurs on
// every resume, so counting suppressions as failures would report a flood for a
// cache that is deliberately not trying, and hide the case where the warmer is
// genuinely broken.
func warmResult(err error) string {
	switch {
	case errors.Is(err, errWarmSuppressed):
		return warmSuppressed
	case errors.Is(err, errBadIdentityKnown):
		return warmPinned
	case errors.Is(err, errWarmInFlight):
		return warmAlreadyWarming
	case errors.Is(err, errBadTarget):
		return warmBadTarget
	default:
		return warmFailed
	}
}

// probeVerdictIsAboutTheBytes reports whether a failed version probe proves that
// the binary itself cannot run, as opposed to something about this node or this
// moment having stopped the probe.
//
// Only the former may pin an identity, because a pin is not a delay: nothing
// clears it but a promotion, so a wrong one ends envd upgrades on this node for
// the generation. Process creation fails for plenty of reasons that say nothing
// about the image -- fd exhaustion surfaces as pipe2 EMFILE because Output()
// allocates pipes, a packed node can fail fork with ENOMEM, ETXTBSY can outlast
// os/exec's own retry window, and a cache directory mounted noexec would fail
// every exec with EACCES. None of those are verdicts, and all of them recur, so
// all of them belong on the schedule.
func probeVerdictIsAboutTheBytes(ctx context.Context, err error) bool {
	// Whatever the error says, a probe that ran out of time was stopped rather
	// than answered.
	if ctx.Err() != nil {
		return false
	}

	// The loader looked at the image and refused it: not an executable, or not
	// this architecture. That is exactly a verdict about the bytes. It is rarer
	// than it looks now that validateImage runs first -- most images that would
	// reach ENOEXEC are incomplete and are rejected before the exec -- so what
	// remains here is chiefly the wrong-architecture case.
	if errors.Is(err, syscall.ENOEXEC) {
		return true
	}

	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		if exit.ProcessState == nil {
			return false
		}

		// A signalled child is usually a property of the node -- an OOM kill, a
		// stray SIGTERM -- so ExitError alone is not the test. Three signals are the
		// exception, and they exist for the damage the structural layer cannot see:
		// an image that is COMPLETE but corrupt inside parses cleanly, loads, begins
		// executing, and then faults. Measured on a 2.4 MB Go binary, 4 KB of 0xff
		// written into the middle parses and then raises SIGSEGV.
		// Truncation is validateImage's job and no longer reaches here at all.
		if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			switch ws.Signal() {
			case syscall.SIGBUS, syscall.SIGSEGV, syscall.SIGILL:
				return true
			}
		}

		// An ordinary nonzero exit is NOT a verdict, however much it looks like one.
		// It cannot be told apart from a binary that runs perfectly well and simply
		// does not accept -version: measured, a Go program whose flag set lacks the
		// flag exits 2 with an EMPTY stdout, putting its complaint on stderr, which
		// is indistinguishable from a corrupt image judging itself. Since a pin is
		// permanent, the ambiguous case has to schedule -- so what pins here is only
		// evidence from outside the image: the loader refusing it, or the CPU
		// faulting on its own instructions. That the probe is `<copy> -version`, an
		// interface the image is free to change, is exactly why its exit code is not
		// evidence about the bytes.
	}

	return false
}

// beginWarm claims the right to warm one identity of srcPath, reporting false
// when a warm of those exact bytes is already in flight, and how long its holder
// has held the slot -- which is what tells a node busy warming from a source that
// has stopped answering.
func (c *Cache) beginWarm(srcPath string, fi os.FileInfo) (bool, time.Duration) {
	key := identityKey(srcPath, fi)

	c.mu.Lock()
	defer c.mu.Unlock()

	if claimed, ok := c.warming[key]; ok {
		return false, time.Since(claimed)
	}
	c.warming[key] = time.Now()

	return true, 0
}

func (c *Cache) endWarm(srcPath string, fi os.FileInfo) {
	key := identityKey(srcPath, fi)

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.warming, key)
}

// identityKey names one generation of one source binary: the same (path, size,
// mtime) triple the entry map revalidates against. It keys both mechanisms that
// collapse concurrent work -- the warming slot and the flight -- so a promotion
// landing mid-copy is a different key everywhere rather than in one place only.
func identityKey(srcPath string, fi os.FileInfo) string {
	return srcPath + "\x00" +
		strconv.FormatInt(fi.Size(), 10) + "\x00" +
		strconv.FormatInt(fi.ModTime().UnixNano(), 10)
}

// scheduleSuppressed reports whether srcPath is inside its retry schedule.
//
// Deliberately identity-free, so it can be consulted BEFORE the source is
// stat-ed: a path that is not going to be copied should not cost a metadata
// lookup on the very mount the schedule exists to protect.
func (c *Cache) scheduleSuppressed(srcPath string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, ok := c.backoff[srcPath]

	return ok && time.Now().Before(b.until)
}

// identityKnownBad reports whether these exact bytes have already been judged
// unusable. Unlike the schedule this needs the identity, so it is checked
// after the stat -- which the caller has already paid for by this point.
func (c *Cache) identityKnownBad(srcPath string, fi os.FileInfo) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, ok := c.backoff[srcPath]

	return ok && b.hasBad && b.badSize == fi.Size() && b.badModTime.Equal(fi.ModTime())
}

// noteFailure schedules the next attempt for srcPath after a failure that may
// clear on its own, doubling from the base to the ceiling.
func (c *Cache) noteFailure(srcPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := c.backoffLocked(srcPath)
	b.failures++
	// Shift rather than multiply, and cap the shift: past 33 the shift stops
	// producing a sane delay -- it overflows int64 to a negative duration from 34,
	// and wraps to zero further up. Either is < backoffCeiling, so it would be
	// taken as the delay and schedule the next attempt in the past.
	delay := backoffCeiling
	if b.failures <= 30 {
		if d := backoffBase << (b.failures - 1); d < backoffCeiling {
			delay = d
		}
	}
	b.until = time.Now().Add(delay)
}

// markBadIdentity records that these bytes cannot run, and stops scheduling
// retries of them: no delay makes a truncated binary executable. The schedule is
// left cleared so that a promotion -- a different identity -- is warmed at the
// next miss rather than waiting out a window it had no part in.
func (c *Cache) markBadIdentity(srcPath string, fi os.FileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := c.backoffLocked(srcPath)
	b.hasBad = true
	b.badSize = fi.Size()
	b.badModTime = fi.ModTime()
	b.failures = 0
	b.until = time.Time{}
}

// noteSuccess clears everything remembered about srcPath. A published entry is
// proof that neither the schedule nor the bad-identity record applies any more.
func (c *Cache) noteSuccess(srcPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.backoff, srcPath)
}

// shouldWarn reports whether a failure of srcPath may be logged above Debug,
// allowing one line per path per ceiling-length window.
//
// A node that cannot warm stops upgrading envd while its resumes keep succeeding,
// which is the one state in which this cache is worse than no cache -- so it is
// worth a log line and not only a counter. The rate limit is what makes that
// affordable: the failure recurs on every resume.
func (c *Cache) shouldWarn(srcPath string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	b := c.backoffLocked(srcPath)
	now := time.Now()
	if !b.warnedAt.IsZero() && now.Sub(b.warnedAt) < backoffCeiling {
		return false
	}
	b.warnedAt = now

	return true
}

// backoffLocked returns srcPath's record, creating it. Callers hold mu.
func (c *Cache) backoffLocked(srcPath string) *pathBackoff {
	b, ok := c.backoff[srcPath]
	if !ok {
		b = &pathBackoff{}
		if c.backoff == nil {
			c.backoff = make(map[string]*pathBackoff)
		}
		c.backoff[srcPath] = b
	}

	return b
}

// store publishes an entry, evicts past the cap, and sweeps the directory.
func (c *Cache) store(ctx context.Context, e *Entry) error {
	// A warm that began before a promotion can finish after one that began after
	// it, because the two carry different keys and so run concurrently. The stale
	// one must not be published: it would delete the fresh copy just below and
	// leave an entry matching nothing, so every lookup misses until another warm
	// heals it. Lookup's own revalidation does not cover this -- it keeps the stale
	// entry from being SERVED, not from destroying the good copy on its way in.
	//
	// The test is against the SOURCE, not against whatever is resident. Comparing
	// generations to each other would rest on mtimes only ever increasing, and an
	// entry that once recorded an mtime no later source can beat would then reject
	// every publication for that path forever -- a silent, permanent wedge. Asking
	// "does this entry describe the source right now" cannot wedge: a resident
	// entry that no longer describes it is simply replaced by one that does.
	//
	// Bounded, and outside the lock. This stats the network filesystem the whole
	// cache exists to keep off the resume path, and os.Stat honours no deadline: on
	// a wedged mount an unbounded call here would hold this path's warming slot for
	// the life of the process, leaving the node a permanent miss with no warm
	// metric to say why. Holding mu across it would be worse still, since Lookup
	// takes the same mutex -- the wait moved off the hot path, quietly put back.
	fi, err := c.statSourceBounded(ctx, e.SourcePath)
	if err != nil {
		// Freshness unconfirmable, so the copy cannot be published -- but this is a
		// failure of the mount or of the budget, not a promotion, and the caller has
		// to be able to tell those apart.
		os.Remove(e.LocalPath)

		return fmt.Errorf("revalidate envd binary source %q: %w", e.SourcePath, err)
	}
	if fi.Size() != e.size || !fi.ModTime().Equal(e.modTime) {
		os.Remove(e.LocalPath)

		return errSuperseded
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if prev, ok := c.entries[e.SourcePath]; ok {
		// Replacing a generation of the same path: the old copy has no further
		// readers once the entry is gone.
		if prev.LocalPath != e.LocalPath {
			os.Remove(prev.LocalPath)
		}
		c.dropOrderLocked(e.SourcePath)
	}
	// Appended on replace as well as on first insert. Without this the order is
	// pure first-insertion, so the boot-warmed promoted binary stays order[0] for
	// the life of the process and is the FIRST thing evicted once a ramp fills the
	// cap with staged versions -- while the dead staged paths that pushed it out
	// survive. Every such eviction costs a deferred upgrade per resume until a warm
	// re-lands, and it repeats on the next flip.
	c.order = append(c.order, e.SourcePath)
	c.entries[e.SourcePath] = e

	for len(c.order) > maxEntries {
		oldest := c.order[0]
		c.order = c.order[1:]
		if victim, ok := c.entries[oldest]; ok {
			delete(c.entries, oldest)
			os.Remove(victim.LocalPath)
		}
	}

	c.sweepLocked()

	return nil
}

// dropOrderLocked removes srcPath from the insertion order. Callers hold mu.
func (c *Cache) dropOrderLocked(srcPath string) {
	for i, p := range c.order {
		if p == srcPath {
			c.order = append(c.order[:i], c.order[i+1:]...)

			return
		}
	}
}

// sweepLocked deletes everything in the cache directory that no live entry
// references and no copy is still working on. It is the only bound that survives
// a restart: an in-memory policy cannot see the copies a previous process left
// behind, and this directory is otherwise append-only across the lifetime of a
// node.
func (c *Cache) sweepLocked() {
	keep := make(map[string]struct{}, len(c.entries)+len(c.inFlight))
	for _, e := range c.entries {
		keep[e.LocalPath] = struct{}{}
	}
	// A copy that has been renamed into place but not yet published belongs to
	// no entry. Deleting it would fail an unrelated resolution.
	for p := range c.inFlight {
		keep[p] = struct{}{}
	}

	names, err := os.ReadDir(c.dir)
	if err != nil {
		// Best-effort: a directory we cannot list is not a reason to fail a
		// resolution that already succeeded.
		return
	}
	for _, n := range names {
		if n.IsDir() {
			continue
		}
		// A temporary file may belong to a copy that is still writing it, whose
		// rename would then fail. Those are cleaned up by copyIn's own deferred
		// remove and by the once-per-cache reap.
		if strings.HasPrefix(n.Name(), tempPrefix) {
			continue
		}
		p := filepath.Join(c.dir, n.Name())
		if _, ok := keep[p]; ok {
			continue
		}
		os.Remove(p)
	}
}

// reapTempFiles removes temporary files in dir. It runs once per cache, before
// this process has created any, so every one it finds was orphaned by a previous
// process crashing mid-copy.
func reapTempFiles(dir string) {
	names, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, n := range names {
		if !n.IsDir() && strings.HasPrefix(n.Name(), tempPrefix) {
			os.Remove(filepath.Join(dir, n.Name()))
		}
	}
}
