package envdbin

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A probe failure only pins an identity when the verdict is about the bytes.
// This is the precondition for declining to retry at all: a probe killed by the
// warm's own deadline, or one that found the copy already swept away, says
// nothing about the binary -- and pinned, it would hold a GOOD binary out until
// the next promotion rather than delaying it by one window.
func TestOnlyAVerdictAboutTheBytesPinsAnIdentity(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// probe receives a hook that cancels the warm's own context, so a case can
		// make the context done at exactly the moment the probe returns -- after the
		// copy has already landed. An already-expired context cannot model this: the
		// copy checks the deadline and fails first, so the classification under test
		// would never run and the case would pass vacuously.
		probe   func(cancel func()) func(context.Context, string) (string, error)
		wantBad bool
	}{
		{
			name: "the loader refusing the image is pinned",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(_ context.Context, path string) (string, error) {
					return "", errNotExecutable(path)
				}
			},
			wantBad: true,
		},
		{
			name: "a probe stopped by the warm's own deadline is not",
			probe: func(cancel func()) func(context.Context, string) (string, error) {
				return func(context.Context, string) (string, error) {
					cancel()

					return "", errors.New("signal: killed")
				}
			},
			wantBad: false,
		},
		{
			// An ordinary nonzero exit looks like the image judging itself and is not:
			// a binary that runs fine but does not accept -version exits the same way
			// (measured, exit 2 with an empty stdout and its complaint on stderr), and
			// -version is an interface the image may change. Since a pin is permanent
			// and this evidence is ambiguous, it schedules.
			name: "a child that ran and exited nonzero is not",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(ctx context.Context, _ string) (string, error) {
					return "", errExitedNonZero(ctx, t)
				}
			},
			wantBad: false,
		},
		{
			name: "a copy that vanished under the probe is not",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(context.Context, string) (string, error) {
					return "", &fs.PathError{Op: "fork/exec", Err: fs.ErrNotExist}
				}
			},
			wantBad: false,
		},
		{
			name: "fd exhaustion allocating the probe's pipes is not",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(context.Context, string) (string, error) {
					return "", errFdExhaustion()
				}
			},
			wantBad: false,
		},
		{
			name: "a packed node failing fork is not",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(_ context.Context, path string) (string, error) {
					return "", errForkOOM(path)
				}
			},
			wantBad: false,
		},
		{
			name: "a cache directory mounted noexec is not",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(_ context.Context, path string) (string, error) {
					return "", errNoexecMount(path)
				}
			},
			wantBad: false,
		},
		{
			// A COMPLETE image that is corrupt inside parses cleanly, loads, begins
			// executing, and then faults. Measured on a 2.4 MB Go binary, 4 KB of 0xff
			// written into the middle parses and then raises SIGSEGV. Truncation never
			// reaches here -- validateImage owns it -- so this is the case
			// markBadIdentity exists for that a signal-blind rule would miss.
			name: "an image that faults on its own instructions is pinned",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(ctx context.Context, _ string) (string, error) {
					return "", errSignalled(ctx, t, "SEGV")
				}
			},
			wantBad: true,
		},
		{
			name: "an image killed by SIGBUS is pinned",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(ctx context.Context, _ string) (string, error) {
					return "", errSignalled(ctx, t, "BUS")
				}
			},
			wantBad: true,
		},
		{
			name: "a child killed by an unrelated signal is not",
			probe: func(func()) func(context.Context, string) (string, error) {
				return func(ctx context.Context, _ string) (string, error) {
					return "", errSignalled(ctx, t, "TERM")
				}
			},
			wantBad: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, "envd")
			writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			probed := false
			inner := tc.probe(cancel)
			c := newCacheForTest(t, filepath.Join(dir, "cache"), func(pc context.Context, path string) (string, error) {
				probed = true

				return inner(pc, path)
			})

			err := c.Warm(ctx, src)
			require.Error(t, err)
			require.True(t, probed,
				"the copy must have landed and the probe run, or this case says nothing about classification")

			require.Equal(t, tc.wantBad, c.identityKnownBad(src, mustStat(t, src)))

			if tc.wantBad {
				require.ErrorIs(t, err, errBadTarget)

				return
			}

			// Not pinned means retryable: with the schedule cleared the same identity
			// is copied and probed again, rather than refused outright.
			require.NotErrorIs(t, err, errBadTarget,
				"a probe failure that is not about the bytes must not be reported as a bad target")
			c.noteSuccess(src)
			require.NotErrorIs(t, c.Warm(t.Context(), src), errBadIdentityKnown,
				"the identity must still be retried")
		})
	}
}

// The schedule doubles from the base and stops at the ceiling. Asserted on the
// instants the policy computes, never by waiting: the claim is about the
// schedule, and a clock-based version of this test would measure the machine.
func TestTheScheduleDoublesFromTheBaseAndStopsAtTheCeiling(t *testing.T) {
	t.Parallel()

	c := NewCache(t.TempDir(), nil)
	const src = "/fc-envd/envd"

	var delays []time.Duration
	for range 12 {
		before := time.Now()
		c.noteFailure(src)
		c.mu.Lock()
		until := c.backoff[src].until
		c.mu.Unlock()
		delays = append(delays, until.Sub(before).Round(time.Second))
	}

	require.Equal(t, backoffBase, delays[0], "the first retry waits the base")
	require.Equal(t, 2*backoffBase, delays[1])
	require.Equal(t, 4*backoffBase, delays[2])
	for i, d := range delays {
		require.LessOrEqual(t, d, backoffCeiling, "delay %d exceeded the ceiling", i)
	}
	require.Equal(t, backoffCeiling, delays[len(delays)-1], "it must reach and hold the ceiling")

	// A success forgets everything, so a node that recovers is immediately back to
	// normal rather than serving out a window it no longer needs.
	c.noteSuccess(src)
	require.False(t, c.scheduleSuppressed(src))
}

// One Cache serves both upgrade targets, which resolve from independent flags.
// A defence held cache-wide would let an unrunnable binary named by one flag
// starve warms of a good binary named by the other -- and both report
// binary_not_cached at the call sites, so the starvation would be
// indistinguishable from a cold node.
func TestABadTargetOnOnePathDoesNotStarveAnother(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bad := filepath.Join(dir, "envd.bad")
	good := filepath.Join(dir, "envd")
	writeSource(t, bad, "unrunnable", time.Unix(1_700_000_000, 0))
	writeSource(t, good, "binary-v1", time.Unix(1_700_000_000, 0))

	probe := func(_ context.Context, path string) (string, error) {
		if filepath.Base(path) == filepath.Base(localName(bad, mustStat(t, bad))) {
			return "", errNotExecutable(path)
		}

		return "0.7.0", nil
	}
	c := newCacheForTest(t, filepath.Join(dir, "cache"), probe)

	require.ErrorIs(t, c.Warm(t.Context(), bad), errBadTarget)

	// The good path is untouched by that verdict, on the first attempt and on
	// every one after it.
	for range 3 {
		require.NoError(t, c.Warm(t.Context(), good),
			"a bad target on one path must not suppress warms of another")
	}
	e, ok := c.Lookup(good)
	require.True(t, ok)
	require.Equal(t, "0.7.0", e.Version)
}

// A path inside its schedule must not cost a metadata lookup on the mount the
// schedule exists to protect. Counted through the injected source stat, which is
// the only way to observe it.
func TestAPathInsideItsScheduleIsNotStatedOnTheSource(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})

	// Atomic because the counter is written from the goroutine statSourceBounded takes
	// the stat on and read from the goroutine require.Never runs its condition on --
	// Never returns on its own timer without waiting for an in-flight check, so a
	// leftover poller can still be reading when the warm below increments. A run in
	// which the property breaks then reports as the failed assertion it is, rather
	// than as a data race.
	var stats atomic.Int64
	c.statSrc = func(path string) (os.FileInfo, error) {
		stats.Add(1)

		return os.Stat(path)
	}

	c.noteFailure(src)
	before := stats.Load()
	require.ErrorIs(t, c.Warm(t.Context(), src), errWarmSuppressed)
	require.Equal(t, before, stats.Load(), "a suppressed path must not be stat-ed on the source")

	// The background shape has to hold the same property, and the check that gives
	// it is shared with the resume path, which is where it earns itself: a miss
	// recurs on every resume, so a suppressed path stat-ed from the warmer would
	// put back the per-resume mount traffic the schedule exists to remove. Driven
	// here through the shape that has no identity to hand down, because that is
	// the one whose stat the check has to pre-empt.
	c.WarmAsync(t.Context(), src)
	require.Never(t, func() bool {
		return stats.Load() > before
	}, 200*time.Millisecond, 10*time.Millisecond,
		"a suppressed path must not be stat-ed by the background warmer either")

	// Positive twin: with the schedule clear the same call does stat, so the
	// assertion above is about the suppression and not about a stat that never
	// happens either way.
	c.noteSuccess(src)
	require.NoError(t, c.Warm(t.Context(), src))
	require.Greater(t, stats.Load(), before, "an unsuppressed warm must stat the source")
}

// A node that cannot warm keeps serving resumes and quietly stops upgrading
// envd, so it earns a log line -- but the failure recurs on every resume, so the
// line has to be rate limited per path.
func TestTheFailureLogIsRateLimitedPerPath(t *testing.T) {
	t.Parallel()

	c := NewCache(t.TempDir(), nil)

	require.True(t, c.shouldWarn("/a"), "the first failure of a path speaks")
	require.False(t, c.shouldWarn("/a"), "the second inside the window does not")
	require.True(t, c.shouldWarn("/b"), "a different path is not rate limited by /a")

	// The window is per path and measured from the last line, so moving /a's
	// stamp back lets it speak again without touching /b.
	c.mu.Lock()
	c.backoff["/a"].warnedAt = time.Now().Add(-2 * backoffCeiling)
	c.mu.Unlock()
	require.True(t, c.shouldWarn("/a"))
	require.False(t, c.shouldWarn("/b"))
}

// A wedged mount must not turn one warm into a permanent miss. Publication
// revalidates against the source -- which is what stops a stale warm deleting the
// fresh copy -- but os.Stat honours no deadline, so an unbounded call there would
// hold this path's warming slot for the life of the process and the node would
// stop warming with no metric to say why.
func TestAWedgedSourceStatDoesNotRetainThePublisher(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})

	// A stat that never returns, as a wedged gcsfuse metadata lookup does. Released
	// at the end so the parked goroutine cannot outlive the test.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	entered := make(chan struct{}, 1)
	c.statSrc = func(string) (os.FileInfo, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release

		return nil, errors.New("unreachable")
	}

	// The deadline is the caller's, so publication gives up with it rather than
	// waiting on the mount.
	ctx, cancel := context.WithCancel(t.Context())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		c.store(ctx, &Entry{
			SourcePath: src,
			LocalPath:  filepath.Join(dir, "cache", "copy"),
			Version:    "0.7.0",
			size:       1,
			modTime:    time.Unix(1_700_000_000, 0),
		})
	}()

	<-entered
	cancel()

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		require.Fail(t, "publication was retained by a wedged source stat; the warming slot would leak")
	}

	// It declined to publish, which is the safe direction: an unverified entry
	// must not be able to delete a copy it did not make. Lookup stats the source
	// too, so the real stat goes back first -- otherwise this assertion would park
	// on the same wedge it just proved survivable.
	c.statSrc = os.Stat
	_, ok := c.Lookup(src)
	require.False(t, ok)
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)

	return fi
}

// The bound belongs to every source stat a warm takes, not just the one in
// publication. A warm that was handed no identity has to stat the source before
// it can do anything else -- the blocking shape this test drives, and the boot
// warm -- and a metadata lookup blocked in the kernel is not interruptible from
// userspace, so an unbounded call there retains whoever made it for the life of
// the process: the caller itself here, one goroutine for the boot warm. It is
// why that stat is taken before any slot is claimed.
func TestAWedgedSourceStatDoesNotRetainTheWarm(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	entered := make(chan struct{}, 1)
	c.statSrc = func(string) (os.FileInfo, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release

		return nil, errors.New("unreachable")
	}

	ctx, cancel := context.WithCancel(t.Context())
	returned := make(chan error, 1)
	go func() { returned <- c.Warm(ctx, src) }()

	<-entered
	cancel()

	select {
	case err := <-returned:
		require.Error(t, err, "the warm must fail rather than hang")
	case <-time.After(10 * time.Second):
		require.Fail(t, "Warm was retained by a wedged source stat; the warming slot would leak")
	}
}

// A publication that cannot confirm freshness discards the copy. That is a failed
// warm and has to be scheduled like one: reported as a success it would clear the
// schedule, leave nothing cached, and let every following resume retry
// immediately -- an unbounded loop wearing the label of a promotion.
func TestAnUnconfirmablePublishIsAFailureNotASupersede(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})

	// The copy and the probe succeed; only the revalidation stat fails, which is
	// what a spent budget or an unanswering mount looks like at that point.
	var calls int
	c.statSrc = func(path string) (os.FileInfo, error) {
		calls++
		if calls == 1 {
			return os.Stat(path)
		}

		return nil, errors.New("mount did not answer")
	}

	err := c.Warm(t.Context(), src)
	require.Error(t, err, "an unconfirmable publish must surface as a failed warm")
	require.NotErrorIs(t, err, errSuperseded, "a mount failure is not a promotion")

	// Scheduled, so the next resume does not immediately try again.
	c.mu.Lock()
	b := c.backoff[src]
	c.mu.Unlock()
	require.NotNil(t, b, "the failure must be scheduled")
	require.Equal(t, 1, b.failures)
	require.True(t, c.scheduleSuppressed(src))

	// Positive twin: a genuine supersede -- freshness confirmed, identity moved on
	// -- is NOT a failure and leaves no schedule behind.
	c.noteSuccess(src)
	calls = 0
	c.statSrc = func(path string) (os.FileInfo, error) {
		calls++
		if calls == 1 {
			return os.Stat(path)
		}
		// A promotion landed while the copy was being made.
		writeSource(t, src, "binary-v2", time.Unix(1_700_000_500, 0))

		return os.Stat(path)
	}
	require.NoError(t, c.Warm(t.Context(), src), "a supersede is not an error")
	c.mu.Lock()
	b = c.backoff[src]
	c.mu.Unlock()
	require.Nil(t, b, "a supersede must not schedule a retry")
}

// The classifier reads real error shapes, so the tests build real ones rather
// than a string that happens to say "exec format error" -- which is exactly the
// substitution that let a denylist look correct.

// errNotExecutable is what the loader returns for an image it refuses: not an
// executable, or not this architecture.
func errNotExecutable(path string) error {
	return &fs.PathError{Op: "fork/exec", Path: path, Err: syscall.ENOEXEC}
}

// errFdExhaustion is what Output() returns when it cannot allocate its pipes.
func errFdExhaustion() error {
	return &os.SyscallError{Syscall: "pipe2", Err: syscall.EMFILE}
}

// errForkOOM is a packed node failing fork. Note it is a PathError whose Err is
// ENOMEM, so fs.ErrNotExist does not match it -- the shape that made a two-item
// denylist look sufficient.
func errForkOOM(path string) error {
	return &fs.PathError{Op: "fork/exec", Path: path, Err: syscall.ENOMEM}
}

// errNoexecMount is every exec failing because the cache directory is mounted
// noexec: deterministic, and under a denylist it would pin every identity.
func errNoexecMount(path string) error {
	return &fs.PathError{Op: "fork/exec", Path: path, Err: syscall.EACCES}
}

// errExitedNonZero and errSignalled come from real processes, because
// exec.ExitError carries an os.ProcessState that cannot be fabricated
// meaningfully -- and the distinction under test is precisely what that state
// reports.
func errExitedNonZero(ctx context.Context, t *testing.T) error {
	t.Helper()
	_, err := exec.CommandContext(ctx, "/bin/sh", "-c", "exit 3").Output()
	require.Error(t, err)

	return err
}

func errSignalled(ctx context.Context, t *testing.T, signal string) error {
	t.Helper()
	_, err := exec.CommandContext(ctx, "/bin/sh", "-c", "kill -"+signal+" $$").Output()
	require.Error(t, err)

	return err
}

// The whole class, asserted once: no source stat taken inside a warming slot may
// be UNBOUNDED. Two are -- the one that resolves a short read mid-copy and
// publication's revalidation -- and both are bounded; the warm's own stat is
// taken before the slot is claimed, and the Lookup that once decided
// ok-vs-superseded afterwards is gone. This pins the invariant instead of the
// instances: with the source stat wedged from the copy onwards, so that
// publication's re-check is the call that stalls, the slot must still be
// released.
func TestNoSourceStatInsideTheWarmingSlotCanWedgeIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})
	// Short, because the only way to observe a wedge is to wait one out.
	c.budget = 200 * time.Millisecond

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	// The mount wedges only once the copy exists, which is what puts the wedge in
	// the one stat that runs after the copy and still inside the slot: the
	// freshness re-check publication makes. Wedging from the start instead makes
	// the warm fail at its own stat, so that step is never reached and the test
	// passes whether or not the defect is present.
	copied := func() bool {
		names, err := os.ReadDir(filepath.Join(dir, "cache"))
		if err != nil {
			return false
		}
		for _, n := range names {
			if !strings.HasPrefix(n.Name(), tempPrefix) {
				return true
			}
		}

		return false
	}
	c.statSrc = func(path string) (os.FileInfo, error) {
		if copied() {
			<-release

			return nil, errors.New("wedged mount")
		}

		return os.Stat(path)
	}

	// WarmAsync's budget is its own, so this returns without the test waiting.
	c.WarmAsync(t.Context(), src)

	// Precondition: the warm got far enough to copy, so the wedge below is reached
	// at all.
	require.Eventually(t, copied, 10*time.Second, 20*time.Millisecond,
		"the warm never copied, so this test would not reach the step under test")

	// The slot is what matters: once it is released, a later warm of these bytes
	// can run. Held, they are a permanent miss with no metric to say why.
	fi := mustStat(t, src)
	require.Eventually(t, func() bool {
		ok, _ := c.beginWarm(src, fi)

		return ok
	}, 10*time.Second, 20*time.Millisecond,
		"the warming slot was never released; these bytes could never warm again")
	c.endWarm(src, fi)
}

// Structural validation is what makes a truncated promotion a deterministic
// verdict instead of one inferred from how a process died -- and it reaches that
// verdict without executing the bytes, so a truncated artifact never runs on the
// host. A complete image that is corrupt inside still has to be exec'd to be
// judged; TestStructuralValidationOwnsIncompletenessOnly pins that split.
func TestATruncatedImageIsPinnedWithoutBeingExecuted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	// Cutting the payload alone still parses -- the fixture's header describes no
	// sections -- so cut into the header itself, which is what a partially-written
	// artifact looks like.
	full := elfWithPayload("the-whole-binary")
	require.NoError(t, os.WriteFile(src, full[:20], 0o755))
	require.NoError(t, os.Chtimes(src, time.Unix(1_700_000_000, 0), time.Unix(1_700_000_000, 0)))

	var probes int
	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		probes++

		return "0.7.0", nil
	})

	err := c.Warm(t.Context(), src)
	require.ErrorIs(t, err, errBadTarget, "an unloadable image is a verdict about the bytes")
	require.ErrorIs(t, err, errBadImage)
	require.Zero(t, probes, "an incomplete image must never be executed to be judged")

	fi, statErr := os.Stat(src)
	require.NoError(t, statErr)
	require.True(t, c.identityKnownBad(src, fi), "and the identity must not be retried")

	// Positive twin: an intact image of the same shape is accepted and probed, so
	// the assertions above are about the truncation.
	writeSource(t, src, "the-whole-binary", time.Unix(1_700_000_100, 0))
	require.NoError(t, c.Warm(t.Context(), src))
	require.Positive(t, probes)
}

// A copy the source outran describes a generation that no longer exists. It has
// to be abandoned before the structural check, because a partial image fails that
// check and a failed check is a verdict about the bytes -- so carrying it forward
// would report a benign promotion as a corrupt target and pin an identity the
// source has already left behind.
func TestACopyTheSourceOutranIsSupersededNotABadTarget(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")

	// The source holds only a fragment of an image -- what a partially-written
	// artifact looks like -- while its stat claims the full length. So the copy
	// that comes out is genuinely unloadable, which is what makes this test able to
	// tell the two verdicts apart at all.
	full := elfWithPayload("generation-one")
	require.NoError(t, os.WriteFile(src, full[:20], 0o755))
	require.NoError(t, os.Chtimes(src, time.Unix(1_700_000_000, 0), time.Unix(1_700_000_000, 0)))

	var probes int
	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		probes++

		return "0.7.0", nil
	})

	var calls int
	c.statSrc = func(path string) (os.FileInfo, error) {
		calls++
		if calls == 1 {
			// Claim the whole image, so the short read is detected.
			return fakeSize{FileInfo: mustStat(t, path), size: int64(len(full))}, nil
		}
		// By the time the shortfall is resolved, a promotion has landed: the
		// benign cause of a shortfall.
		writeSource(t, src, "generation-two", time.Unix(1_700_000_500, 0))

		return os.Stat(path)
	}

	result, err := c.warm(t.Context(), src)
	require.NoError(t, err, "a promotion mid-copy is not a failure")
	require.Equal(t, warmSuperseded, result)
	require.Zero(t, probes, "an abandoned copy must not be probed either")

	fi := mustStat(t, src)
	require.False(t, c.identityKnownBad(src, fi),
		"the new generation must not be pinned on the strength of a partial copy of the old one")
	c.mu.Lock()
	b := c.backoff[src]
	c.mu.Unlock()
	require.Nil(t, b, "nor scheduled: nothing failed")

	// And the new generation warms normally on the next miss.
	require.NoError(t, c.Warm(t.Context(), src))
	e, ok := c.Lookup(src)
	require.True(t, ok)
	require.Equal(t, "0.7.0", e.Version)
}

// fakeSize reports a size of its own, modelling a stat that claims more than the
// reads deliver.
type fakeSize struct {
	os.FileInfo

	size int64
}

func (f fakeSize) Size() int64 { return f.size }

// A source stat that fails has to arm the schedule like every other failure that
// may clear. A miss re-triggers a warm on every resume, so without it a mount that
// cannot answer its metadata spawns one warm per resume and floods warms{failed} --
// the flood the schedule exists to stop, arriving through the one path that was not
// arming it.
func TestASourceStatFailureSchedulesLikeAnyOther(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeSource(t, src, "binary-v1", time.Unix(1_700_000_000, 0))

	c := newCacheForTest(t, filepath.Join(dir, "cache"), func(context.Context, string) (string, error) {
		return "0.7.0", nil
	})

	var stats int
	c.statSrc = func(string) (os.FileInfo, error) {
		stats++

		return nil, errors.New("mount did not answer its metadata")
	}

	require.Error(t, c.Warm(t.Context(), src))

	c.mu.Lock()
	b := c.backoff[src]
	c.mu.Unlock()
	require.NotNil(t, b, "a source stat failure must be scheduled")
	require.Equal(t, 1, b.failures)
	require.True(t, c.scheduleSuppressed(src))

	// Which is what stops the flood: the next warm declines before touching the
	// mount at all, so the stat count does not move.
	before := stats
	require.ErrorIs(t, c.Warm(t.Context(), src), errWarmSuppressed)
	require.Equal(t, before, stats, "a scheduled path must not re-stat the source")
}

// The structural layer owns incompleteness and nothing more. Corruption inside an
// otherwise whole image parses cleanly and has to reach the probe, which is the
// layer that can judge it -- so the two layers must not be described, or tested, as
// if either covered the other's case.
func TestStructuralValidationOwnsIncompletenessOnly(t *testing.T) {
	t.Parallel()

	full := elfWithPayload("the-whole-binary")

	// Incomplete: rejected without executing anything.
	require.Error(t, validateImageBytes(t, full[:20]),
		"an image cut inside its header must be rejected structurally")

	// Complete but corrupt inside: parses, so this layer passes it on. The bytes
	// after the header are payload here, which is what a mid-file corruption of a
	// real binary looks like to elf.NewFile.
	corrupt := append([]byte(nil), full...)
	for i := elfHeaderLen; i < len(corrupt); i++ {
		corrupt[i] = 0xff
	}
	require.NoError(t, validateImageBytes(t, corrupt),
		"corruption past the headers parses; judging it is the probe's job, not this layer's")
}

// validateImageBytes writes b to a temp file and runs the production structural
// check over it.
func validateImageBytes(t *testing.T, b []byte) error {
	t.Helper()
	p := filepath.Join(t.TempDir(), "img")
	require.NoError(t, os.WriteFile(p, b, 0o755))

	fi, err := os.Stat(p)
	require.NoError(t, err)

	return validateImage(p, fi.Size())
}
