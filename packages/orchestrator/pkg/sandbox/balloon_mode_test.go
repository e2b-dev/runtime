package sandbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func TestBalloonModeOf(t *testing.T) {
	t.Parallel()
	assert.Equal(t, userfaultfd.BalloonModeReporting, balloonModeOf(fc.BalloonCaps{Reporting: true, Hinting: true}), "reporting wins: it is the mechanism that discards on its own")
	assert.Equal(t, userfaultfd.BalloonModeHinting, balloonModeOf(fc.BalloonCaps{Hinting: true}))
	assert.Equal(t, userfaultfd.BalloonModeNone, balloonModeOf(fc.BalloonCaps{}))
}

// A sandbox without a process (a build, a test) keeps the unknown label and
// the read is a no-op rather than a nil dereference.
func TestLabelBalloonMode_NoProcess(t *testing.T) {
	t.Parallel()
	s := &Sandbox{
		Metadata:  &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "mode-test"}},
		Resources: &Resources{memory: uffd.NewNoopMemory(1<<30, 2<<20)},
	}
	s.labelBalloonMode(t.Context())
	assert.Equal(t, "unknown", s.BalloonMode())

	s.StampBalloonMode(userfaultfd.BalloonModeHinting)
	assert.Equal(t, "hinting", s.BalloonMode())
}

// A mode stamped from the template or the boot config is not re-derived: the
// device read only fills in unknown.
func TestLabelBalloonMode_KeepsKnownMode(t *testing.T) {
	t.Parallel()
	s := &Sandbox{
		Metadata:  &Metadata{Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "mode-known"}},
		Resources: &Resources{memory: uffd.NewNoopMemory(1<<30, 2<<20)},
	}
	s.StampBalloonMode(userfaultfd.BalloonModeNone)
	s.labelBalloonMode(t.Context())
	assert.Equal(t, "none", s.BalloonMode())
}

// ResolveBalloonMode reads the device once per process: the answer is
// stamped and a second call does not read again; a failed read stays
// unknown and leaves the stamp alone; a stamp the device contradicts is
// replaced.
func TestResolveBalloonMode(t *testing.T) {
	t.Parallel()

	t.Run("reads once and stamps", func(t *testing.T) {
		t.Parallel()
		reads := 0
		s := NewBalloonTestSandbox("resolve-once", "v1.14-0.2.1", func(context.Context) (fc.BalloonCaps, error) {
			reads++

			return fc.BalloonCaps{Hinting: true}, nil
		})
		assert.Equal(t, userfaultfd.BalloonModeHinting, s.ResolveBalloonMode(t.Context()))
		assert.Equal(t, "hinting", s.BalloonMode())
		// The process caches the config; the seam has to as well to mirror it.
		s.readBalloonCaps = func(context.Context) (fc.BalloonCaps, error) {
			reads++

			return fc.BalloonCaps{Hinting: true}, nil
		}
		assert.Equal(t, userfaultfd.BalloonModeHinting, s.ResolveBalloonMode(t.Context()))
		assert.Equal(t, 2, reads, "each resolve is one read of the per-process cache")
	})

	t.Run("failed read stays unknown, stamp untouched", func(t *testing.T) {
		t.Parallel()
		s := NewBalloonTestSandbox("resolve-fail", "v1.14-0.2.1", func(context.Context) (fc.BalloonCaps, error) {
			return fc.BalloonCaps{}, errors.New("api busy")
		})
		s.StampBalloonMode(userfaultfd.BalloonModeNone)
		assert.Equal(t, userfaultfd.BalloonModeUnknown, s.ResolveBalloonMode(t.Context()))
		assert.Equal(t, "none", s.BalloonMode())
	})

	t.Run("device corrects the stamp", func(t *testing.T) {
		t.Parallel()
		s := NewBalloonTestSandbox("resolve-correct", "v1.14-0.2.1", func(context.Context) (fc.BalloonCaps, error) {
			return fc.BalloonCaps{Reporting: true}, nil
		})
		s.StampBalloonMode(userfaultfd.BalloonModeHinting)
		assert.Equal(t, userfaultfd.BalloonModeReporting, s.ResolveBalloonMode(t.Context()))
		assert.Equal(t, "reporting", s.BalloonMode())
	})

	t.Run("read bounded by the resolve timeout", func(t *testing.T) {
		t.Parallel()
		s := NewBalloonTestSandbox("resolve-slow", "v1.14-0.2.1", func(ctx context.Context) (fc.BalloonCaps, error) {
			<-ctx.Done()

			return fc.BalloonCaps{}, ctx.Err()
		})
		start := time.Now()
		assert.Equal(t, userfaultfd.BalloonModeUnknown, s.ResolveBalloonMode(t.Context()))
		assert.Less(t, time.Since(start), 2*balloonModeResolveTimeout)
	})
}

// labelBalloonMode primes the device read whether or not a stamp exists, so a
// checkpoint finds a cache hit; a stamp the device contradicts is corrected.
func TestLabelBalloonMode_PrimesAndCorrects(t *testing.T) {
	t.Parallel()
	reads := 0
	s := NewBalloonTestSandbox("label-prime", "v1.14-0.2.1", func(context.Context) (fc.BalloonCaps, error) {
		reads++

		return fc.BalloonCaps{Reporting: true}, nil
	})
	s.StampBalloonMode(userfaultfd.BalloonModeHinting)
	s.labelBalloonMode(t.Context())
	assert.Equal(t, 1, reads, "the device is read even though a stamp exists")
	assert.Equal(t, "reporting", s.BalloonMode())
}

// A failed checkpoint-time read is not repeated on every checkpoint: the mode
// stays unknown without a read until the retry window passes.
func TestResolveBalloonMode_FailedReadIsNotRetriedAtOnce(t *testing.T) {
	t.Parallel()
	reads := 0
	s := NewBalloonTestSandbox("resolve-memo", "v1.14-0.2.1", func(context.Context) (fc.BalloonCaps, error) {
		reads++

		return fc.BalloonCaps{}, errors.New("api busy")
	})
	assert.Equal(t, userfaultfd.BalloonModeUnknown, s.ResolveBalloonMode(t.Context()))
	assert.Equal(t, userfaultfd.BalloonModeUnknown, s.ResolveBalloonMode(t.Context()))
	assert.Equal(t, 1, reads, "the second resolve inside the retry window reads nothing")

	s.balloonReadRetryAt.Store(time.Now().Add(-time.Second).UnixNano())
	assert.Equal(t, userfaultfd.BalloonModeUnknown, s.ResolveBalloonMode(t.Context()))
	assert.Equal(t, 2, reads, "past the window the device is asked again")
}

// A prime that succeeds after a failed checkpoint-time read reopens the
// device read: the next resolve does not sit out the retry window.
func TestResolveBalloonMode_PrimeClearsRetry(t *testing.T) {
	t.Parallel()
	fail := true
	s := NewBalloonTestSandbox("resolve-prime-clears", "v1.14-0.2.1", func(context.Context) (fc.BalloonCaps, error) {
		if fail {
			return fc.BalloonCaps{}, errors.New("api busy")
		}

		return fc.BalloonCaps{Hinting: true}, nil
	})
	assert.Equal(t, userfaultfd.BalloonModeUnknown, s.ResolveBalloonMode(t.Context()))
	fail = false
	s.labelBalloonMode(t.Context())
	assert.Equal(t, userfaultfd.BalloonModeHinting, s.ResolveBalloonMode(t.Context()), "the prime's success reopens the checkpoint read")
}
