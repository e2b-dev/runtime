package sandbox

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

const (
	balloonModeReadTimeout = 5 * time.Second
	// balloonModeResolveTimeout bounds the device read a checkpoint decision
	// makes; the read is cached per process, so it is paid once.
	balloonModeResolveTimeout = 500 * time.Millisecond
	// balloonModeRetryAfter is how long a failed checkpoint-time read keeps
	// the mode unknown before the device is asked again.
	balloonModeRetryAfter = 30 * time.Second
)

// balloonCapsReader is the device read, or nil for a sandbox without one.
func (s *Sandbox) balloonCapsReader() func(context.Context) (fc.BalloonCaps, error) {
	if s.readBalloonCaps != nil {
		return s.readBalloonCaps
	}
	if s.process == nil {
		return nil
	}

	return s.process.BalloonCaps
}

// labelBalloonMode primes the per-process balloon read once the process is
// up, off the start path, so the checkpoint decision finds a cache hit, and
// stamps the mode: onto an unknown stamp (a template built before the
// metadata field), or over a stamp the device contradicts.
func (s *Sandbox) labelBalloonMode(ctx context.Context) {
	read := s.balloonCapsReader()
	if read == nil {
		return
	}
	stamped := s.BalloonModeValue()
	mode, err := s.readBalloonMode(ctx, read, balloonModeReadTimeout)
	switch {
	case err != nil && ctx.Err() == nil:
		sbxlogger.I(s).Warn(ctx, "balloon mode unread after start", zap.Error(err), zap.String("stamped", stamped.String()))
	case err == nil && stamped != userfaultfd.BalloonModeUnknown && stamped != mode:
		sbxlogger.I(s).Warn(ctx, "balloon mode stamp disagrees with the device; device wins",
			zap.String("stamped", stamped.String()), zap.String("device", mode.String()))
	}
}

// readBalloonMode reads the mode from the device within timeout and stamps it.
func (s *Sandbox) readBalloonMode(ctx context.Context, read func(context.Context) (fc.BalloonCaps, error), timeout time.Duration) (userfaultfd.BalloonMode, error) {
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	caps, err := read(readCtx)
	if err != nil {
		return userfaultfd.BalloonModeUnknown, err
	}
	mode := balloonModeOf(caps)
	s.StampBalloonMode(mode)
	// A successful read from any caller reopens the checkpoint-time read.
	s.balloonReadRetryAt.Store(0)

	return mode, nil
}

// DeferredMemoryExport is whether an in-place checkpoint of this sandbox
// would export memory through the CoW window, evaluated with the sandbox's
// own flag context. The checkpoint gate and the export must agree on it, so
// both ask here.
func (s *Sandbox) DeferredMemoryExport(ctx context.Context) bool {
	return s.featureFlags.BoolFlag(ctx, featureflags.DeferMemoryExportFlag, sandboxLDContext(s.Runtime, s.Config))
}

// ResolveBalloonMode is the mode for a decision that must not disagree with
// the VM: the device's answer, from one bounded read cached per process
// (primed by labelBalloonMode after start, so this is normally a cache hit).
// A stamp the device contradicts is corrected and logged. Unknown when there
// is no process or the read fails; a failed read is not retried for
// balloonModeRetryAfter, so an unreadable balloon does not cost every
// checkpoint the full bound. The stamp is a label, never a stand-in.
func (s *Sandbox) ResolveBalloonMode(ctx context.Context) userfaultfd.BalloonMode {
	read := s.balloonCapsReader()
	if read == nil {
		return userfaultfd.BalloonModeUnknown
	}
	if retryAt := s.balloonReadRetryAt.Load(); retryAt != 0 && time.Now().UnixNano() < retryAt {
		return userfaultfd.BalloonModeUnknown
	}
	stamped := s.BalloonModeValue()
	mode, err := s.readBalloonMode(ctx, read, balloonModeResolveTimeout)
	if err != nil {
		if ctx.Err() == nil {
			s.balloonReadRetryAt.Store(time.Now().Add(balloonModeRetryAfter).UnixNano())
			sbxlogger.I(s).Warn(ctx, "balloon mode unread at checkpoint; treated as unknown", zap.Error(err))
		}

		return userfaultfd.BalloonModeUnknown
	}
	if stamped != userfaultfd.BalloonModeUnknown && stamped != mode {
		sbxlogger.I(s).Warn(ctx, "balloon mode stamp disagrees with the device; device wins",
			zap.String("stamped", stamped.String()), zap.String("device", mode.String()))
	}

	return mode
}

// SetSyncWPForTest marks the sandbox as resumed with synchronous write
// protection, for tests outside this package that drive the checkpoint gate.
func (s *Sandbox) SetSyncWPForTest(v bool) {
	s.useSyncWP = v
}

// SetFeatureFlagsForTest gives the sandbox the flag client its flag reads use.
func (s *Sandbox) SetFeatureFlagsForTest(ff *featureflags.Client) {
	s.featureFlags = ff
}

// NewBalloonTestSandbox is a sandbox whose balloon "device" answers read; for
// tests of the code that decides on the balloon mode.
func NewBalloonTestSandbox(id, firecrackerVersion string, read func(context.Context) (fc.BalloonCaps, error)) *Sandbox {
	return &Sandbox{
		Metadata: &Metadata{
			Config:  NewConfig(Config{FirecrackerConfig: fc.Config{FirecrackerVersion: firecrackerVersion}}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: id},
		},
		Resources:       &Resources{memory: uffd.NewNoopMemory(1<<30, 2<<20)},
		readBalloonCaps: read,
	}
}

func balloonModeOf(caps fc.BalloonCaps) userfaultfd.BalloonMode {
	switch {
	case caps.Reporting:
		return userfaultfd.BalloonModeReporting
	case caps.Hinting:
		return userfaultfd.BalloonModeHinting
	default:
		return userfaultfd.BalloonModeNone
	}
}

// StampBalloonMode records the mode the device runs, on the sandbox and its
// serve metrics: from the template at resume, the configuration at boot, or
// a device read.
func (s *Sandbox) StampBalloonMode(mode userfaultfd.BalloonMode) {
	s.balloonMode.Store(uint32(mode))
	if s.Resources == nil {
		return
	}
	if labeler, ok := s.Resources.memory.(uffd.BalloonModeLabeler); ok {
		labeler.SetBalloonMode(mode)
	}
}

// BalloonModeValue is the stamped mode; unknown before any source landed.
func (s *Sandbox) BalloonModeValue() userfaultfd.BalloonMode {
	return userfaultfd.BalloonMode(s.balloonMode.Load())
}

// BalloonMode is the cohort label for spans and counters: reporting, hinting,
// none, or unknown before the read landed.
func (s *Sandbox) BalloonMode() string {
	return s.BalloonModeValue().String()
}
