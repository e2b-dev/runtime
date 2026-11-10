//go:build linux

package fc

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestClassifyExit(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		info     *ExitInfo
		expected CrashCause
	}{
		{
			name:     "never reaped",
			info:     nil,
			expected: CrashCauseUnknown,
		},
		{
			name:     "guest reset counted in the last metrics line",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsRead: true, GuestResets: 1},
			expected: CrashCauseGuestReset,
		},
		{
			// A reset an earlier periodic flush carried still counts when the
			// last line never arrived.
			name:     "guest reset counted before the metrics stopped",
			info:     &ExitInfo{Exited: true, ExitCode: 0, GuestResets: 1},
			expected: CrashCauseGuestReset,
		},
		{
			name:     "guest crash reported, then a reset",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsRead: true, GuestResets: 1, GuestCrashes: 1, GuestCrashCode: 0xef},
			expected: CrashCauseGuestCrash,
		},
		{
			// A crash report without the reset that restarts the guest is not
			// a restart.
			name:     "guest crash reported, no reset",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsRead: true, GuestCrashes: 1, GuestCrashCode: 0xef},
			expected: CrashCauseCleanExit,
		},
		{
			name:     "guest power-off after a reported crash",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsRead: true, GuestResets: 1, GuestCrashes: 1, GuestPowerOffs: 1},
			expected: CrashCauseGuestPowerOff,
		},
		{
			name:     "guest power-off",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsRead: true, GuestPowerOffs: 1},
			expected: CrashCauseGuestPowerOff,
		},
		{
			// A guest that reset and then powered off ended on the power-off.
			name:     "guest power-off after a reset",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsRead: true, GuestResets: 1, GuestPowerOffs: 1},
			expected: CrashCauseGuestPowerOff,
		},
		{
			name:     "firecracker failed after a guest power-off",
			info:     &ExitInfo{Exited: true, ExitCode: 1, MetricsRead: true, GuestPowerOffs: 1},
			expected: CrashCauseExitError,
		},
		{
			// A triple fault ends Firecracker without a reset.
			name:     "guest ended without a reset",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsRead: true},
			expected: CrashCauseCleanExit,
		},
		{
			name:     "clean exit whose metrics were not read to the end",
			info:     &ExitInfo{Exited: true, ExitCode: 0, MetricsAwaited: true},
			expected: CrashCauseUnknown,
		},
		{
			// A reset does not explain Firecracker failing afterwards.
			name:     "firecracker failed after a guest reset",
			info:     &ExitInfo{Exited: true, ExitCode: 1, MetricsRead: true, GuestResets: 1},
			expected: CrashCauseExitError,
		},
		{
			name:     "firecracker failed",
			info:     &ExitInfo{Exited: true, ExitCode: 1},
			expected: CrashCauseExitError,
		},
		{
			name:     "killed by someone else",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGKILL},
			expected: CrashCauseExternalSignal,
		},
		{
			name:     "terminated by someone else",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGTERM},
			expected: CrashCauseExternalSignal,
		},
		{
			name:     "killed by our own stop",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGKILL, SentByStop: true},
			expected: CrashCauseRequestedSignal,
		},
		{
			// A clean exit is the guest's doing either way.
			name:     "clean exit during our stop",
			info:     &ExitInfo{Exited: true, ExitCode: 0, SentByStop: true, MetricsRead: true},
			expected: CrashCauseCleanExit,
		},
		{
			// Before Firecracker installs its handlers, a fault still kills
			// it by the signal.
			name:     "faulted on a memory access",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGBUS},
			expected: CrashCauseFault,
		},
		{
			// uffd failing to serve a page raises SIGBUS, which Firecracker
			// catches and exits 149 for; hugepage exhaustion has its own
			// alert and must not arrive as a Firecracker failure.
			name:     "caught a memory fault",
			info:     &ExitInfo{Exited: true, ExitCode: 149},
			expected: CrashCauseFault,
		},
		{
			name:     "caught a bad syscall",
			info:     &ExitInfo{Exited: true, ExitCode: 148},
			expected: CrashCauseFault,
		},
		{
			name:     "caught a segfault",
			info:     &ExitInfo{Exited: true, ExitCode: 150},
			expected: CrashCauseFault,
		},
		{
			name:     "caught an illegal instruction",
			info:     &ExitInfo{Exited: true, ExitCode: 157},
			expected: CrashCauseFault,
		},
		{
			// Nothing inside Firecracker raises SIGHUP.
			name:     "caught a hangup",
			info:     &ExitInfo{Exited: true, ExitCode: 156},
			expected: CrashCauseExternalSignal,
		},
		{
			name:     "exceeded a file size limit",
			info:     &ExitInfo{Exited: true, ExitCode: 151},
			expected: CrashCauseExitError,
		},
		{
			name:     "aborted itself",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGABRT, CoreDumped: true},
			expected: CrashCauseFault,
		},
		{
			// A fault during our teardown is still a fault: the SIGTERM we
			// sent is not what killed it.
			name:     "faulted while we were stopping it",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGSEGV, SentByStop: true},
			expected: CrashCauseFault,
		},
		{
			name:     "neither exited nor signaled",
			info:     &ExitInfo{},
			expected: CrashCauseUnknown,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, ClassifyExit(tt.info))
		})
	}
}

// Each crash cause maps to the guest event the host can name from it.
func TestCrashCauseGuestEvent(t *testing.T) {
	t.Parallel()

	for cause, event := range map[CrashCause]GuestEvent{
		CrashCauseGuestPowerOff:       GuestEventPowerOff,
		CrashCauseGuestReset:          GuestEventRebootOrCrash,
		CrashCauseGuestCrash:          GuestEventCrashWithRestart,
		CrashCauseCleanExit:           GuestEventCrashNoRestart,
		CrashCauseExitError:           GuestEventFailure,
		CrashCauseExternalSignal:      GuestEventFailure,
		CrashCauseFault:               GuestEventFailure,
		CrashCauseMemoryHandlerFailed: GuestEventFailure,
		CrashCauseRequestedSignal:     GuestEventFailure,
		CrashCauseUnknown:             GuestEventUnknown,
	} {
		assert.Equal(t, event, cause.GuestEvent(), cause)
	}
}

func TestExitInfoLogFields(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		info     *ExitInfo
		expected []zap.Field
	}{
		{
			name:     "never reaped",
			info:     nil,
			expected: nil,
		},
		{
			name: "signalled",
			info: &ExitInfo{Signaled: true, Signal: syscall.SIGKILL, CoreDumped: true},
			expected: []zap.Field{
				zap.String("fc_signal", "killed"),
				zap.Bool("fc_core_dumped", true),
			},
		},
		{
			name: "exited",
			info: &ExitInfo{Exited: true, ExitCode: 7, MetricsRead: true, GuestResets: 2},
			expected: []zap.Field{
				zap.Int("fc_exit_code", 7),
				zap.Uint64("fc_guest_resets", 2),
				zap.Uint64("fc_guest_poweroffs", 0),
				zap.Uint64("fc_guest_crashes", 0),
				zap.String("fc_guest_crash_code", "0x0"),
				zap.Bool("fc_metrics_read", true),
			},
		},
		{
			name:     "neither exited nor signaled",
			info:     &ExitInfo{},
			expected: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.info.LogFields())
		})
	}
}

// The recorded status must survive the branches that resolve Exit clean.
func TestHandleExitPublishesExitInfo(t *testing.T) {
	t.Parallel()

	t.Run("signalled exit resolves Exit without an error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		require.NoError(t, cmd.Process.Kill())

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce(), reaped: make(chan struct{})}
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))
		require.NoError(t, p.Exit.Error())

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.True(t, info.Signaled)
		assert.Equal(t, syscall.SIGKILL, info.Signal)
		assert.Equal(t, CrashCauseExternalSignal, ClassifyExit(info))
	})

	t.Run("non-zero exit resolves Exit with an error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 7")

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce(), reaped: make(chan struct{})}
		require.Error(t, p.handleExit(ctx, cmd.Wait()))
		require.Error(t, p.Exit.Error())

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, &ExitInfo{Exited: true, ExitCode: 7}, info)
		assert.Equal(t, CrashCauseExitError, ClassifyExit(info))
	})

	t.Run("clean exit", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce(), reaped: make(chan struct{})}
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))
		require.NoError(t, p.Exit.Error())

		assert.Equal(t, &ExitInfo{Exited: true, ExitCode: 0}, p.ExitInfo())
		assert.Equal(t, CrashCauseCleanExit, ClassifyExit(p.ExitInfo()))
	})

	t.Run("fault caught by firecracker's handler", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 149")

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce(), reaped: make(chan struct{})}
		require.Error(t, p.handleExit(ctx, cmd.Wait()))

		assert.Equal(t, &ExitInfo{Exited: true, ExitCode: 149}, p.ExitInfo())
		assert.Equal(t, CrashCauseFault, ClassifyExit(p.ExitInfo()))
	})
}

// The guest's resets reach ExitInfo only if the exit handler waits for the
// metrics reader to read Firecracker's last line, which arrives as it exits.
func TestHandleExitFoldsGuestResets(t *testing.T) {
	t.Parallel()

	// startReader starts the FIFO reader and returns the write end that
	// stands in for Firecracker's.
	startReader := func(t *testing.T, p *Process) *os.File {
		t.Helper()

		require.NoError(t, syscall.Mkfifo(p.metricsPath, 0o600))
		p.startMetricsFIFOReader(t.Context())

		w, err := os.OpenFile(p.metricsPath, os.O_WRONLY, os.ModeNamedPipe)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })

		return w
	}

	t.Run("resets in a periodic flush and in the last line", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		w := startReader(t, p)

		_, err := w.WriteString(`{"utc_timestamp_ms":1,"i8042":{"reset_count":1}}` + "\n")
		require.NoError(t, err)
		_, err = w.WriteString(`{"utc_timestamp_ms":2,"i8042":{"reset_count":0}}` + "\n")
		require.NoError(t, err)
		_, err = w.WriteString(`{"utc_timestamp_ms":3,"i8042":{"reset_count":1}}` + "\n")
		require.NoError(t, err)
		require.NoError(t, w.Close())

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.True(t, info.MetricsRead)
		assert.Equal(t, uint64(2), info.GuestResets)
		assert.Equal(t, CrashCauseGuestReset, ClassifyExit(info))
	})

	t.Run("no reset", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		w := startReader(t, p)

		_, err := w.WriteString(`{"utc_timestamp_ms":1,"i8042":{"reset_count":0}}` + "\n")
		require.NoError(t, err)
		require.NoError(t, w.Close())

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		assert.Equal(t, CrashCauseCleanExit, ClassifyExit(p.ExitInfo()))
	})

	t.Run("power-off in the last line", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		w := startReader(t, p)

		_, err := w.WriteString(`{"utc_timestamp_ms":1,"i8042":{"reset_count":0}}` + "\n")
		require.NoError(t, err)
		_, err = w.WriteString(`{"utc_timestamp_ms":2,"i8042":{"reset_count":0},"acpi_pm":{"poweroff_count":1}}` + "\n")
		require.NoError(t, err)
		require.NoError(t, w.Close())

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, uint64(1), info.GuestPowerOffs)
		assert.Equal(t, CrashCauseGuestPowerOff, ClassifyExit(info))
	})

	// The crash count is a per-flush delta; the code is the last crash's and
	// survives a later line that carries no code.
	t.Run("crash in a periodic flush", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		p.Versions.Guest = crashReportingGuest{guest.Default()}
		w := startReader(t, p)

		_, err := w.WriteString(`{"utc_timestamp_ms":1,"pvcrash":{"crash_count":1,"crash_code":239}}` + "\n")
		require.NoError(t, err)
		_, err = w.WriteString(`{"utc_timestamp_ms":2,"i8042":{"reset_count":1}}` + "\n")
		require.NoError(t, err)
		require.NoError(t, w.Close())

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, uint64(1), info.GuestCrashes)
		assert.Equal(t, uint64(0xef), info.GuestCrashCode)
		assert.Equal(t, CrashCauseGuestCrash, ClassifyExit(info))
	})

	// The default guest names no crash group: a line that carries one counts nothing.
	t.Run("a crash group the profile does not name", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		w := startReader(t, p)

		_, err := w.WriteString(`{"utc_timestamp_ms":1,"pvcrash":{"crash_count":1,"crash_code":239},"i8042":{"reset_count":1}}` + "\n")
		require.NoError(t, err)
		require.NoError(t, w.Close())

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, uint64(0), info.GuestCrashes)
		assert.Equal(t, CrashCauseGuestReset, ClassifyExit(info))
	})

	// Without the drain the exit does not wait for a reader that never ends,
	// and a clean exit stays clean_exit, as before the guest counters.
	t.Run("no drain asked for", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		p.drainOnExit.Store(false)
		startReader(t, p)

		start := time.Now()
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))
		assert.Less(t, time.Since(start), metricsDrainTimeout/2, "the exit did not wait for the reader")

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.False(t, info.MetricsRead)
		assert.False(t, info.MetricsAwaited)
		assert.Equal(t, CrashCauseCleanExit, ClassifyExit(info))
	})

	// A drain asked for only at the exit, as a flag turned on while the
	// process ran, still counts the last line.
	t.Run("drain decided at the exit", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		p.drainOnExit.Store(false)
		var asked atomic.Bool
		p.SetExitDrain(asked.Load)
		w := startReader(t, p)

		_, err := w.WriteString(`{"utc_timestamp_ms":1,"i8042":{"reset_count":1}}` + "\n")
		require.NoError(t, err)
		require.NoError(t, w.Close())
		asked.Store(true)

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.True(t, info.MetricsAwaited)
		assert.True(t, info.MetricsRead)
		assert.Equal(t, uint64(1), info.GuestResets)
		assert.Equal(t, CrashCauseGuestReset, ClassifyExit(info))
	})

	// A write end that outlives the process keeps the reader from the end of
	// the FIFO; the exit still resolves, with the metrics marked incomplete.
	t.Run("the last line never arrives", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")
		p := testStopProcess(t, cmd)
		startReader(t, p)

		start := time.Now()
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))
		assert.Less(t, time.Since(start), metricsDrainTimeout+time.Second)

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.False(t, info.MetricsRead)
		assert.Equal(t, CrashCauseUnknown, ClassifyExit(info))
	})
}

// Stop records the signals it sends, which is what separates our signal from a
// foreign one.
func TestProcessStopClaimsTheKill(t *testing.T) {
	t.Parallel()

	t.Run("claims its own kill however the reap is ordered", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		pid := cmd.Process.Pid

		p := testStopProcess(t, cmd)
		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			_ = p.handleExit(ctx, cmd.Wait())
		}()
		t.Cleanup(func() {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			<-waitDone
		})

		require.NoError(t, p.Stop(ctx))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, syscall.SIGTERM, info.Signal)
		assert.True(t, info.SentByStop)
		assert.Equal(t, CrashCauseRequestedSignal, ClassifyExit(info))
	})

	// Teardown calls Stop after the process is gone. Without the reap-time
	// sample that late claim relabels every external kill as ours.
	t.Run("does not claim an exit already classified", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		require.NoError(t, cmd.Process.Kill())

		p := testStopProcess(t, cmd)
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		// What the exit-wait goroutine does before anything classifies.
		require.NoError(t, p.Stop(ctx))
		assert.Zero(t, p.sentSignals.Load())

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.False(t, info.SentByStop)
		assert.Equal(t, CrashCauseExternalSignal, ClassifyExit(info))
	})

	// The resume path stops the sandbox as soon as uffd OR Firecracker exits,
	// so Stop can signal a Firecracker that something else already killed but
	// nothing has reaped yet. A zombie accepts the signal, so the record is
	// set; the terminating signal is still not the one we sent.
	t.Run("does not claim a kill that landed before its signal", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		require.NoError(t, syscall.Kill(cmd.Process.Pid, syscall.SIGKILL))
		waitForZombie(t, cmd.Process.Pid)

		p := testStopProcess(t, cmd)
		require.NoError(t, p.signal(syscall.SIGTERM), "a zombie accepts signals")

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, syscall.SIGKILL, info.Signal)
		assert.False(t, info.SentByStop)
		assert.Equal(t, CrashCauseExternalSignal, ClassifyExit(info))
	})
}

// crashReportingGuest reports its crashes through the pvcrash metrics group.
type crashReportingGuest struct{ guest.Profile }

func (crashReportingGuest) Reset() guest.Reset {
	return guest.Reset{Crash: guest.CrashReport{Group: "pvcrash", CountField: "crash_count", CodeField: "crash_code"}}
}

func testStopProcess(t *testing.T, cmd *exec.Cmd) *Process {
	t.Helper()

	p := &Process{
		cmd:         cmd,
		Exit:        utils.NewErrorOnce(),
		reaped:      make(chan struct{}),
		metricsDone: make(chan struct{}),
		files:       &storage.SandboxFiles{SandboxID: "test"},
		metricsPath: filepath.Join(t.TempDir(), "metrics.fifo"),
	}
	p.DrainMetricsOnExit()

	return p
}

// waitForZombie waits until pid has died and is waiting to be reaped.
func waitForZombie(t *testing.T, pid int) {
	t.Helper()

	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	require.Eventually(t, func() bool {
		stat, err := os.ReadFile(statPath)
		if err != nil {
			return false
		}

		// The state follows the parenthesised command name.
		rest := stat[bytes.LastIndexByte(stat, ')')+1:]

		return len(rest) > 1 && rest[1] == 'Z'
	}, 10*time.Second, time.Millisecond)
}
