package cpus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/reaper"
)

// fakeCPUSysfs is an in-memory CPU hotplug interface. CPU0 is always online and cannot go
// offline, as on the guest kernel.
type fakeCPUSysfs struct {
	mu     sync.Mutex
	online []bool
	err    error
	// hook runs before each SetOnline; an error fails the change, and blocking stalls it.
	hook func(cpu int, online bool) error
	// inert CPUs accept writes without changing state, as the kernel does for a CPU
	// whose device already holds the requested state.
	inert map[int]bool
}

func newFakeCPUSysfs(possible, online int) *fakeCPUSysfs {
	f := &fakeCPUSysfs{online: make([]bool, possible)}
	for cpu := range online {
		f.online[cpu] = true
	}

	return f
}

func (f *fakeCPUSysfs) Possible() ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}

	cpus := make([]int, len(f.online))
	for cpu := range cpus {
		cpus[cpu] = cpu
	}

	return cpus, nil
}

func (f *fakeCPUSysfs) Online() ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}

	return f.onlineLocked(), nil
}

func (f *fakeCPUSysfs) SetOnline(cpu int, online bool) error {
	f.mu.Lock()
	hook := f.hook
	f.mu.Unlock()
	if hook != nil {
		if err := hook(cpu, online); err != nil {
			return err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if cpu <= 0 || cpu >= len(f.online) {
		return fmt.Errorf("cpu%d has no online file", cpu)
	}
	if !f.inert[cpu] {
		f.online[cpu] = online
	}

	return nil
}

func (f *fakeCPUSysfs) setHook(hook func(cpu int, online bool) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = hook
}

func (f *fakeCPUSysfs) onlineCPUs() []int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.onlineLocked()
}

func (f *fakeCPUSysfs) onlineLocked() []int {
	var cpus []int
	for cpu, on := range f.online {
		if on {
			cpus = append(cpus, cpu)
		}
	}

	return cpus
}

// failFirst fails the first n changes, then lets them through.
func failFirst(n int32) func(int, bool) error {
	var calls atomic.Int32

	return func(int, bool) error {
		if calls.Add(1) <= n {
			return errors.New("injected EBUSY")
		}

		return nil
	}
}

func failAlways(int, bool) error { return errors.New("injected EIO") }

func newTestManager(t *testing.T, sys *fakeCPUSysfs) *manager {
	t.Helper()

	m, ok := New(zerolog.Nop(), &reaper.Registry{}, WithTargetFile(filepath.Join(t.TempDir(), "cpu-target"))).(*manager)
	require.True(t, ok)
	m.sys = sys
	m.retryBase, m.retryCap, m.maxStalled = 10*time.Millisecond, 40*time.Millisecond, 4

	return m
}

// converged is how a consumer reads the state: no target, or the online count matches.
func converged(st State) bool {
	return st.Target == 0 || st.Online == st.Target
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, 5*time.Millisecond)
}

func eventuallyOnline(t *testing.T, sys *fakeCPUSysfs, n int) {
	t.Helper()
	eventually(t, func() bool { return len(sys.onlineCPUs()) == n })
}

// newGate returns wait, which blocks until open is called or the test ends.
func newGate(t *testing.T) (wait, open func()) {
	t.Helper()

	ch := make(chan struct{})
	open = sync.OnceFunc(func() { close(ch) })
	t.Cleanup(open)

	return func() { <-ch }, open
}

func TestReconcile(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		possible   int
		online     []int
		target     int
		wantOnline []int
	}{
		"grow onlines the lowest offline CPUs":    {16, []int{0, 1, 2, 3}, 8, []int{0, 1, 2, 3, 4, 5, 6, 7}},
		"grow fills gaps before extending":        {8, []int{0, 1, 3, 5}, 6, []int{0, 1, 2, 3, 4, 5}},
		"shrink offlines the highest online CPUs": {16, []int{0, 1, 2, 3, 4, 5, 6, 7}, 4, []int{0, 1, 2, 3}},
		"shrink takes the highest across gaps":    {8, []int{0, 1, 3, 5}, 2, []int{0, 1}},
		"matching target writes nothing":          {16, []int{0, 1, 2, 3}, 4, []int{0, 1, 2, 3}},
		"all possible CPUs":                       {8, []int{0, 1}, 8, []int{0, 1, 2, 3, 4, 5, 6, 7}},
		"target 1 keeps CPU0":                     {8, []int{0, 1, 2, 3}, 1, []int{0}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sys := newFakeCPUSysfs(tc.possible, 0)
			for _, cpu := range tc.online {
				sys.online[cpu] = true
			}
			m := newTestManager(t, sys)
			require.NoError(t, m.SetTarget(tc.target))

			// Bounded so a reconcile that never settles fails here, not at the suite timeout.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := m.reconcile(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.wantOnline, sys.onlineCPUs())
			assert.Equal(t, State{Online: len(tc.wantOnline), Possible: tc.possible, Target: tc.target}, m.Status())
		})
	}
}

// A write the kernel accepts without effect must end the run as an error, or the loop
// would redo the same CPU forever.
func TestWriteWithoutEffectIsAnError(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(8, 4)
	sys.inert = map[int]bool{5: true}
	m := newTestManager(t, sys)
	require.NoError(t, m.SetTarget(8))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	changed, err := m.reconcile(ctx)

	require.ErrorContains(t, err, "cpu5: write reported success")
	assert.Equal(t, 1, changed, "cpu4 came up before the inert one")
	assert.Equal(t, []int{0, 1, 2, 3, 4}, sys.onlineCPUs())
}

func TestSetTargetRejectsOutOfRange(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	m := newTestManager(t, sys)

	for _, n := range []int{0, -1, 17} {
		require.Error(t, m.SetTarget(n), n)
	}
	_, err := os.Stat(m.targetFile)
	require.ErrorIs(t, err, os.ErrNotExist, "a rejected target must not be persisted")
	assert.Equal(t, []int{0, 1, 2, 3}, sys.onlineCPUs())
	assert.True(t, converged(m.Status()), "no target means nothing to converge on")
}

func TestStartAppliesTargetInBackground(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	m := newTestManager(t, sys)
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(12))

	eventuallyOnline(t, sys, 12)
	eventually(t, func() bool {
		st := m.Status()

		return converged(st) && st.Attempts == 1
	})
}

// A target that changes mid-run takes effect before the next CPU, not after the run.
// Growing 4->12, the hook lowers the target to 6 as cpu6 comes up, so the worker onlines
// 4, 5, 6 and offlines 6 again: four writes, where finishing the old run first would
// take fourteen.
func TestTargetReReadBeforeEachCPU(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	m := newTestManager(t, sys)
	var writes atomic.Int32
	sys.setHook(func(cpu int, online bool) error {
		writes.Add(1)
		if cpu == 6 && online {
			return m.SetTarget(6)
		}

		return nil
	})
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(12))

	eventuallyOnline(t, sys, 6)
	// Online == Target also holds mid-run, after the hook lowers the target and before
	// cpu6 comes up; only a finished run has a settled write count.
	eventually(t, func() bool {
		st := m.Status()

		return converged(st) && st.Attempts > 0
	})
	assert.Equal(t, int32(4), writes.Load())
}

func TestRetriesUntilConverged(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	sys.setHook(failFirst(2))
	m := newTestManager(t, sys)
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(8))

	eventuallyOnline(t, sys, 8)
	eventually(t, func() bool { return m.Status().Attempts == 3 }) // two failed runs and the converging one
}

func TestGivesUpWhenStalledUntilNewTarget(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	sys.setHook(failAlways)
	m := newTestManager(t, sys)
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(8))

	eventually(t, func() bool { return m.Status().Attempts == 4 })
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 4, m.Status().Attempts, "no retries after maxStalled runs without progress")
	assert.Equal(t, []int{0, 1, 2, 3}, sys.onlineCPUs())

	sys.setHook(nil)
	require.NoError(t, m.SetTarget(6))

	eventuallyOnline(t, sys, 6)
	eventually(t, func() bool { return m.Status().Attempts == 1 }) // a new target starts a fresh budget
}

// A run that brings a CPU up before failing resets the stall count, so the worker still
// gets maxStalled runs without progress afterwards: five attempts in all, not four.
func TestProgressResetsStallBudget(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	var calls atomic.Int32
	sys.setHook(func(int, bool) error {
		if calls.Add(1) == 1 {
			return nil
		}

		return errors.New("injected EIO")
	})
	m := newTestManager(t, sys)
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(8))

	eventually(t, func() bool { return m.Status().Attempts == 5 })
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 5, m.Status().Attempts, "no retries after maxStalled runs without progress")
	assert.Len(t, sys.onlineCPUs(), 5)
}

// Every other write fails, so every run but the last ends in an error, yet each brings a
// CPU up. That is more failed runs than maxStalled, and the worker must still get there.
// A new target that lands while the worker is backing off from the old one must get a
// full budget, not inherit the old target's stall count.
func TestNewTargetDuringBackoffGetsFreshBudget(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	sys.setHook(failAlways)
	m := newTestManager(t, sys)
	m.retryBase, m.retryCap = 300*time.Millisecond, 300*time.Millisecond // long enough to land SetTarget inside a backoff
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(8))
	eventually(t, func() bool { return m.Status().Attempts == 3 }) // three of four stalled runs spent; now backing off

	require.NoError(t, m.SetTarget(6))

	// With a fresh budget the new target gets maxStalled runs before the worker gives up.
	// Inheriting the old count would stop it after one.
	eventually(t, func() bool { return m.Status().Attempts == 4 })
}

func TestSlowProgressIsNotGivenUp(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	var calls atomic.Int32
	sys.setHook(func(int, bool) error {
		if calls.Add(1)%2 == 1 {
			return errors.New("injected EBUSY")
		}

		return nil
	})
	m := newTestManager(t, sys)
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(12))

	eventuallyOnline(t, sys, 12)
	// One run with no progress, seven that each failed after one CPU, and the converging one.
	eventually(t, func() bool { return m.Status().Attempts == 9 })
}

func TestBackoffDoublesAndCaps(t *testing.T) {
	t.Parallel()

	m := newTestManager(t, newFakeCPUSysfs(4, 4))
	m.retryBase, m.retryCap = time.Second, 5*time.Second

	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 5 * time.Second, 9: 5 * time.Second} {
		assert.Equal(t, want, m.backoff(attempt), attempt)
	}
}

func TestStartResumesPersistedTarget(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(16, 4)
	first := newTestManager(t, sys)
	require.NoError(t, first.SetTarget(12))

	restarted := newTestManager(t, sys)
	restarted.targetFile = first.targetFile
	restarted.Start(t.Context())

	eventuallyOnline(t, sys, 12)
	assert.Equal(t, 12, restarted.Status().Target)
}

func TestStartIgnoresInvalidPersistedTarget(t *testing.T) {
	t.Parallel()

	for name, content := range map[string]string{
		"not a number":      "lots\n",
		"zero":              "0",
		"negative":          "-3",
		"above possible":    "17",
		"empty":             "",
		"longer than bound": strings.Repeat("1", 1<<20),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sys := newFakeCPUSysfs(16, 4)
			m := newTestManager(t, sys)
			require.NoError(t, os.WriteFile(m.targetFile, []byte(content), 0o644))

			m.Start(t.Context())

			assert.Equal(t, 0, m.Status().Target)
			time.Sleep(50 * time.Millisecond)
			assert.Len(t, sys.onlineCPUs(), 4)
		})
	}
}

// A write stuck in the kernel blocks the worker but nothing else: Status keeps
// answering and reports how long the write has been pending.
func TestStuckWriteIsReportedPending(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(8, 4)
	wait, unblock := newGate(t)
	sys.setHook(func(cpu int, _ bool) error {
		if cpu == 5 {
			wait()
		}

		return nil
	})
	m := newTestManager(t, sys)
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(8))

	eventually(t, func() bool { return m.Status().WritePending > 50*time.Millisecond })
	assert.Equal(t, []int{0, 1, 2, 3, 4}, sys.onlineCPUs(), "CPUs before the stuck one came up")

	unblock()

	eventuallyOnline(t, sys, 8)
	eventually(t, func() bool { return m.Status().WritePending == 0 })
}

// The target drops back while a bring-up is stuck. When the bring-up finally lands, the
// worker re-reads the target and undoes it.
func TestTargetChangedDuringStuckWriteWins(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(4, 2)
	wait, unblock := newGate(t)
	var offlined atomic.Bool
	sys.setHook(func(cpu int, online bool) error {
		switch {
		case cpu == 2 && online:
			wait()
		case cpu == 2:
			offlined.Store(true)
		}

		return nil
	})
	m := newTestManager(t, sys)
	m.Start(t.Context())

	require.NoError(t, m.SetTarget(3))
	eventually(t, func() bool { return m.Status().WritePending > 0 })
	require.NoError(t, m.SetTarget(2))

	unblock()

	eventually(t, offlined.Load)
	eventually(t, func() bool { return converged(m.Status()) })
	assert.Equal(t, []int{0, 1}, sys.onlineCPUs())
}

// An upgrade can land while the previous envd's write is still in the kernel, after the
// target that write served was lowered. The new worker must let that write land before
// reconciling, or it stops on a count the write is about to change.
func TestInheritedWriteLandsBeforeReconciling(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(4, 2)
	var writes atomic.Int32
	sys.setHook(func(int, bool) error {
		writes.Add(1)

		return nil
	})
	m := newTestManager(t, sys)
	writer := exec.CommandContext(t.Context(), "cat")
	stdin, err := writer.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, writer.Start())
	pid := writer.Process.Pid
	require.NoError(t, writer.Process.Release())
	m.children.Adopt(pid)
	require.NoError(t, m.SetTarget(2))

	m.Start(t.Context())

	eventually(t, func() bool { return m.Status().WritePending > 0 })
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, writes.Load(), "nothing is written while the inherited write runs")

	sys.mu.Lock()
	sys.online[2] = true // the inherited write lands, then its process exits
	sys.mu.Unlock()
	require.NoError(t, stdin.Close())

	eventually(t, func() bool { return len(sys.onlineCPUs()) == 2 && converged(m.Status()) })
	assert.Equal(t, []int{0, 1}, sys.onlineCPUs())
	assert.Equal(t, time.Duration(0), m.Status().WritePending)
}

// When the inherited write leaves nothing to do, nothing else clears the pending time.
func TestInheritedWriteStopsBeingPendingOnceReaped(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(4, 2)
	m := newTestManager(t, sys)
	writer := exec.CommandContext(t.Context(), "cat")
	stdin, err := writer.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, writer.Start())
	pid := writer.Process.Pid
	require.NoError(t, writer.Process.Release())
	m.children.Adopt(pid)
	m.Start(t.Context())

	eventually(t, func() bool { return m.Status().WritePending > 0 })
	require.NoError(t, stdin.Close())
	eventually(t, func() bool { return m.Status().WritePending == 0 })
}

func TestStatusWarnsOnceWithoutCPUMasks(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	m := newTestManager(t, &fakeCPUSysfs{err: errors.New("no sysfs")})
	m.logger = zerolog.New(&logs)

	for range 3 {
		assert.Equal(t, State{}, m.Status())
	}

	assert.Equal(t, 1, strings.Count(logs.String(), "cannot read CPU masks"))
}

func TestNoopManager(t *testing.T) {
	t.Parallel()

	n := NewNoopManager()
	require.NoError(t, n.SetTarget(64))
	assert.Equal(t, State{}, n.Status())
}
