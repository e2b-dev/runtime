//go:build linux

package cgroups

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTreeFixture builds a synthetic cgroup2 tree on disk and returns a manager rooted at
// it, with procSelfCgroup pointed at a file placing envd at selfPath.
//
// Real directories and real cgroup.freeze / cgroup.events files, not an in-memory fake:
// the walk's whole job is readdir plus read/write of those files, so a fake would test
// the traversal while skipping the part that talks to the kernel. Constructing the struct
// directly bypasses NewCgroup2Manager's statfs check, which is the only thing that needs
// an actual cgroup2 mount.
func newTreeFixture(t *testing.T, selfPath string, dirs ...string) (*WorkloadFreezer, string) {
	t.Helper()

	root := t.TempDir()
	// Every directory gets the interface files, intermediates included: "a/b/c" makes
	// three cgroups, and a test that asserts on "a/b" would otherwise fail to read it.
	mk := func(rel string) {
		full := filepath.Join(root, rel)
		require.NoError(t, os.MkdirAll(full, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(full, "cgroup.freeze"), []byte("0\n"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(full, "cgroup.events"), []byte("populated 1\nfrozen 0\n"), 0o644))
	}
	// The tree root deliberately gets NO cgroup.freeze / cgroup.events, because cgroup v2
	// does not create them on the mount root. A fixture that provides them there hides a
	// walk that reads the root's state -- which is a walk that does nothing on a real guest.
	require.NoError(t, os.MkdirAll(root, 0o755))
	for _, d := range dirs {
		parts := strings.Split(d, "/")
		for i := range parts {
			mk(filepath.Join(parts[:i+1]...))
		}
	}

	procFile := filepath.Join(t.TempDir(), "cgroup")
	require.NoError(t, os.WriteFile(procFile, []byte("0::"+selfPath+"\n"), 0o644))

	mgr := &Cgroup2Manager{
		rootPath:    root,
		cgroupFDs:   map[ProcessType]int{},
		cgroupPaths: map[ProcessType]string{},
	}
	// The static list is thawed unconditionally, so these must resolve.
	for _, pt := range WorkloadProcessTypes {
		p := filepath.Join(root, string(pt))
		require.NoError(t, os.MkdirAll(p, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(p, "cgroup.freeze"), []byte("0\n"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(p, "cgroup.events"), []byte("populated 1\nfrozen 0\n"), 0o644))
		mgr.cgroupPaths[pt] = p
	}

	f := NewWorkloadFreezer(mgr)
	f.procSelfCgroup = procFile

	return f, root
}

// freezeOnDisk marks a cgroup frozen the way the KERNEL would leave it: cgroup.freeze=1 on
// the target, and "frozen 1" in cgroup.events on the target AND every descendant.
//
// That second half is the part worth modelling. Freezing is hierarchical, so one write stops
// a whole subtree: descendants keep cgroup.freeze=0 while reporting frozen in
// cgroup.events. A fixture that only wrote the request would let a reader of the settled
// state look correct while being wrong about every nested cgroup -- which is exactly the bug
// the resume audit shipped with.
func freezeOnDisk(t *testing.T, root, rel string) {
	t.Helper()

	target := filepath.Join(root, rel)
	require.NoError(t, os.WriteFile(filepath.Join(target, "cgroup.freeze"), []byte("1\n"), 0o644))
	require.NoError(t, filepath.WalkDir(target, func(p string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}

		return os.WriteFile(filepath.Join(p, "cgroup.events"), []byte("populated 1\nfrozen 1\n"), 0o644)
	}))
}

func frozenOnDisk(t *testing.T, root, rel string) bool {
	t.Helper()

	b, err := os.ReadFile(filepath.Join(root, rel, "cgroup.freeze"))
	require.NoError(t, err)

	// Trimmed, like FreezeRequestedAt does: the kernel's cgroup.freeze carries a trailing
	// newline and so do the fixtures, while writeCgroupProp writes the bare value.
	return strings.TrimSpace(string(b)) == "1"
}

func TestSelfCgroupPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    string
		wantErr bool
	}{
		{name: "cgroup2 only", content: "0::/system.slice/envd.service\n", want: "/root/system.slice/envd.service"},
		{name: "at the root", content: "0::/\n", want: "/root"},
		// A hybrid host still has v1 lines; only the 0:: one describes cgroup2.
		{
			name:    "ignores v1 lines",
			content: "12:pids:/system.slice\n1:name=systemd:/system.slice\n0::/system.slice/envd.service\n",
			want:    "/root/system.slice/envd.service",
		},
		// No 0:: line means the file is not what we think it is. Guessing the root would
		// make the walk freeze every top-level cgroup, envd's own included.
		{name: "no cgroup2 line", content: "1:name=systemd:/system.slice\n", wantErr: true},
		{name: "empty", content: "", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			procFile := filepath.Join(t.TempDir(), "cgroup")
			require.NoError(t, os.WriteFile(procFile, []byte(tc.content), 0o644))

			got, err := SelfCgroupPath(procFile, "/root")
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAncestorChain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		self string
		want []string
	}{
		{
			name: "nested",
			self: "/sys/fs/cgroup/system.slice/envd.service",
			want: []string{"/sys/fs/cgroup", "/sys/fs/cgroup/system.slice", "/sys/fs/cgroup/system.slice/envd.service"},
		},
		{name: "envd at the root", self: "/sys/fs/cgroup", want: []string{"/sys/fs/cgroup"}},
		// Outside the tree we are about to walk: exclude nothing but the root, and let the
		// caller notice the chain is implausibly short.
		{name: "outside the root", self: "/somewhere/else", want: []string{"/sys/fs/cgroup"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, AncestorChain("/sys/fs/cgroup", tc.self))
		})
	}
}

// TestSweepHierarchy_FreezesTheComplement is the core of the feature: everything that is
// not envd's own ancestry, and not needed by the resume, gets frozen — including cgroups
// the customer created that the static list never touched.
func TestSweepHierarchy_FreezesTheComplement(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice",
		"system.slice/envd.service",
		"system.slice/customer.service", // a customer unit under a slice
		"system.slice/systemd-journald.service",
		"system.slice/rpcbind.service",
		"system.slice/rpcbind.socket",
		"init.scope",
		"socats",
		"user.slice", // a shell session's own tree
		"niteshift",  // a raw mkdir at the true root
		"niteshift/deep/deeper",
	)

	// MaxWait 0: this asserts WHICH cgroups were written to, not whether they settled.
	// Confirmation is covered separately, against a manager whose state can be driven.
	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Equal(t, ModeHierarchy, res.Mode)

	// Frozen: the customer's cgroups, wherever they live.
	for _, rel := range []string{"system.slice/customer.service", "user.slice", "niteshift"} {
		assert.True(t, frozenOnDisk(t, root, rel), "%s should be frozen", rel)
	}

	// Not frozen: envd's own chain, or the resume depends on it.
	// The root is absent from this list on purpose: cgroup v2 gives it no cgroup.freeze at
	// all, so "the root was not frozen" is not something to read back -- it is guaranteed by
	// the kernel, and the fixture reproduces that by leaving the file out.
	for _, rel := range []string{"system.slice", "system.slice/envd.service"} {
		assert.False(t, frozenOnDisk(t, root, rel), "%s is on envd's chain and must stay live", rel)
	}
	for _, rel := range []string{
		"init.scope", "socats", "system.slice/systemd-journald.service",
		// rpcbind holds the local portmapper that rpc.statd registers with, and the
		// resume thaw runs after setupNFS -- measured: frozen rpcbind turns `rpcinfo -p`
		// from 0.145s into a timeout.
		"system.slice/rpcbind.service", "system.slice/rpcbind.socket",
	} {
		assert.False(t, frozenOnDisk(t, root, rel), "%s is allowlisted and must stay live", rel)
	}
	assert.Equal(t, 5, res.Allowlisted)

	// Not recursed into: freezing is hierarchical, so the write to niteshift already
	// stops its whole subtree. Writing to descendants would be wasted work, and the
	// absence of those writes is what proves the walk is breadth-bounded.
	assert.False(t, frozenOnDisk(t, root, "niteshift/deep"),
		"the walk must not descend below a frozen cgroup")
	assert.False(t, frozenOnDisk(t, root, "niteshift/deep/deeper"))
}

// TestSweepHierarchy_EnvdAtTheRoot covers the other plausible layout: envd in the root
// cgroup itself (0::/), where the ancestor chain is just the root and every top-level
// cgroup is therefore a candidate. Worth pinning separately because the chain-exclusion
// logic has nothing to exclude here, which is exactly when an off-by-one would freeze
// envd's own cgroup and wedge the guest.
func TestSweepHierarchy_EnvdAtTheRoot(t *testing.T) {
	t.Parallel()

	// system.slice is planted deliberately. With envd at the root it is NOT on envd's
	// chain, so a walk that only descended the chain would freeze it wholesale -- and
	// freezing is hierarchical, so that would stop journald and rpcbind underneath it even
	// though both are allowlisted, while still passing the exact-path check.
	f, root := newTreeFixture(t, "/",
		"workload-a", "workload-b", "init.scope",
		"system.slice",
		"system.slice/systemd-journald.service",
		"system.slice/rpcbind.service",
		"system.slice/customer.service",
	)

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Equal(t, ModeHierarchy, res.Mode)

	assert.True(t, frozenOnDisk(t, root, "workload-a"))
	assert.True(t, frozenOnDisk(t, root, "workload-b"))
	// Not read back: cgroup v2 creates no cgroup.freeze on the mount root, so the root being
	// unfrozen is a kernel guarantee rather than an assertion. What matters here is that the
	// sweep did not try -- covered by the per-cgroup expectations below.
	assert.False(t, frozenOnDisk(t, root, "init.scope"), "still allowlisted")

	// The load-bearing assertions for this layout.
	assert.False(t, frozenOnDisk(t, root, "system.slice"),
		"freezing system.slice would stop its allowlisted children through the hierarchy")
	assert.False(t, frozenOnDisk(t, root, "system.slice/systemd-journald.service"))
	assert.False(t, frozenOnDisk(t, root, "system.slice/rpcbind.service"))
	// ...while its non-allowlisted children are still frozen individually.
	assert.True(t, frozenOnDisk(t, root, "system.slice/customer.service"),
		"descending into system.slice must still freeze what is not allowlisted")
}

// TestSweepHierarchy_FallsBackWhenSelfIsUnknown covers the one input the walk cannot do
// without. Freezing a guessed complement could stop envd itself, and a frozen envd cannot
// answer the /init that would thaw it — so this degrades to the static list instead.
func TestSweepHierarchy_FallsBackWhenSelfIsUnknown(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service", "niteshift")
	// A /proc/self/cgroup with no cgroup2 line, for this freezer only.
	bad := filepath.Join(t.TempDir(), "cgroup")
	require.NoError(t, os.WriteFile(bad, []byte("1:name=systemd:/system.slice\n"), 0o644))
	f.procSelfCgroup = bad

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})

	require.Error(t, err, "the caller must learn the walk could not run")
	assert.Equal(t, ModeLegacy, res.Mode, "and that it got the legacy set instead")
	assert.False(t, frozenOnDisk(t, root, "niteshift"), "no guessing at the complement")
	for _, pt := range WorkloadProcessTypes {
		assert.True(t, frozenOnDisk(t, root, string(pt)), "the static list still froze")
	}
}

// TestSweepHierarchy_TruncatesAtTheBound pins that the bound is reported, not swallowed.
// A sweep that quietly covers less than it should is worse than today's behaviour,
// because today's narrow coverage is at least known.
func TestSweepHierarchy_TruncatesAtTheBound(t *testing.T) {
	t.Parallel()

	f, _ := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "a", "b", "c", "d", "e")

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy, MaxCgroups: 3})
	require.NoError(t, err)

	assert.True(t, res.Truncated, "the bound bit, so coverage is incomplete and must say so")
	assert.Equal(t, 3, res.Visited, "and it stopped exactly at the bound")
}

// TestUnfreeze_DiscoversFrozenCgroupsAnywhere is the thaw's central guarantee: it undoes what
// is actually frozen, not what this version would have frozen. That is what makes it
// correct when the freeze was performed by a different envd, in a different mode, under a
// different bound — none of which it can know.
func TestUnfreeze_DiscoversFrozenCgroupsAnywhere(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service",
		"legacy-frozen",          // frozen by some previous envd
		"deep/nested/far",        // frozen deep, where a breadth-bounded walk would not look
		"init.scope",             // allowlisted, but frozen anyway by an older version
		"system.slice/untouched", // never frozen
	)
	// A freeze of OURS is in effect: every sweep writes the static list, in both modes, so
	// that is the evidence the thaw uses to decide whether discovery is its business at all.
	freezeSweptSet(t, root)
	for _, rel := range []string{"legacy-frozen", "deep/nested/far", "init.scope"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, rel, "cgroup.freeze"), []byte("1\n"), 0o644))
	}

	res, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.True(t, res.Discovered)

	for _, rel := range []string{"legacy-frozen", "deep/nested/far"} {
		assert.False(t, frozenOnDisk(t, root, rel), "%s was frozen and must be thawed", rel)
	}
	// The allowlist is a freeze-side concept. If an older envd froze init.scope, refusing
	// to thaw it here would strand systemd for the sandbox's life.
	assert.False(t, frozenOnDisk(t, root, "init.scope"),
		"the thaw ignores the allowlist: it undoes observed state, whoever wrote it")
	assert.GreaterOrEqual(t, res.Thawed, 3, "at least the three planted cgroups")
}

// TestUnfreeze_LeavesEnvdsOwnChainAlone: thawing our own cgroup would be harmless today
// (it is never frozen), but the exclusion is what keeps the thaw honest if that ever
// changes — and it is the same chain the freeze excludes.
func TestUnfreeze_LeavesEnvdsOwnChainAlone(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service", "system.slice", "system.slice/envd.service")
	require.NoError(t, os.WriteFile(filepath.Join(root, "system.slice", "cgroup.freeze"), []byte("1\n"), 0o644))

	res, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)

	assert.True(t, frozenOnDisk(t, root, "system.slice"), "on envd's chain, so not the thaw's business")
	assert.Zero(t, res.Thawed)
}

// TestUnfreeze_TruncationIsReported: a truncated thaw may have left cgroups frozen, which
// is a stranded guest rather than a degradation. It must never pass quietly.
func TestUnfreeze_TruncationIsReported(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service", "a", "b", "c", "d", "e", "f")
	// Our freeze in effect, so the discovering walk -- the only thing that can truncate -- runs.
	freezeSweptSet(t, root)
	require.NoError(t, os.WriteFile(filepath.Join(root, "f", "cgroup.freeze"), []byte("1\n"), 0o644))

	res, err := f.UnfreezeReporting(t.Context(), 2)

	require.Error(t, err, "a truncated thaw is an error, not a quiet partial success")
	assert.True(t, res.Truncated)
}

// TestThawWatchdog_FiresWhenNoThawArrives covers the case nothing else can: an
// orchestrator that resumes and never calls /init leaves the guest frozen for its whole
// life, because envd is not restarted by a resume and has no other cue.
func TestThawWatchdog_FiresWhenNoThawArrives(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service", "system.slice", "system.slice/envd.service", "workload")

	fired := make(chan ThawResult, 1)
	f.SetThawWatchdog(30*time.Millisecond, func(res ThawResult, _ error) { fired <- res })

	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	require.True(t, frozenOnDisk(t, root, "workload"))

	select {
	case res := <-fired:
		assert.Positive(t, res.Thawed)
		assert.False(t, frozenOnDisk(t, root, "workload"), "the backstop must actually thaw")
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog never fired: a guest whose /init never arrives stays frozen")
	}
}

// TestThawWatchdog_DisarmedByAThaw is the other half: the ordinary path must not leave a
// timer that thaws a workload frozen by some later, unrelated pause.
func TestThawWatchdog_DisarmedByAThaw(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service", "system.slice", "system.slice/envd.service", "workload")

	fired := make(chan ThawResult, 1)
	f.SetThawWatchdog(50*time.Millisecond, func(res ThawResult, _ error) { fired <- res })

	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	require.NoError(t, f.Unfreeze(t.Context()))

	// Re-freeze with the watchdog disabled, so anything that thaws it now can only be a
	// leftover timer.
	f.SetThawWatchdog(0, nil)
	_, err = f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)

	select {
	case <-fired:
		t.Fatal("a thaw must disarm the watchdog; it fired against a later freeze")
	case <-time.After(200 * time.Millisecond):
		assert.True(t, frozenOnDisk(t, root, "workload"), "the second freeze must still stand")
	}
}

// freezeSweptSet marks frozen everything a real pre-pause sweep would have frozen in the
// fixture: the static user/pty cgroups the fixture always creates, plus the named extras.
// Without this the audit correctly reports user and ptys as escapes, because an unfrozen
// workload cgroup at resume IS an escape -- the fixture, not the audit, was unrealistic.
func freezeSweptSet(t *testing.T, root string, extra ...string) {
	t.Helper()
	for _, pt := range WorkloadProcessTypes {
		freezeOnDisk(t, root, string(pt))
	}
	for _, rel := range extra {
		freezeOnDisk(t, root, rel)
	}
}

// TestAuditFrozenState is the counter that would have caught both defects found in this
// area by other means: an allowlist missing rpcbind, and an allowlisted cgroup frozen
// transitively through an unwalked parent. Both present as violations > 0 on the first
// resume, which is the point of auditing rather than reasoning.
func TestAuditFrozenState(t *testing.T) {
	t.Parallel()

	t.Run("a correctly frozen guest reports nothing wrong", func(t *testing.T) {
		t.Parallel()

		f, root := newTreeFixture(t, "/system.slice/envd.service",
			"system.slice", "system.slice/envd.service",
			"system.slice/systemd-journald.service", "init.scope", "socats", "workload")
		freezeSweptSet(t, root, "workload")

		res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
		require.NoError(t, err)
		assert.True(t, res.Applicable)
		assert.Equal(t, 3, res.Frozen, "workload plus the two static cgroups")
		assert.Zero(t, res.Escaped)
		assert.Zero(t, res.Violations)
	})

	// The bug arm. A frozen allowlisted cgroup means the resume is running without
	// something it depends on -- rpcbind here, which nfsvers=3 mounts need.
	t.Run("a frozen allowlisted cgroup is a violation", func(t *testing.T) {
		t.Parallel()

		f, root := newTreeFixture(t, "/system.slice/envd.service",
			"system.slice", "system.slice/envd.service",
			"system.slice/rpcbind.service", "workload")
		freezeSweptSet(t, root, "workload")
		freezeOnDisk(t, root, "system.slice/rpcbind.service")

		res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Violations, "rpcbind is allowlisted and was frozen")
		assert.Equal(t, 3, res.Frozen, "and the legitimate ones are still counted separately")
	})

	// The race arm: a cgroup that appeared after the sweep and before the snapshot. Nothing
	// is wrong with the code; the count sizes how often the window is actually used.
	t.Run("an unfrozen workload cgroup is an escape", func(t *testing.T) {
		t.Parallel()

		f, root := newTreeFixture(t, "/system.slice/envd.service",
			"system.slice", "system.slice/envd.service", "frozen-one", "appeared-late")
		freezeSweptSet(t, root, "frozen-one")

		res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Escaped, "appeared-late was the sweep's business and is running")
		assert.Zero(t, res.Violations)
	})

	// Without this guard the audit would slander every fresh sandbox: nothing has been
	// paused, so every workload cgroup is legitimately running and would count as an escape.
	// A guest that was never paused is covered by the MODE, not by the counts: it has no
	// recorded sweep, so TestAuditFrozenState_LegacySweepIsNotAudited's empty-mode arm is
	// where that case lives. Passing ModeHierarchy here would assert the opposite -- that a
	// hierarchy sweep did run -- so the interesting case at this mode is the sweep that ran
	// and froze nothing.
	t.Run("a hierarchy sweep that froze nothing still reports its escapes", func(t *testing.T) {
		t.Parallel()

		f, _ := newTreeFixture(t, "/system.slice/envd.service",
			"system.slice", "system.slice/envd.service", "workload-a", "workload-b")

		res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
		require.NoError(t, err)
		assert.True(t, res.Applicable,
			"a sweep whose writes all failed has the most escapes and the most need to be seen")
		assert.Zero(t, res.Frozen)
		// Four, not two: the fixture also plants the static user/ptys cgroups, and with
		// nothing frozen they are every bit as escaped as the two named workloads.
		assert.Equal(t, 4, res.Escaped, "everything the sweep should have stopped ran through")
		assert.Zero(t, res.Violations)
	})

	// envd's own cgroups are exempt, not escapes -- they are supposed to be running.
	t.Run("envd's own chain is neither escape nor violation", func(t *testing.T) {
		t.Parallel()

		f, root := newTreeFixture(t, "/system.slice/envd.service",
			"system.slice", "system.slice/envd.service", "workload")
		freezeSweptSet(t, root, "workload")

		res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
		require.NoError(t, err)
		assert.Zero(t, res.Escaped, "envd's chain is running by design, not an escape")
	})
}

// TestAuditFrozenState_DescendantsOfAFrozenParentAreNotEscapes: the walk writes only the top
// of each subtree, so every cgroup beneath it keeps cgroup.freeze=0 while being just as
// stopped. Reading the request rather than the settled state called each of those an escape
// -- hundreds of them on a container-runtime guest, i.e. loudest exactly on the layouts this
// freeze exists to cover.
func TestAuditFrozenState_DescendantsOfAFrozenParentAreNotEscapes(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service",
		"customer", "customer/pod-a", "customer/pod-a/container-1", "customer/pod-b")
	freezeSweptSet(t, root, "customer") // one write, whole subtree stopped

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)

	assert.Zero(t, res.Escaped,
		"pod-a, container-1 and pod-b are stopped through their parent, not escapes")
	assert.Zero(t, res.Violations)
	assert.GreaterOrEqual(t, res.Frozen, 4, "the whole customer subtree counts as stopped")
}

// TestAuditFrozenState_TransitiveAllowlistFreezeIsAViolation is the case the audit was
// written for and originally could not see: with envd at the root, freezing system.slice
// wholesale stops journald and rpcbind through the hierarchy. Their own cgroup.freeze stays
// 0, so a request-reading audit reported no violation at all.
func TestAuditFrozenState_TransitiveAllowlistFreezeIsAViolation(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/",
		"system.slice",
		"system.slice/systemd-journald.service",
		"system.slice/rpcbind.service",
		"workload")
	freezeSweptSet(t, root, "workload")
	// What a chain-only walk would have done: freeze system.slice whole.
	freezeOnDisk(t, root, "system.slice")

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)

	// system.slice itself is in the descend set, and journald and rpcbind are allowlisted:
	// all three are stopped and all three must be reported.
	assert.GreaterOrEqual(t, res.Violations, 3,
		"an allowlisted cgroup stopped through its parent is still a cgroup the resume needs")
}

// TestUnfreeze_FailedThawKeepsTheWatchdogArmed: disarming before the walk dropped the
// backstop for precisely the case it exists to cover. A thaw that truncates leaves cgroups
// frozen, and with the timer already cancelled nothing ever retries them.
func TestUnfreeze_FailedThawKeepsTheWatchdogArmed(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "a", "b", "c", "d", "e", "f")
	freezeOnDisk(t, root, "f")

	fired := make(chan ThawResult, 1)
	f.SetThawWatchdog(40*time.Millisecond, func(res ThawResult, _ error) { fired <- res })
	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)

	// A bound this tight truncates, so the thaw cannot have finished the tree.
	_, err = f.UnfreezeReporting(t.Context(), 2)
	require.Error(t, err, "a truncated thaw is an error")

	select {
	case <-fired:
		// The backstop survived and retried, which is the whole point.
	case <-time.After(2 * time.Second):
		t.Fatal("a truncated thaw disarmed the watchdog: nothing will retry the frozen cgroups")
	}
}

// TestAuditFrozenState_LegacySweepIsNotAudited pins the DEFAULT production configuration.
// With freeze-guest-hierarchy off every pause is a legacy sweep, which freezes user and pty and
// nothing else. Scoring that against the walk's expectation would report every other cgroup in
// the guest as an escape, on every resume -- the audit must decline instead.
func TestAuditFrozenState_LegacySweepIsNotAudited(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/", "system.slice", "customer.service")
	// exactly what a legacy sweep leaves behind: the static set, and nothing else.
	freezeSweptSet(t, root)

	for _, mode := range []FreezeMode{ModeLegacy, FreezeMode("")} {
		res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, mode)
		require.NoError(t, err)
		assert.False(t, res.Applicable, "mode %q must not be scored against the walk", mode)
		assert.Zero(t, res.Escaped)
		assert.Zero(t, res.Violations)
	}

	// The same tree under a hierarchy sweep DOES report the escapes, so it is the mode guard
	// that silences the audit and not an inert fixture.
	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)
	assert.True(t, res.Applicable)
	assert.Positive(t, res.Escaped, "customer.service is running and was never frozen")
}

// TestThawWatchdog_SupersededFireIsANoOp pins the generation guard. time.Timer.Stop cannot
// recall a callback that has already been dispatched, so without the guard a watchdog armed by
// one freeze could wake up after a LATER freeze, thaw that freeze's cgroups, and -- because the
// thaw came back clean -- disarm the later freeze's own timer too. A freeze silently undone,
// with its backstop removed in the same breath.
//
// The stale fire is invoked directly rather than waited for: the window that makes it dangerous
// is the freeze lock, and holding it is what the guard has to survive.
func TestThawWatchdog_SupersededFireIsANoOp(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "workload")
	f.SetThawWatchdog(time.Hour, func(ThawResult, error) {})

	// Freeze #1 arms a timer; remember its generation the way its callback closure does.
	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	f.watchdogMu.Lock()
	staleGen := f.watchdogGen
	f.watchdogMu.Unlock()

	// Freeze #2 supersedes it.
	_, err = f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	require.True(t, frozenOnDisk(t, root, "workload"), "freeze #2 froze the workload")

	// Freeze #1's callback finally runs.
	_, fired, _ := f.thawForWatchdog(t.Context(), staleGen)
	assert.False(t, fired, "a superseded watchdog must not thaw")
	assert.True(t, frozenOnDisk(t, root, "workload"),
		"freeze #2's cgroups must still be frozen: a stale fire undoing them is the bug")

	// The current generation still works, or the guard would have disabled the backstop.
	f.watchdogMu.Lock()
	liveGen := f.watchdogGen
	f.watchdogMu.Unlock()
	_, fired, _ = f.thawForWatchdog(t.Context(), liveGen)
	assert.True(t, fired, "the armed generation must still fire")
	assert.False(t, frozenOnDisk(t, root, "workload"), "and it must thaw")
}

// TestAuditFrozenState_TruncationIsReported: an audit that measured half the hierarchy must
// not read like one that found nothing wrong. Every count is a floor once the bound bites, so
// the flag is the only thing that stops a zero-violations result from being taken at face
// value -- the same reason the freeze and the thaw report theirs.
func TestAuditFrozenState_TruncationIsReported(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "a", "b", "c", "d", "e")
	freezeSweptSet(t, root, "a", "b", "c", "d", "e")

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, 3, ModeHierarchy)
	require.NoError(t, err)
	assert.True(t, res.Truncated, "the bound stopped the walk")
	assert.True(t, res.Applicable, "truncation alone is a finding, so the result must be reported")
	assert.LessOrEqual(t, res.Visited, 3)

	// Unbounded, the same tree is complete.
	res, err = AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)
	assert.False(t, res.Truncated)
}

// TestAuditFrozenState_UnreadableCgroupDoesNotPruneItsSubtree covers the second half of the
// same walk-order bug: classifying before enumerating meant one unreadable cgroup silently
// dropped everything beneath it, which is worst exactly when the hierarchy is churning.
func TestAuditFrozenState_UnreadableCgroupDoesNotPruneItsSubtree(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "mid", "mid/deep")
	freezeSweptSet(t, root)
	freezeOnDisk(t, root, "mid/deep")

	// `mid` becomes unreadable the way a cgroup removed mid-walk does: its interface files
	// go away while the directory (and its child) remain enumerable.
	require.NoError(t, os.Remove(filepath.Join(root, "mid", "cgroup.events")))
	require.NoError(t, os.Remove(filepath.Join(root, "mid", "cgroup.freeze")))

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)
	assert.Positive(t, res.Frozen, "mid/deep is frozen and must still be seen through an unreadable parent")
}

// TestResumeFrozen_RestoresWhatTheExecveDropped covers the live-upgrade state the handover blob
// cannot carry on its own. The incoming envd inherits a workload frozen by its predecessor, the
// carried record -- and nothing else. Two things are missing and both bite before the
// post-upgrade /init: the freeze-active flag, without which a freeze taken in the meantime
// adopts our own frozen cgroups as the guest's and every later thaw preserves them; and the
// watchdog, whose timer belonged to the previous process image.
func TestResumeFrozen_RestoresWhatTheExecveDropped(t *testing.T) {
	t.Parallel()

	// The outgoing envd: one guest-frozen cgroup, then a sweep that freezes the rest.
	out, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "workload", "guest-own")
	freezeOnDisk(t, root, "guest-own")
	_, err := out.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	carried := out.GuestFrozenPaths()
	require.Equal(t, []string{"guest-own"}, carried)
	require.True(t, frozenOnDisk(t, root, "workload"), "the outgoing envd froze the workload")

	// The incoming envd: same tree, same still-frozen cgroups, a fresh freezer.
	in := NewWorkloadFreezer(out.mgr)
	in.procSelfCgroup = out.procSelfCgroup
	fired := make(chan ThawResult, 1)
	in.SetThawWatchdog(40*time.Millisecond, func(res ThawResult, _ error) { fired <- res })
	in.SetGuestFrozenPaths(carried)
	require.NoError(t, in.ResumeFrozen(t.Context()))

	// A freeze before the post-upgrade /init must not adopt our predecessor's freezes.
	res, err := in.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Equal(t, 1, res.PreFrozen,
		"only the guest's cgroup is the guest's; the workload was frozen by our predecessor")

	// And the backstop exists again, so an /init that never arrives still thaws.
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("the watchdog was not re-armed after the handover: a frozen guest with no backstop")
	}
	assert.False(t, frozenOnDisk(t, root, "workload"), "the backstop thawed what we froze")
	assert.True(t, frozenOnDisk(t, root, "guest-own"), "and still left the guest's own freeze alone")
}

// TestScanGuestFrozen_RecordsOnlyTheGuestsOwn pins what the scan is allowed to record.
// Anything of ours found frozen must be absent from the record, so the thaw still clears
// it: an allowlisted cgroup left frozen costs the resume its journal or its port
// forwarding, which is the arm-E' guarantee.
func TestScanGuestFrozen_RecordsOnlyTheGuestsOwn(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "system.slice/systemd-journald.service",
		"socats", "customer", "customer/deep", "customer/deep/deeper")

	// the guest froze one shallow and one DEEP cgroup
	freezeOnDisk(t, root, "customer")
	freezeOnDisk(t, root, "customer/deep/deeper")
	// and something of ours is frozen too -- drift from an older envd
	freezeOnDisk(t, root, "socats")
	freezeOnDisk(t, root, "system.slice/systemd-journald.service")

	got, truncated, errs := ScanGuestFrozen(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups)
	require.Empty(t, errs)
	assert.False(t, truncated)

	rel := make([]string, 0, len(got))
	for p := range got {
		r, err := filepath.Rel(root, p)
		require.NoError(t, err)
		rel = append(rel, r)
	}
	slices.Sort(rel)
	assert.Equal(t, []string{"customer", "customer/deep/deeper"}, rel,
		"only the guest's own cgroups, and the deep one must be found -- the thaw descends there")
}

// TestGuestFrozen_ThawLeavesTheGuestsOwnFrozen is the end-to-end shape of the record: freeze,
// then thaw, and what the guest froze is still frozen while what we froze is not.
func TestGuestFrozen_ThawLeavesTheGuestsOwnFrozen(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer", "workload")
	freezeOnDisk(t, root, "customer")

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Equal(t, 1, res.PreFrozen, "the guest's cgroup is recorded, not written to")

	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.Equal(t, 1, thaw.Preserved)

	requested, err := f.mgr.(PathManager).FreezeRequestedAt(filepath.Join(root, "customer"))
	require.NoError(t, err)
	assert.True(t, requested, "the guest's own freeze must survive the pause/resume")

	requested, err = f.mgr.(PathManager).FreezeRequestedAt(filepath.Join(root, "workload"))
	require.NoError(t, err)
	assert.False(t, requested, "a cgroup WE froze must be thawed")
}

// TestGuestFrozen_NoFreezeOfOursMeansNoDiscovery is the direction a lost record must NOT take.
// On every path where we never swept -- the master flag off, /freeze never reaching us, a transport
// failure -- there is nothing of ours to undo, and a thaw that discovers anyway clears cgroups the
// guest froze itself. The pre-walk code only ever thawed the static list, so discovering here is a
// regression against what the guest had before this feature existed, not a safe default.
func TestGuestFrozen_NoFreezeOfOursMeansNoDiscovery(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")
	freezeOnDisk(t, root, "customer")

	// Nothing of ours is frozen: the static list is untouched, which is what a resume looks
	// like when no freeze ever happened.
	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.False(t, thaw.Discovered, "no freeze of ours is in effect, so the walk is not ours to run")

	requested, err := f.mgr.(PathManager).FreezeRequestedAt(filepath.Join(root, "customer"))
	require.NoError(t, err)
	assert.True(t, requested, "the guest's own freeze must survive a resume we never froze for")
}

// settlingManager supplies the one kernel behaviour the on-disk fixture cannot: a write to
// cgroup.freeze SETTLES into cgroup.events. The fixture writes the two files independently, so
// without this a thawed cgroup keeps reading `frozen 1` forever -- and most of this package reads
// the settled state, so a test on such a tree is testing a tree no kernel would produce. It hid a
// real ordering bug: the thaw cleared the static list and then asked whether the static list was
// frozen.
type settlingManager struct {
	PathManager

	t     *testing.T
	root  string
	paths map[ProcessType]string
}

func (m *settlingManager) settle(path string, frozen bool) {
	m.t.Helper()

	state := "frozen 0\n"
	if frozen {
		state = "frozen 1\n"
	}
	// The whole subtree, because freezing is hierarchical and the kernel reports descendants
	// as frozen through their ancestor.
	require.NoError(m.t, filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err //nolint:wrapcheck // fixture helper
		}

		return os.WriteFile(filepath.Join(p, "cgroup.events"), []byte("populated 1\n"+state), 0o644)
	}))
}

func (m *settlingManager) Freeze(pt ProcessType) error {
	if err := m.PathManager.Freeze(pt); err != nil {
		return err //nolint:wrapcheck // decorator
	}
	m.settle(m.paths[pt], true)

	return nil
}

func (m *settlingManager) FreezeAt(path string) error {
	if err := m.PathManager.FreezeAt(path); err != nil {
		return err //nolint:wrapcheck // decorator
	}
	m.settle(path, true)

	return nil
}

func (m *settlingManager) Unfreeze(pt ProcessType) error {
	if err := m.PathManager.Unfreeze(pt); err != nil {
		return err //nolint:wrapcheck // decorator
	}
	m.settle(m.paths[pt], false)

	return nil
}

func (m *settlingManager) UnfreezeAt(path string) error {
	if err := m.PathManager.UnfreezeAt(path); err != nil {
		return err //nolint:wrapcheck // decorator
	}
	m.settle(path, false)

	return nil
}

// newSettling wraps a fixture's manager. Kept separate from newTreeFixture so the tests that do
// not read settled state are unaffected.
func newSettling(t *testing.T, f *WorkloadFreezer, root string) {
	t.Helper()

	mgr := f.mgr.(*Cgroup2Manager)
	paths := make(map[ProcessType]string, len(mgr.cgroupPaths))
	maps.Copy(paths, mgr.cgroupPaths)
	f.mgr = &settlingManager{PathManager: mgr, t: t, root: root, paths: paths}
}

// TestGuestFrozen_LostRecordWithOurFreezeInEffectThawsEverything is the other direction, and the
// one that must stay fail-open: our freeze IS in place but the record is gone (a crashed envd, a
// handover blob that would not decode). Preserving nothing is right here -- thawing more than
// intended costs the guest a resumed container, while thawing less strands the whole sandbox.
func TestGuestFrozen_LostRecordWithOurFreezeInEffectThawsEverything(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")
	freezeSweptSet(t, root, "customer") // our static freeze, plus a cgroup we froze
	freezeOnDisk(t, root, "customer")
	// Settling matters here and nowhere else in this file: the thaw clears the static list
	// first, and the question it then asks is whether the static list is frozen. On a real
	// kernel the answer flips the moment the static thaw lands.
	newSettling(t, f, root)

	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.True(t, thaw.Discovered, "our freeze is in effect, so the walk must run")
	assert.Zero(t, thaw.Preserved, "no record means nothing is known to be the guest's")
	assert.Positive(t, thaw.Thawed)

	requested, err := f.mgr.(PathManager).FreezeRequestedAt(filepath.Join(root, "customer"))
	require.NoError(t, err)
	assert.False(t, requested, "never strand: with our freeze in effect and no record, clear it")
}

// TestGuestFrozen_RecordedInLegacyModeToo is the case the reported bug actually lives in:
// freeze-guest-hierarchy ships off, so every pause today takes the legacy sweep -- but the
// thaw walks the whole hierarchy regardless, so a record scoped to the walk would leave
// the default configuration clearing the guest's own freezes.
func TestGuestFrozen_RecordedInLegacyModeToo(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")
	freezeOnDisk(t, root, "customer")

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeLegacy})
	require.NoError(t, err)
	require.Equal(t, ModeLegacy, res.Mode)
	assert.Equal(t, 1, res.PreFrozen, "the scan runs in legacy mode too")

	_, err = f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)

	requested, err := f.mgr.(PathManager).FreezeRequestedAt(filepath.Join(root, "customer"))
	require.NoError(t, err)
	assert.True(t, requested, "the guest's freeze survives with the walk disabled")
}

// TestGuestFrozenPaths_RoundTripsThroughRelativePaths covers the handover carrier: the
// record crosses an execve as paths relative to the cgroup root, because an absolute path
// from the outgoing process image means nothing to the incoming one.
func TestGuestFrozenPaths_RoundTripsThroughRelativePaths(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer", "other")
	freezeOnDisk(t, root, "customer")
	freezeOnDisk(t, root, "other")

	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)

	carried := f.GuestFrozenPaths()
	assert.Equal(t, []string{"customer", "other"}, carried, "relative, sorted, no root prefix")

	// A fresh freezer over the same tree: the incoming envd after the swap. Its own tree must
	// show our freeze still in place, which is what a handover leaves behind and what tells the
	// thaw the walk is its business.
	next, nextRoot := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer", "other")
	freezeSweptSet(t, nextRoot, "customer", "other")
	next.SetGuestFrozenPaths(carried)
	thaw, err := next.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.Equal(t, 2, thaw.Preserved, "the restored record must survive the handover")
}

// TestGuestFrozen_SecondFreezeDoesNotAdoptOurOwn is the bug the watchdog test above surfaced.
// The live-upgrade handover takes the freeze again while the pause's freeze is still in effect.
// If that second freeze re-ran the guest scan it would find OUR cgroups frozen, record them as
// the guest's, and the thaw would then preserve them -- a guest stranded frozen, which is the
// one outcome the whole design refuses.
func TestGuestFrozen_SecondFreezeDoesNotAdoptOurOwn(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "workload", "guest-own")
	freezeOnDisk(t, root, "guest-own")

	first, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	require.Equal(t, 1, first.PreFrozen, "only the guest's cgroup is the guest's")
	require.True(t, frozenOnDisk(t, root, "workload"), "we froze the workload")

	// The handover's freeze, inside the same frozen window.
	second, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Equal(t, 1, second.PreFrozen,
		"still one: a second freeze must not adopt our own frozen cgroups as the guest's")

	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.Equal(t, 1, thaw.Preserved)
	assert.False(t, frozenOnDisk(t, root, "workload"), "our freeze must still be cleared")
	assert.True(t, frozenOnDisk(t, root, "guest-own"), "the guest's must still be kept")
}

// TestAuditFrozenState_NotAfterTheThaw covers the /init that is not a resume. The resume audit
// runs inside /init before its thaw, but /init is retried, and the in-place checkpoint path thaws
// through POST /unfreeze itself and then re-inits — so a look at the tree AFTER the thaw is an
// ordinary occurrence, not an edge case. Scored against the sweep it would find every cgroup
// unfrozen and call all of them escapes, overwriting the real audit and inflating the metric.
func TestAuditFrozenState_NotAfterTheThaw(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "workload", "other")

	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	require.Equal(t, ModeHierarchy, f.UnthawedSweepMode(), "the sweep's state is in place")

	// The sweep writes cgroup.freeze; the kernel is what then reports frozen in cgroup.events,
	// and the audit reads the latter. freezeSweptSet plays that part.
	freezeSweptSet(t, root, "workload", "other")

	// The resume audit, while the freeze is still in effect.
	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, f.UnthawedSweepMode())
	require.NoError(t, err)
	require.True(t, res.Applicable)
	require.Positive(t, res.Frozen)
	require.Zero(t, res.Escaped, "nothing escaped: the sweep froze the complement")

	_, err = f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	require.False(t, frozenOnDisk(t, root, "workload"), "the thaw ran")

	// A second look, after the thaw.
	assert.Equal(t, FreezeMode(""), f.UnthawedSweepMode(),
		"the state the sweep produced is gone, so there is nothing left to audit against")
	res, err = AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, f.UnthawedSweepMode())
	require.NoError(t, err)
	assert.False(t, res.Applicable, "a thawed tree must not be reported as a guest that escaped")
	assert.Zero(t, res.Escaped)
}

// TestAuditFrozenState_NotAfterADirtyThawEither is the narrower half of the same rule. A thaw that
// truncated or failed leaves cgroups frozen and keeps the guest record alive for the watchdog's
// retry, but it has still cleared some cgroups — so auditing what remains would report exactly
// those as escapes. The audit's window closes on the first thaw ATTEMPT, not on a clean one.
func TestAuditFrozenState_NotAfterADirtyThawEither(t *testing.T) {
	t.Parallel()

	f, _ := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "a", "b", "c", "d", "e")

	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)

	// A bound this tight truncates: a dirty thaw by construction.
	_, err = f.UnfreezeReporting(t.Context(), 2)
	require.Error(t, err, "a truncated thaw is an error")

	assert.Equal(t, FreezeMode(""), f.UnthawedSweepMode(),
		"a partial thaw has still destroyed the state the audit would score against")
}

// thawMidWalk is a PathManager that performs a real thaw the first time the audit reads freeze
// state, i.e. from inside the walk. That makes the interleaving deterministic without a hook in
// production code and without goroutines: the audit's own first read is the trigger.
type thawMidWalk struct {
	PathManager

	f    *WorkloadFreezer
	once sync.Once
}

func (t *thawMidWalk) FrozenAt(path string) (bool, error) {
	t.once.Do(func() { _, _ = t.f.UnfreezeReporting(context.Background(), DefaultThawMaxCgroups) })

	return t.PathManager.FrozenAt(path)
}

// TestAuditFrozenSet_DiscardsAWalkAThawRanThrough pins the guard. A thaw landing mid-walk leaves
// the walk reading cgroups frozen ahead of it and unfrozen behind it, so its tail counts cgroups
// as escaped that were cleared by the thaw rather than missed by the sweep. Reachable on the
// in-place checkpoint whose pause failed, where the envd re-init and the POST /unfreeze are
// launched concurrently.
func TestAuditFrozenSet_DiscardsAWalkAThawRanThrough(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "a", "b", "c", "d")
	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	freezeSweptSet(t, root, "a", "b", "c", "d")

	// Undisturbed, this tree audits as fully frozen with nothing escaped.
	clean, err := f.AuditFrozenSet(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups)
	require.NoError(t, err)
	require.True(t, clean.Applicable)
	require.Positive(t, clean.Frozen)
	require.Zero(t, clean.Escaped)

	// Now with a thaw landing inside the walk. Re-freeze first: the probe above left the tree
	// as it was, but this arm needs a sweep whose state a thaw can still destroy.
	_, err = f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	freezeSweptSet(t, root, "a", "b", "c", "d")

	res, err := f.AuditFrozenSet(&thawMidWalk{PathManager: f.mgr.(PathManager), f: f},
		f.procSelfCgroup, DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.False(t, res.Applicable, "a walk a thaw ran through must not be reported")
	assert.Zero(t, res.Escaped, "and must not contribute escapes the sweep never missed")
}

// TestAuditFrozenState_NamesTheOffenders: a count says the allowlist did not hold, a name says
// which cgroup. Verifying this on the dev cluster took planting drift and discriminating across
// two pause cycles to work out that `socats` was the violation -- exactly the work an operator
// should not have to repeat from a metric.
//
// The sample is bounded and stays out of the response header: these are guest-chosen names, and
// attacker-controlled text in an HTTP header is a header-injection surface.
func TestAuditFrozenState_NamesTheOffenders(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "socats", "workload", "latecomer")
	freezeSweptSet(t, root, "workload")
	// The arm-E' shape: an allowlisted cgroup frozen by something other than this sweep.
	freezeOnDisk(t, root, "socats")

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)

	assert.Positive(t, res.Violations)
	assert.Contains(t, res.ViolationPaths, "socats", "the violation must be named, not just counted")
	assert.Positive(t, res.Escaped)
	assert.Contains(t, res.EscapedPaths, "latecomer", "and so must the cgroup that ran through")
	assert.LessOrEqual(t, len(res.ViolationPaths), auditPathSample)
	assert.LessOrEqual(t, len(res.EscapedPaths), auditPathSample)
}

// TestAuditFrozenState_OffenderSampleIsBounded: the paths are a sample for a human, not a second
// copy of the tree. An unbounded list would grow with a hostile hierarchy, which is the threat
// model, and the counts are the measurement regardless.
func TestAuditFrozenState_OffenderSampleIsBounded(t *testing.T) {
	t.Parallel()

	many := make([]string, 0, 20)
	for i := range 20 {
		many = append(many, fmt.Sprintf("late-%02d", i))
	}
	f, root := newTreeFixture(t, "/system.slice/envd.service",
		append([]string{"system.slice", "system.slice/envd.service", "workload"}, many...)...)
	freezeSweptSet(t, root, "workload")

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, res.Escaped, 20, "every latecomer counted")
	assert.Len(t, res.EscapedPaths, auditPathSample, "but only a sample named")
}

// failAt makes one PathManager call fail for one cgroup with a chosen error, passing every
// other call through to the real tree.
//
// Both halves are parameters on purpose. The walks divide errors into two classes -- a
// cgroup that went away, which every walk tolerates, and everything else, which is counted
// -- so a fixture has to be able to land on either side. And the errno a removal produces
// depends on which call meets it, so the method has to be selectable too.
type failAt struct {
	PathManager

	method string
	target string
	err    error

	// calls counts how often the injected call was actually made, which is how a test
	// asserts that a target was dropped from a poll set rather than re-read until the
	// budget ran out. That is a structural claim, and counting the calls settles it
	// without a clock -- an elapsed-time bound would measure the machine instead.
	calls atomic.Int64
}

const (
	failFreezeRequestedAt = "FreezeRequestedAt"
	failFrozenAt          = "FrozenAt"
	failFreezeAt          = "FreezeAt"
	failUnfreezeAt        = "UnfreezeAt"
	failChildrenOf        = "ChildrenOf"
)

// injected returns the error to report from method, or nil to fall through to the real
// tree. Shaped as a PathError because that is what the os package returns and what the
// production guards therefore have to unwrap.
func (m *failAt) injected(method, path string) error {
	if method != m.method || path != m.target {
		return nil
	}
	m.calls.Add(1)

	return &fs.PathError{Op: "open", Path: path, Err: m.err}
}

func (m *failAt) FreezeRequestedAt(path string) (bool, error) {
	if e := m.injected(failFreezeRequestedAt, path); e != nil {
		return false, e
	}

	return m.PathManager.FreezeRequestedAt(path)
}

func (m *failAt) FrozenAt(path string) (bool, error) {
	if e := m.injected(failFrozenAt, path); e != nil {
		return false, e
	}

	return m.PathManager.FrozenAt(path)
}

func (m *failAt) FreezeAt(path string) error {
	if e := m.injected(failFreezeAt, path); e != nil {
		return e
	}

	return m.PathManager.FreezeAt(path)
}

func (m *failAt) UnfreezeAt(path string) error {
	if e := m.injected(failUnfreezeAt, path); e != nil {
		return e
	}

	return m.PathManager.UnfreezeAt(path)
}

func (m *failAt) ChildrenOf(path string) ([]string, error) {
	if e := m.injected(failChildrenOf, path); e != nil {
		return nil, e
	}

	return m.PathManager.ChildrenOf(path)
}

// TestFreeze_ScanErrorIsNotFatal is the one that matters for the live upgrade. A cgroup that
// vanishes while the guest-freeze scan walks is a race the sweep itself tolerates, but the
// handover refuses to swap on ANY error from the freeze -- so propagating a scan error would
// abort upgrades over nothing. The scan's failure mode is benign by construction: an
// unclassified cgroup is absent from the record, so the thaw clears it, which is what happened
// before the record existed.
func TestFreeze_ScanErrorIsNotFatal(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "workload", "workload/deep")
	// Broken DEEPER than the sweep ever writes: the sweep stops a subtree by freezing its top,
	// so it never touches workload/deep, while the scan walks the whole tree and does. That is
	// what isolates a scan failure from a sweep failure -- breaking a top-level cgroup would
	// fail the write too, and sweep errors are meant to propagate.
	//
	// EACCES rather than a removal: a cgroup that vanished is not a scan failure at all, so
	// deleting one would test the tolerance rather than the failure this test is about.
	f.mgr = &failAt{
		PathManager: f.mgr.(PathManager),
		method:      failFreezeRequestedAt,
		target:      filepath.Join(root, "workload", "deep"),
		err:         syscall.EACCES,
	}

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})

	require.NoError(t, err,
		"a scan error must not fail the freeze: the live-upgrade handover treats any error as fatal")
	assert.Positive(t, res.ScanFailed, "but it must not be silent either")
	assert.Positive(t, res.Requested, "and the sweep still did its work")
}

// failingUnfreeze fails every thaw write while broken is set, so a rescue completes dirty.
type failingUnfreeze struct {
	PathManager

	broken atomic.Bool
}

func (m *failingUnfreeze) UnfreezeAt(path string) error {
	if m.broken.Load() {
		return errors.New("write cgroup.freeze: input/output error")
	}

	return m.PathManager.UnfreezeAt(path)
}

func (m *failingUnfreeze) Unfreeze(pt ProcessType) error {
	if m.broken.Load() {
		return errors.New("write cgroup.freeze: input/output error")
	}

	return m.PathManager.Unfreeze(pt)
}

// TestThawWatchdog_DirtyRescueRearms: the backstop was single-shot. armWatchdog is reached only
// from the freeze paths, and a rescue that fails does not disarm what it failed to undo -- so one
// transient write error during the rescue left a live sandbox with a stopped workload and nothing
// left to retry it. The retry is the whole value of a backstop; a backstop with one attempt is a
// coin flip.
func TestThawWatchdog_DirtyRescueRearms(t *testing.T) {
	t.Parallel()

	f, _ := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")
	broken := &failingUnfreeze{PathManager: f.mgr.(PathManager)}
	broken.broken.Store(true)
	f.mgr = broken

	fired := make(chan ThawResult, 4)
	f.SetThawWatchdog(30*time.Millisecond, func(res ThawResult, _ error) { fired <- res })
	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)

	// Two fires, not one. No deadline is asserted beyond a generous ceiling: the claim is that
	// a SECOND attempt happens at all, and pinning when it happens would test the clock.
	for i := range 2 {
		select {
		case res := <-fired:
			require.Positive(t, res.Failed, "the rescue was meant to fail, or this proves nothing")
		case <-time.After(3 * time.Second):
			t.Fatalf("rescue %d never came: a failed rescue left nothing to retry the freeze", i+1)
		}
	}

	// Let the next attempt succeed, so the rescue loop stops instead of retrying past the test.
	broken.broken.Store(false)
}

// vanishOnRead deletes a cgroup from under the walk, which is what a container runtime doing its
// own cleanup does at any moment. beforeWrite delays the removal until after the state read, so
// the same race can be aimed at either side of read-then-write.
type vanishOnRead struct {
	PathManager

	t           *testing.T
	target      string
	beforeWrite bool
	once        sync.Once
}

func (m *vanishOnRead) FreezeRequestedAt(path string) (bool, error) {
	if path != m.target {
		return m.PathManager.FreezeRequestedAt(path)
	}

	if m.beforeWrite {
		// Report it frozen, then take it away: the thaw's write is what finds it gone.
		frozen, err := m.PathManager.FreezeRequestedAt(path)
		m.once.Do(func() { require.NoError(m.t, os.RemoveAll(path)) })

		return frozen, err
	}

	m.once.Do(func() { require.NoError(m.t, os.RemoveAll(path)) })

	return m.PathManager.FreezeRequestedAt(path)
}

// TestUnfreeze_DiscoveryToleratesAVanishedCgroup: the discovering thaw walks a tree the guest is
// still mutating, so a cgroup that exists at readdir and is gone by the read is ordinary, not a
// fault. Counting it as a failure makes every resume of a busy guest report a dirty thaw, which
// both keeps the watchdog armed for nothing and buries the failures that are real.
func TestUnfreeze_DiscoveryToleratesAVanishedCgroup(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		beforeWrite bool
	}{
		{"gone before the state read", false},
		{"gone between the read and the thaw", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f, root := newTreeFixture(t, "/system.slice/envd.service",
				"system.slice", "system.slice/envd.service", "customer", "doomed")
			freezeSweptSet(t, root, "customer", "doomed")

			f.mgr = &vanishOnRead{
				PathManager: f.mgr.(PathManager),
				t:           t,
				target:      filepath.Join(root, "doomed"),
				beforeWrite: tc.beforeWrite,
			}

			res, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
			require.NoError(t, err, "a cgroup that vanished mid-thaw is a race, not a failure")
			assert.Zero(t, res.Failed)
			assert.False(t, frozenOnDisk(t, root, "customer"), "and the rest of the tree still thaws")
		})
	}
}

// thawAndRefreezeMidWalk thaws and then re-freezes, both from inside the audit's walk, so the
// sweep mode ends up back at the value the walk started with while the tree changed twice.
type thawAndRefreezeMidWalk struct {
	PathManager

	f    *WorkloadFreezer
	once sync.Once
}

func (m *thawAndRefreezeMidWalk) FrozenAt(path string) (bool, error) {
	m.once.Do(func() {
		_, _ = m.f.UnfreezeReporting(context.Background(), DefaultThawMaxCgroups)
		_, _ = m.f.Freeze(context.Background(), FreezeOptions{Mode: ModeHierarchy})
	})

	return m.PathManager.FrozenAt(path)
}

// TestAuditFrozenSet_DiscardsAcrossAThawAndRefreeze pins why the mid-walk guard counts a
// generation rather than comparing the sweep mode. Comparing the mode is an ABA test: a thaw
// clears it and a fresh hierarchy freeze puts the same value back, so a walk that spanned both
// passes an equality check having sampled some cgroups behind the thaw and the rest after the
// re-freeze. The counts that come out of such a walk are a blend of two trees, and escapes is
// exactly the field they corrupt -- the audit publishing a wrong number is worse than the audit
// publishing nothing, because a wrong one is indistinguishable from a real regression.
func TestAuditFrozenSet_DiscardsAcrossAThawAndRefreeze(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "workload-a", "workload-b")
	freezeSweptSet(t, root, "workload-a", "workload-b")
	_, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)

	res, err := f.AuditFrozenSet(
		&thawAndRefreezeMidWalk{PathManager: f.mgr.(PathManager), f: f},
		f.procSelfCgroup, DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.False(t, res.Applicable,
		"the mode returned to its old value, but the tree the walk measured did not")
}

// TestAuditFrozenState_ChildrenOfAnAllowlistedCgroupAreExempt: the freeze is hierarchical, so
// the exemption has to be too. The sweep never descends into an allowlisted cgroup, so its
// children are never candidates -- and an exact-path audit then reported every one of them as an
// escape, i.e. reported the exemption it was told to honour. Real guests have such children:
// journald under a slice, and socket units that activate into a sub-cgroup.
func TestAuditFrozenState_ChildrenOfAnAllowlistedCgroupAreExempt(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service",
		"system.slice/systemd-journald.service", "system.slice/systemd-journald.service/child",
		"workload")
	freezeSweptSet(t, root, "workload")

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)
	assert.Zero(t, res.Escaped, "a child of an exempt unit is exempt, not an escape")
	assert.Zero(t, res.Violations)
}

// TestAuditFrozenState_RpcStatdIsAllowlisted: statd is started by systemd into its own unit
// cgroup, not into init.scope, so covering the thing that launches it does not cover it. It
// holds the NLM lock state an nfsvers=3 mount without nolock needs, which is the mount every
// volume-backed sandbox performs on the resume path, before the thaw runs.
func TestAuditFrozenState_RpcStatdIsAllowlisted(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service",
		"system.slice/rpc-statd.service", "workload")
	freezeSweptSet(t, root, "workload")
	freezeOnDisk(t, root, "system.slice/rpc-statd.service")

	res, err := AuditFrozenState(f.mgr.(PathManager), f.procSelfCgroup, DefaultThawMaxCgroups, ModeHierarchy)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Violations, "a frozen statd breaks the NFSv3 mount the resume performs")
}

// TestFreeze_RootSpellingDoesNotBreakTheGuestScan: every membership set the walks build --
// AncestorChain, DescendSet, the thaw's onChain -- is made of cleaned paths, while the walk itself
// starts from the root as spelled. A cgroup-root carrying a trailing slash (it comes from a flag,
// passed through verbatim) therefore fails its own membership lookup, so the walks treat the MOUNT
// ROOT as an ordinary cgroup and read cgroup.freeze there -- a file that does not exist on the
// root. Every pause then reports a scan failure and every thaw is dirty, which drives the whole
// dirty-thaw state machine: the watchdog never disarms and freezeActive never clears. Cleaning at
// the one place the root is read removes the class rather than one instance of it.
func TestFreeze_RootSpellingDoesNotBreakTheGuestScan(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")
	f.mgr.(*Cgroup2Manager).rootPath = root + "/./"

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Zero(t, res.ScanFailed, "the mount root is not a cgroup and must not be read as one")

	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.Zero(t, thaw.Failed)
	assert.False(t, frozenOnDisk(t, root, "customer"), "and the tree still thaws through the odd root")
}

// TestSweepHierarchy_TruncationCannotStarveTheStaticList: children are enumerated in name order,
// so leaving the static cgroups to the walk let a guest decide whether they were frozen at all --
// create enough cgroups sorting before "pty" and "user" and they fall past the bound. Two things
// break when that happens: the cgroups holding what envd itself spawned keep running through the
// snapshot, which is a regression against the behaviour that predates this walk; and their settled
// state is the evidence a restarted envd uses to tell that it froze anything, so the thaw stops
// discovering. The bound exists for a hostile hierarchy, so it must not be a thing a hostile
// hierarchy can aim.
func TestSweepHierarchy_TruncationCannotStarveTheStaticList(t *testing.T) {
	t.Parallel()

	// Names chosen to sort before every static cgroup, which is all the guest has to do.
	crowd := make([]string, 0, 8)
	for i := range 8 {
		crowd = append(crowd, fmt.Sprintf("aaa%03d", i))
	}
	f, root := newTreeFixture(t, "/system.slice/envd.service",
		append([]string{"system.slice", "system.slice/envd.service"}, crowd...)...)
	// Settling, because the claims here are about the SETTLED state: that is what the thaw
	// reads as evidence, and what a write to cgroup.freeze turns into on a real kernel.
	newSettling(t, f, root)

	// A truncated FREEZE is a reported flag rather than an error -- unlike a truncated thaw,
	// which strands a guest.
	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy, MaxCgroups: 4})
	require.NoError(t, err)
	require.True(t, res.Truncated, "the bound must actually bite, or this proves nothing")

	for _, pt := range WorkloadProcessTypes {
		frozen, e := f.mgr.Frozen(pt)
		require.NoError(t, e)
		assert.True(t, frozen, "%s must be frozen whatever the walk had budget for", pt)
	}

	// And the thaw can still tell that a freeze of ours is in effect, which is what the static
	// list is evidence for.
	assert.True(t, f.freezeInEffect())
	for _, rel := range []string{"pty", "user"} {
		assert.True(t, frozenOnDisk(t, root, rel), "%s carries the freeze request on disk", rel)
	}
}

// TestSweepHierarchy_StaticCgroupsAreCountedOnce: the static pass and the walk both cover the
// static cgroups, so the walk has to skip what the pass already requested. A second write would
// succeed -- cgroup.freeze is idempotent -- and silently inflate Requested and the confirmation
// set, which is the arithmetic every freeze metric is read against.
func TestSweepHierarchy_StaticCgroupsAreCountedOnce(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")
	// Settling, so the confirmation wait can be asserted too: a double-written cgroup would
	// appear twice in the pending set and be counted twice as frozen.
	newSettling(t, f, root)

	// MaxWait, so the confirmation wait actually polls: without it the sweep returns before
	// reading any settled state and Frozen stays 0 whatever was written.
	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy, MaxWait: 5 * time.Second})
	require.NoError(t, err)
	// user, pty (the static pass) and customer (the walk). The fixture plants no other
	// freezable cgroup, so this is exact rather than a floor.
	assert.Equal(t, 3, res.Requested)
	assert.Equal(t, 3, res.Frozen)
}

// freezeAfterScan makes a cgroup read unfrozen to the pre-pause scan and frozen to the sweep that
// follows it -- which is what a guest freezing one of its own cgroups in that window looks like from
// here. Keyed on the read count rather than on a clock, so it is deterministic.
type freezeAfterScan struct {
	PathManager

	target string
	reads  atomic.Int32
}

func (m *freezeAfterScan) FreezeRequestedAt(path string) (bool, error) {
	if path != m.target {
		return m.PathManager.FreezeRequestedAt(path)
	}
	// First read is the scan's; every later one is the sweep's or the thaw's.
	if m.reads.Add(1) == 1 {
		return false, nil
	}

	return true, nil
}

// TestSweepHierarchy_AGuestFreezeAfterTheScanIsStillTheGuests: the record comes from a scan taken
// before the sweep writes anything, and the guest keeps running until the pause completes -- so a
// cgroup it freezes in between was, on the pre-walk code, simply left alone, and became ours to
// clear once the thaw started discovering. Re-reading the request immediately before writing closes
// the half of that window a sweep can see: whatever is frozen at that moment was frozen by someone
// else, because a sweep never reads back its own writes.
func TestSweepHierarchy_AGuestFreezeAfterTheScanIsStillTheGuests(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer", "late")
	late := filepath.Join(root, "late")
	f.mgr = &freezeAfterScan{PathManager: f.mgr.(PathManager), target: late}

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Positive(t, res.PreFrozen, "the late freeze is reported as one we are leaving alone")

	f.sweepMu.Lock()
	_, recorded := f.guestFrozen[late]
	f.sweepMu.Unlock()
	assert.True(t, recorded, "and it is in the record, which is what the thaw reads")

	// The whole point: the resume must not clear it.
	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.Positive(t, thaw.Preserved)
	assert.False(t, frozenOnDisk(t, root, "customer"), "while what WE froze is still thawed")
}

// TestSweepHierarchy_NoRescanDoesNotAdoptOurOwnFreeze is the guard on the above. A second freeze
// inside the same frozen window takes no new scan, and there "frozen and not in the record"
// describes our own cgroups exactly as well as the guest's -- adopting them would hand the whole
// workload to the guest and leave every later thaw preserving it. The live-upgrade handover takes
// the freeze again inside the window, so this is the ordinary path, not a corner.
func TestSweepHierarchy_NoRescanDoesNotAdoptOurOwnFreeze(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")
	newSettling(t, f, root) // so the second freeze sees the first one's state settled

	first, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	require.Zero(t, first.PreFrozen)

	// Second freeze in the same window: freezeInEffect is true, so no rescan.
	second, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	assert.Zero(t, second.PreFrozen, "our own freeze must never be adopted as the guest's")

	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.Zero(t, thaw.Preserved)
	assert.False(t, frozenOnDisk(t, root, "customer"), "and the workload is released")
}

// TestSweepHierarchy_TruncationKeepsTheLateGuestFreezes: the bound returns from the middle of the
// walk, so anything collected before it has to survive that return. A cgroup skipped as the guest's
// but never written into the record is strictly worse than not having looked at all -- the sweep
// leaves it frozen and the thaw, finding it unrecorded, clears it. That is the failure the adoption
// exists to prevent, reintroduced by an early return.
func TestSweepHierarchy_TruncationKeepsTheLateGuestFreezes(t *testing.T) {
	t.Parallel()

	// The late-frozen cgroup sorts first, so the walk reaches it before the bound bites.
	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service",
		"aaa-late", "bbb-one", "ccc-two", "ddd-three", "eee-four")
	late := filepath.Join(root, "aaa-late")
	f.mgr = &freezeAfterScan{PathManager: f.mgr.(PathManager), target: late}

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy, MaxCgroups: 3})
	require.NoError(t, err)
	require.True(t, res.Truncated, "the bound must bite, or this tests the ordinary path")

	f.sweepMu.Lock()
	_, recorded := f.guestFrozen[late]
	f.sweepMu.Unlock()
	assert.True(t, recorded, "a cgroup adopted before the bound must still be in the record")
	assert.Positive(t, res.PreFrozen, "and reported")
}

// failAllFreezes fails every freeze write, by ProcessType and by path, which is what it takes for a
// hierarchy sweep to reach Requested == 0: the static pass runs first and unconditionally, so
// failing only that leaves the walk free to write and the count non-zero.
type failAllFreezes struct {
	PathManager
}

func (m *failAllFreezes) Freeze(ProcessType) error {
	return errors.New("write cgroup.freeze: no such file or directory")
}

func (m *failAllFreezes) FreezeAt(string) error {
	return errors.New("write cgroup.freeze: no such file or directory")
}

// TestFreeze_ASweepThatWroteNothingIsNotAFreezeOfOurs: freezeActive is what tells a later thaw
// that discovery is its business, so it has to mean "we left something frozen" and not "a freeze
// call happened". A sweep whose every write failed left nothing of ours stopped, and a thaw that
// discovered on the strength of it would clear cgroups this process never wrote -- the guest's own,
// on a guest whose cgroup writes are broken. Same condition as the watchdog, which was already
// gated this way: the two must agree, because they answer the same question.
func TestFreeze_ASweepThatWroteNothingIsNotAFreezeOfOurs(t *testing.T) {
	t.Parallel()

	// Nothing freezable: envd's chain plus one allowlisted cgroup, so the walk requests nothing
	// and the static pass is the only writer -- and it fails.
	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "socats")
	freezeOnDisk(t, root, "socats") // the guest froze an allowlisted cgroup: never ours to record
	f.mgr = &failAllFreezes{PathManager: f.mgr.(PathManager)}

	res, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.Error(t, err, "the static writes failed, and that is reported")
	require.Zero(t, res.Requested, "so this sweep wrote nothing")
	assert.False(t, f.freezeInEffect(), "and nothing of ours is frozen")

	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.False(t, thaw.Discovered, "a sweep that wrote nothing must not license a discovering thaw")
	assert.True(t, frozenOnDisk(t, root, "socats"), "so what the guest froze is still frozen")
}

// TestFreeze_ASweepThatWroteNothingDoesNotCloseAnOpenWindow is the other half: the flag also stops
// the NEXT sweep inside the same frozen window from rescanning and adopting our own cgroups as the
// guest's. A later sweep that happens to write nothing must therefore not clear it -- those cgroups
// are still frozen.
func TestFreeze_ASweepThatWroteNothingDoesNotCloseAnOpenWindow(t *testing.T) {
	t.Parallel()

	f, root := newTreeFixture(t, "/system.slice/envd.service",
		"system.slice", "system.slice/envd.service", "customer")

	first, err := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.NoError(t, err)
	require.Positive(t, first.Requested)

	// A second sweep whose writes all fail: it wrote nothing, but the first one's cgroups are
	// still frozen, so the window is still open.
	f.mgr = &failAllFreezes{PathManager: f.mgr.(PathManager)}
	second, _ := f.Freeze(t.Context(), FreezeOptions{Mode: ModeHierarchy})
	require.Zero(t, second.Requested, "the second sweep wrote nothing, which is the case under test")
	assert.True(t, f.freezeInEffect(), "the window an earlier sweep opened is still open")

	// And the thaw still discovers, which is what the open window buys.
	thaw, err := f.UnfreezeReporting(t.Context(), DefaultThawMaxCgroups)
	require.NoError(t, err)
	assert.True(t, thaw.Discovered)
	assert.False(t, frozenOnDisk(t, root, "customer"), "so the first sweep's freeze is released")
}
