package process

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	fs "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

// mustMarshalHandover proto-encodes a HandoverState for the on-disk blob tests.
func mustMarshalHandover(t *testing.T, st *upgrade.HandoverState) []byte {
	t.Helper()
	b, err := proto.Marshal(st)
	require.NoError(t, err)

	return b
}

// spyCgroupManager records Unfreeze calls so we can assert the workload is
// always thawed. Everything else is a no-op.
type spyCgroupManager struct {
	unfreezes atomic.Int64
	// neverFreezes makes cgroup.events never report frozen, i.e. a workload whose
	// tasks are stuck in an uninterruptible wait.
	neverFreezes bool
	// unobservable models a guest with no cgroup manager, where freeze state cannot be
	// read back at all.
	unobservable bool
}

func (m *spyCgroupManager) GetFileDescriptor(cgroups.ProcessType) (int, bool) { return -1, false }
func (m *spyCgroupManager) Freeze(cgroups.ProcessType) error                  { return nil }

func (m *spyCgroupManager) Frozen(cgroups.ProcessType) (bool, error) {
	if m.unobservable {
		return false, cgroups.ErrFrozenUnobservable
	}

	return !m.neverFreezes, nil
}

func (m *spyCgroupManager) Unfreeze(cgroups.ProcessType) error {
	m.unfreezes.Add(1)

	return nil
}
func (m *spyCgroupManager) Close() error { return nil }

func newHandoverTestService(t *testing.T, spy *spyCgroupManager) *Service {
	t.Helper()
	logger := zerolog.Nop()
	cwd := t.TempDir()

	return newService(&logger, &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		Workdir: &cwd,
	}, cgroups.NewWorkloadFreezer(spy))
}

// TestUpgrade_RejectsUnexpectedBinary verifies the exec target is constrained:
// a caller-supplied path other than the fixed DefaultUpgradeBinPath (or empty
// self-exec) is refused before any side effects, so a malformed/forged upgrade
// request can't turn the same-PID exec into arbitrary code execution.
func TestUpgrade_RejectsUnexpectedBinary(t *testing.T) {
	t.Parallel()

	s := newHandoverTestService(t, &spyCgroupManager{})

	err := s.Upgrade("/tmp/attacker-controlled", "0.6.11", nil, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing upgrade")
}

// TestResumeFromHandoverAlwaysUnfreezes is the safety guarantee: however the
// handover resume ends — a malformed blob (error) or no blob at all — the
// workload is thawed. A failed upgrade must never leave the sandbox frozen.
//
//nolint:paralleltest // mutates the package-global HandoverPath; must run serially
func TestResumeFromHandoverAlwaysUnfreezes(t *testing.T) {
	orig := HandoverPath
	t.Cleanup(func() { HandoverPath = orig })

	// Malformed blob -> ResumeFromHandover errors, but must still thaw. A lone
	// varint tag with no payload is invalid protobuf wire format.
	spy := &spyCgroupManager{}
	s := newHandoverTestService(t, spy)
	HandoverPath = filepath.Join(t.TempDir(), "handover.pb")
	require.NoError(t, os.WriteFile(HandoverPath, []byte{0x08}, 0o600))

	_, err := s.ResumeFromHandover(nil)
	require.Error(t, err, "a malformed blob should surface an error")
	assert.Positive(t, spy.unfreezes.Load(), "workload must be thawed even on a bad blob")

	// No blob -> no-op return, but the deferred thaw must still run.
	spy2 := &spyCgroupManager{}
	s2 := newHandoverTestService(t, spy2)
	HandoverPath = filepath.Join(t.TempDir(), "absent.pb")

	_, err = s2.ResumeFromHandover(nil)
	require.NoError(t, err)
	assert.Positive(t, spy2.unfreezes.Load(), "the deferred thaw must run on the no-blob path too")
}

// TestResumeFromHandover_ReArmsWatchersBeforeThaw verifies the incoming handover
// re-arms filesystem watchers (via the callback) while the workload is STILL
// frozen — before the deferred thaw — so no filesystem event can be missed in
// the gap between the thaw and the re-arm.
//
//nolint:paralleltest // mutates the package-global HandoverPath; must run serially
func TestResumeFromHandover_ReArmsWatchersBeforeThaw(t *testing.T) {
	orig := HandoverPath
	t.Cleanup(func() { HandoverPath = orig })

	spy := &spyCgroupManager{}
	s := newHandoverTestService(t, spy)
	HandoverPath = filepath.Join(t.TempDir(), "handover.pb")
	// A valid proto blob with no processes: drives the success path through to
	// the watcher-rearm callback.
	require.NoError(t, os.WriteFile(HandoverPath,
		mustMarshalHandover(t, &upgrade.HandoverState{Schema: handoverSchema, FromVer: "0.6.12"}), 0o600))

	unfreezesAtCallback := int64(-1)
	_, err := s.ResumeFromHandover(func([]*upgrade.HandoverWatcher) (int, int) {
		unfreezesAtCallback = spy.unfreezes.Load()

		return 0, 0
	})
	require.NoError(t, err)

	assert.Equal(t, int64(0), unfreezesAtCallback, "watchers must be re-armed before any thaw")
	assert.Equal(t, int64(0), spy.unfreezes.Load(), "on success the workload stays frozen for the post-upgrade /init to thaw (so no re-adopted process runs before /init restores auth)")
}

// TestHandoverState_ProtoRoundTrip is the core wire-format contract: a
// HandoverState carrying native nested messages (a process config, a retained
// terminal event, and a watcher with buffered filesystem events) survives a
// proto.Marshal -> proto.Unmarshal round-trip unchanged. This is what replaced
// the old JSON+protojson encoding.
func TestHandoverState_ProtoRoundTrip(t *testing.T) {
	t.Parallel()

	orig := &upgrade.HandoverState{
		Schema:  handoverSchema,
		FromVer: "0.6.12",
		Processes: []*upgrade.HandoverProc{{
			Pid:       1214,
			Tag:       "worker",
			HasTag:    true,
			CgType:    "user",
			Config:    &rpc.ProcessConfig{Cmd: "/bin/sh", Cwd: new("/root"), Args: []string{"-c", "echo hi"}},
			StdoutFd:  200,
			StderrFd:  201,
			StdinFd:   202,
			TtyFd:     -1,
			TimeoutMs: 5000,
		}},
		Terminated: []*upgrade.HandoverExit{{
			Pid:         99,
			End:         &rpc.ProcessEvent_EndEvent{ExitCode: 42, Status: "exited"},
			RemainingMs: 1000,
		}},
		Watchers: []*upgrade.HandoverWatcher{{
			Id:               "w1",
			Path:             "/data",
			Recursive:        true,
			IncludeEntryInfo: true,
			PendingEvents:    []*fs.FilesystemEvent{{Name: "a.txt", Type: fs.EventType_EVENT_TYPE_CREATE}},
		}},
		Mounts: []*upgrade.MountEntry{{Path: "/mnt/vol", LifecycleId: "lc-1"}},
		Forwards: []*upgrade.ForwardedPort{{
			Key: "100-8080", Port: 8080, ListenerPid: 100, Family: 4, SocatPid: 555,
		}},
		HelperPids: []int32{777},
	}

	blob, err := proto.Marshal(orig)
	require.NoError(t, err)

	got := &upgrade.HandoverState{}
	require.NoError(t, proto.Unmarshal(blob, got))

	assert.True(t, proto.Equal(orig, got), "round-tripped HandoverState must equal the original")
	// Spot-check the nested messages explicitly so a failure points at the field.
	require.Len(t, got.GetProcesses(), 1)
	assert.Equal(t, "/bin/sh", got.GetProcesses()[0].GetConfig().GetCmd())
	assert.Equal(t, int32(-1), got.GetProcesses()[0].GetTtyFd())
	require.Len(t, got.GetTerminated(), 1)
	assert.Equal(t, int32(42), got.GetTerminated()[0].GetEnd().GetExitCode())
	require.Len(t, got.GetWatchers(), 1)
	require.Len(t, got.GetWatchers()[0].GetPendingEvents(), 1)
	assert.Equal(t, "a.txt", got.GetWatchers()[0].GetPendingEvents()[0].GetName())
	require.Len(t, got.GetMounts(), 1)
	assert.Equal(t, "/mnt/vol", got.GetMounts()[0].GetPath())
	assert.Equal(t, "lc-1", got.GetMounts()[0].GetLifecycleId())
	require.Len(t, got.GetForwards(), 1)
	assert.Equal(t, "100-8080", got.GetForwards()[0].GetKey())
	assert.Equal(t, int32(555), got.GetForwards()[0].GetSocatPid())
	assert.Equal(t, []int32{777}, got.GetHelperPids())
}

// TestResumeFromHandover_RejectsNewerSchema is the §6.4 version gate: a blob
// written by a hypothetical newer envd (schema > this binary's max) is refused
// rather than mis-read, and the workload is still thawed.
//
//nolint:paralleltest // mutates the package-global HandoverPath; must run serially
func TestResumeFromHandover_RejectsNewerSchema(t *testing.T) {
	orig := HandoverPath
	t.Cleanup(func() { HandoverPath = orig })

	spy := &spyCgroupManager{}
	s := newHandoverTestService(t, spy)
	HandoverPath = filepath.Join(t.TempDir(), "handover.pb")
	require.NoError(t, os.WriteFile(HandoverPath,
		mustMarshalHandover(t, &upgrade.HandoverState{Schema: handoverSchema + 1, FromVer: "9.9.9"}), 0o600))

	_, err := s.ResumeFromHandover(nil)
	require.Error(t, err, "a newer-than-known schema must be refused")
	assert.Contains(t, err.Error(), "schema")
	assert.Positive(t, spy.unfreezes.Load(), "workload must be thawed even when the schema is rejected")
}

// TestFreezeWorkloadHold_RefusesSwapOverRunningWorkload pins the handover's strict
// policy. Unlike a pause -- which must never fail because a customer task will not
// stop -- the handover has a cheap safe alternative: abort and leave the current
// envd running. execve-ing over a workload that is still executing is exactly what
// the freeze exists to prevent, so a workload still running must stop the swap.
func TestFreezeWorkloadHold_RefusesSwapOverRunningWorkload(t *testing.T) {
	t.Parallel()

	spy := &spyCgroupManager{neverFreezes: true}
	svc := newHandoverTestService(t, spy)
	svc.handoverMaxWait = 20 * time.Millisecond

	release, err := svc.FreezeWorkloadHold()
	t.Cleanup(release)

	require.ErrorIs(t, err, ErrWorkloadNotFrozen)
}

// TestFreezeWorkloadHold_ProceedsWhenFreezeStateUnobservable covers the guest that
// cannot report freeze state at all (--no-cgroups, or a failed cgroup setup). Refusing
// there would newly block every live upgrade on such a guest while protecting nothing:
// the observation this policy depends on was never available, before this change or
// after it.
func TestFreezeWorkloadHold_ProceedsWhenFreezeStateUnobservable(t *testing.T) {
	t.Parallel()

	svc := newHandoverTestService(t, &spyCgroupManager{unobservable: true})
	svc.handoverMaxWait = 10 * time.Second

	start := time.Now()
	release, err := svc.FreezeWorkloadHold()
	t.Cleanup(release)

	require.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second, "must not wait out the budget")
}

// TestFreezeWorkloadHold_ProceedsWhenFrozen is the counterpart: a workload that
// does stop must not block the upgrade.
func TestFreezeWorkloadHold_ProceedsWhenFrozen(t *testing.T) {
	t.Parallel()

	svc := newHandoverTestService(t, &spyCgroupManager{})

	release, err := svc.FreezeWorkloadHold()
	t.Cleanup(release)

	require.NoError(t, err)
}

// TestSchemaFor_StampsTheLowestVersionThatCarriesTheBlob: the schema a writer stamps is what
// decides whether a PREVIOUS envd can read the blob, and the read happens post-execve, where a
// refusal is not a graceful decline -- there is no old binary left, so the workload is never
// re-adopted and the sandbox is torn down. Stamping the maximum unconditionally therefore traded
// away every rollback, including the overwhelming majority of blobs that carry no
// guest_frozen_cgroups at all and are byte-for-byte readable by the older reader.
func TestSchemaFor_StampsTheLowestVersionThatCarriesTheBlob(t *testing.T) {
	t.Parallel()

	t.Run("no guest-frozen cgroups reads as the base schema", func(t *testing.T) {
		t.Parallel()

		st := &upgrade.HandoverState{FromVer: "0.6.13", Mounts: []*upgrade.MountEntry{{Path: "/mnt/v"}}}
		assert.Equal(t, uint32(handoverSchemaBase), schemaFor(st),
			"nothing in this blob is newer than the base layout, so nothing should refuse it")
	})

	// The other direction has to hold just as firmly: a reader that predates the field would
	// drop the record and then thaw cgroups the guest froze itself, so the stamp must refuse it.
	t.Run("a carried record forces the newer schema", func(t *testing.T) {
		t.Parallel()

		st := &upgrade.HandoverState{FromVer: "0.6.13", GuestFrozenCgroups: []string{"customer/c1"}}
		assert.Equal(t, uint32(handoverSchemaGuestFrozen), schemaFor(st))
	})
}
