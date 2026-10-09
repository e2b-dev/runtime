package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/services/process/handler"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
	"github.com/e2b-dev/infra/packages/envd/pkg"
)

// HandoverPath is the tmpfs blob the outgoing envd writes and the incoming one
// reads across a live self-upgrade. The format is a protobuf-encoded
// upgrade.HandoverState (spec/upgrade/handover.proto) — an additive schema, so
// an outgoing and incoming envd built at different versions stay compatible.
// /run is tmpfs, so it never touches the rootfs diff. A var (not a const) so
// tests can point it at a temp file.
var HandoverPath = "/run/e2b/envd-handover.pb"

// handoverSchema is the maximum HandoverState schema this envd will read: a reader refuses a
// blob whose schema exceeds it (design §6.4 — decode every schema <= N, abort on schema > N)
// rather than mis-read a newer-than-known layout.
//
// Refusing is a hard failure, not a graceful decline. The read happens after the execve, in the
// new image, so there is no old binary left to fall back to: the handover is abandoned, the
// workload is not re-adopted, and SetHandoverResult(failed) tells the orchestrator to tear the
// sandbox down. That cost is why the version a writer STAMPS is chosen per blob
// (schemaFor) rather than pinned to this maximum.
//
// It also constrains how a change to this file is rolled back. Version resolution is
// monotonic — the upgrade target must be strictly newer than the running envd — so a reader
// can never be an older VERSION than the writer, but it can know less: a binary whose version
// sorts above the fleet's while its source predates a field refuses every blob that carries
// that field, and each refusal fails a live resume. Reverting a field here is therefore not a
// rollback while the live-upgrade flag names a target; turning that flag off is, and it takes
// effect immediately, because this blob is tmpfs and is written and read seconds apart inside
// one upgrade. Same trap for an experiment pointed at a branch build: do not aim one that
// lacks a field at a fleet whose envd has it.
const handoverSchema = handoverSchemaDefaults

// The schema versions themselves. A writer stamps the LOWEST version that can express the blob
// it actually produced, so a field's mere existence in the layout costs nothing at runtime.
const (
	// handoverSchemaBase is every field through forwarded ports.
	handoverSchemaBase = 1
	// handoverSchemaGuestFrozen adds guest_frozen_cgroups.
	handoverSchemaGuestFrozen = 2
	// handoverSchemaDefaults adds defaults (the exec context's user/workdir).
	handoverSchemaDefaults = 3
)

// schemaFor is the lowest schema that can carry st without loss.
//
// guest_frozen_cgroups is absent-safe on the wire, but a reader that predates it would drop the
// record and then thaw cgroups the guest froze itself -- so a blob that carries the list must be
// refused by such a reader, and stamping 2 is what refuses it. A blob with an EMPTY list is a
// different case: there is nothing for an older reader to lose, and stamping 2 anyway would fail
// the upgrade for a peer that could have handled the blob perfectly. Empty is the common case,
// since most guests freeze nothing of their own, so pinning the stamp at the maximum would trade
// away nearly every downgrade for a guarantee only the non-empty blob needs.
func schemaFor(st *upgrade.HandoverState) uint32 {
	// A carried default that differs from the compiled-in fallback is behaviour an
	// older reader would drop, and dropping it means the new image serves requests as
	// somebody else. Stamp so such a reader refuses instead. When the default already
	// IS the fallback there is nothing to lose, so do not spend a refusal on it --
	// same reasoning as guest_frozen_cgroups below.
	//
	// Unlike the empty-cgroup list, though, the cheap case is NOT the common one here:
	// most templates record a default user, so most blobs really do stamp 3. What that
	// costs is not downgrades but ROLLBACK -- while the live-upgrade flag names a
	// target, reverting this field does not roll the change back, because a reader that
	// knows less refuses every blob carrying it and each refusal fails a live resume.
	// Turning the flag off is the rollback. See handoverSchema above for the full
	// argument.
	if d := st.GetDefaults(); d != nil &&
		((d.GetUser() != "" && d.GetUser() != execcontext.BuiltinDefaultUser) || d.GetHasWorkdir()) {
		return handoverSchemaDefaults
	}
	if len(st.GetGuestFrozenCgroups()) > 0 {
		return handoverSchemaGuestFrozen
	}

	return handoverSchemaBase
}

// fdBase is where carried fds are dup3'd to; high enough that the fresh runtime
// in the new image won't have grabbed these numbers during early startup.
const fdBase = 200

// DefaultUpgradeBinPath is the only filesystem path a live upgrade will write a
// delivered binary to and re-exec into. Constraining the target (rather than
// trusting a request header or marker file) keeps a malformed or forged upgrade
// request from writing to / executing an arbitrary path. The /upgrade endpoint
// is authenticated, but this is defense-in-depth around a same-PID exec.
const DefaultUpgradeBinPath = "/usr/bin/envd.next"

// dupKeep dup3's oldfd onto target with CLOEXEC cleared so it survives execve.
// Returns (target, nil) on success and (-1, nil) when oldfd is absent. It errors
// if target is already open: dup3 silently closes the occupant, so a target
// collision must abort the upgrade rather than corrupt a live fd. The caller
// holds syscall.ForkLock, so no concurrent Go fd allocation can claim target
// between the F_GETFD check and the dup3.
func dupKeep(oldfd, target int) (int, error) {
	if oldfd < 0 {
		return -1, nil
	}
	if _, err := unix.FcntlInt(uintptr(target), unix.F_GETFD, 0); err == nil {
		return -1, fmt.Errorf("handover fd target %d already in use", target)
	}
	if err := dup3(oldfd, target, 0); err != nil {
		return -1, fmt.Errorf("dup3 %d->%d: %w", oldfd, target, err)
	}

	return target, nil
}

// Upgrade is the outgoing side of a live self-upgrade. It must be
// called with the workload frozen and envd's own spawners quiesced (caller's
// responsibility — see main's /upgrade handler). It serializes the process
// table, carries the I/O fds across execve, and re-execs newBin with the same
// PID. It does not return on success.
func (s *Service) Upgrade(newBin, fromVer string, watchers []*upgrade.HandoverWatcher, mounts []*upgrade.MountEntry, forwards []*upgrade.ForwardedPort, helpers []int32) error {
	// Only re-exec self (empty) or the fixed delivered-binary path — never an
	// arbitrary caller-supplied path. Checked first, before any side effects.
	if newBin != "" && newBin != DefaultUpgradeBinPath {
		return fmt.Errorf("refusing upgrade to unexpected binary %q", newBin)
	}

	st := &upgrade.HandoverState{
		FromVer:    fromVer,
		Watchers:   watchers,
		Mounts:     mounts,
		Forwards:   forwards,
		HelperPids: helpers,
		// Which cgroups the GUEST had frozen before the pre-pause sweep. Read from the
		// freezer here rather than plumbed in by the caller: it is the freezer's own
		// state, and the new image needs it before its /init thaws anything.
		GuestFrozenCgroups: s.workloadFreezer.GuestFrozenPaths(),
		// The exec context this envd was serving with. The new image rebuilds its
		// defaults from scratch after the execve, so without this it depends entirely on
		// the orchestrator's post-upgrade /init to re-send them.
		Defaults: handoverDefaults(s.defaults),
	}
	st.Schema = schemaFor(st)

	// Serialize the process table and relocate each carried fd to its fixed
	// target, holding syscall.ForkLock so no concurrent Go fd allocation can
	// claim a target between dupKeep's free-check and its dup3. A collision or
	// dup3 failure aborts the upgrade with the workload intact (handled below).
	var (
		dupErr error
		dupped []int
	)
	i := 0
	// snapshotMu (write) blocks concurrent Start registration for the whole
	// snapshot→execve window, so no child is spawned-but-unregistered (and thus
	// left behind, unconnectable, across the swap). Taken before ForkLock and
	// released on every exit path alongside it — consistent order, no deadlock
	// (handleStart takes RLock then ForkLock via fork; Upgrade takes them in the
	// same order).
	s.snapshotMu.Lock()
	syscall.ForkLock.Lock()
	s.processes.Range(func(_ uint32, h *handler.Handler) bool {
		stdout, stderr, stdin, tty := h.HandoverFds()
		slot := fdBase + i*5

		hp := &upgrade.HandoverProc{
			Pid:    h.Pid(),
			CgType: string(h.CgType()),
			// Native nested message — no protojson round-trip. The live
			// *rpc.ProcessConfig is carried directly and proto.Marshal encodes it.
			Config: h.Config,
		}
		for _, m := range []struct {
			old int
			tgt int
			dst *int32
		}{
			{stdout, slot + 0, &hp.StdoutFd},
			{stderr, slot + 1, &hp.StderrFd},
			{stdin, slot + 2, &hp.StdinFd},
			{tty, slot + 3, &hp.TtyFd},
		} {
			fd, err := dupKeep(m.old, m.tgt)
			if err != nil {
				dupErr = err

				return false
			}
			*m.dst = int32(fd)
			if fd >= 0 {
				dupped = append(dupped, fd)
			}
		}
		// Carry the remaining timeout so it is re-armed on the new envd. A
		// deadline already in the past is clamped to 1ms (kill ASAP).
		if d, ok := h.Deadline(); ok {
			if rem := time.Until(d).Milliseconds(); rem > 0 {
				hp.TimeoutMs = rem
			} else {
				hp.TimeoutMs = 1
			}
		}
		if h.Tag != nil {
			hp.Tag = *h.Tag
			hp.HasTag = true
		}
		st.Processes = append(st.Processes, hp)
		i++

		return true
	})
	if dupErr != nil {
		// Close the fds already relocated so a retry sees a clean target window,
		// then keep running the old binary.
		for _, fd := range dupped {
			_ = unix.Close(fd)
		}
		syscall.ForkLock.Unlock()
		s.snapshotMu.Unlock()

		return fmt.Errorf("relocate handover fds: %w", dupErr)
	}

	// Keep ForkLock held from the CLOEXEC-clearing relocation above all the way
	// through the execve below. Dropping it here would leave the carried fds
	// (now CLOEXEC-cleared) exposed to a concurrent os/exec fork — the port
	// scanner's socat, an in-flight Start — which would inherit them. The
	// intervening marshal / write / os.Executable never fork, so holding it is
	// safe. On any error return before the execve (which never returns on
	// success — it replaces the image), close the relocated dups so they don't
	// leak into the still-running old envd and its future children, and release
	// the lock. Mirrors the dupErr cleanup above.
	defer func() {
		for _, fd := range dupped {
			_ = unix.Close(fd)
		}
		syscall.ForkLock.Unlock()
		s.snapshotMu.Unlock()
	}()

	// Carry the retention cache so a process that exited shortly before the
	// upgrade keeps its exit code retrievable on the new envd.
	s.terminated.Range(func(pid uint32, r *retainedExit) bool {
		rem := time.Until(r.expiry).Milliseconds()
		if rem <= 0 {
			return true
		}

		he := &upgrade.HandoverExit{Pid: pid, End: r.end, RemainingMs: rem}
		if r.tag != nil {
			he.Tag = *r.tag
			he.HasTag = true
		}
		st.Terminated = append(st.Terminated, he)

		return true
	})

	blob, err := proto.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal handover: %w", err)
	}
	if err := os.WriteFile(HandoverPath, blob, 0o600); err != nil {
		return fmt.Errorf("write handover: %w", err)
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	if newBin == "" {
		newBin = exe
	}

	// argv: keep original flags, append --resume-handover.
	argv := append([]string{newBin}, os.Args[1:]...)
	argv = append(argv, "--resume-handover")

	// Carry the dup'd fds across execve. ForkLock is still held from the
	// relocation above (released by the deferred cleanup), so no concurrent fork
	// can inherit the CLOEXEC-cleared fds. Exec only returns on failure
	// (corrupt/missing staged binary); the deferred cleanup then closes the
	// relocated dups and releases the lock while the old envd keeps running.
	err = syscall.Exec(newBin, argv, os.Environ())

	return fmt.Errorf("execve %s: %w", newBin, err)
}

// ResumeFromHandover is the incoming side: read the blob, re-adopt
// each process from its inherited fds, register it, then thaw the workload.
// No-op if no handover file is present.
// It returns the opaque filesystem-watcher blob (if any) so the caller can hand
// it to the filesystem service to re-arm watches.
// HandoverResult summarizes a completed incoming handover. It is surfaced to the
// orchestrator via the /init X-Envd-Handover header so the envd-side outcome
// (which envd otherwise only logs) is observable fleet-wide.
type HandoverResult struct {
	// Every item is total-carried + failed-subset (ok = total - failed).
	Procs          int
	ProcsFailed    int
	Retained       int
	RetainedFailed int
	Watchers       int
	WatchersFailed int
	// Mounts and Forwards are decoded from the blob and handed back to their
	// owners (the API service's mount ledger and the port forwarder), which are
	// constructed after ResumeFromHandover runs — so unlike watchers they are
	// returned rather than applied via a callback.
	Mounts   []*upgrade.MountEntry
	Forwards []*upgrade.ForwardedPort
	Helpers  []int32
}

func (s *Service) ResumeFromHandover(reArmWatchers func([]*upgrade.HandoverWatcher) (rearmed, failed int)) (HandoverResult, error) {
	// Thaw the workload on every FAILURE path — a bad blob, partial re-adopt, or
	// panic must never leave the sandbox frozen (a degraded-but-running workload
	// beats a hung one). On SUCCESS the workload is deliberately left frozen: the
	// orchestrator's post-upgrade /init thaws it (the thaw in PostInit)
	// only after it has re-established the access token. This closes the window
	// in which a re-adopted — and possibly hostile — guest process could run
	// before /init restores auth and reach the unauthenticated /upgrade endpoint
	// (which execs request-body bytes as root).
	keepFrozen := false
	defer func() {
		if !keepFrozen {
			s.UnfreezeWorkload()
		}
	}()

	blob, err := os.ReadFile(HandoverPath)
	if os.IsNotExist(err) {
		return HandoverResult{}, nil
	}
	if err != nil {
		return HandoverResult{}, fmt.Errorf("read handover: %w", err)
	}

	st := &upgrade.HandoverState{}
	if err := proto.Unmarshal(blob, st); err != nil {
		return HandoverResult{}, fmt.Errorf("unmarshal handover: %w", err)
	}

	// Schema gate (design §6.4): refuse a blob written by a newer envd whose layout this binary
	// does not understand, rather than mis-read it. This runs post-execve, so the outgoing envd
	// is gone and there is nothing to fall back to -- the caller reports the handover failed and
	// the orchestrator tears the sandbox down, which is the intended outcome. A refusal is worse
	// than a successful handover and better than a silently mis-decoded one.
	if st.GetSchema() > handoverSchema {
		return HandoverResult{}, fmt.Errorf("handover schema %d exceeds max supported %d", st.GetSchema(), handoverSchema)
	}

	// Restore the exec context BEFORE any re-adopted process can be interacted with,
	// so a request that arrives between here and the post-upgrade /init resolves
	// against the identity the outgoing envd was serving with rather than the
	// compiled-in fallback. Absent from a blob written by an envd predating the field,
	// in which case the fallback stands and /init remains the only source.
	if userRestored, workdirRestored := ApplyHandoverDefaults(st, s.defaults); userRestored || workdirRestored {
		// Name the fields actually applied. The blob carries them independently, so a
		// message that always says "default user" would report the compiled-in fallback
		// as a restored identity on a workdir-only blob.
		fmt.Fprintf(os.Stderr, "envd: restored exec context from the handover blob (user=%t workdir=%t)\n",
			userRestored, workdirRestored)
	}

	// journald-visible proof of the running image after the swap (from_ver is
	// the outgoing version; pkg.Version is what this new image is).
	fmt.Fprintf(os.Stderr, "envd: resumed as v%s after handover (from v%s, %d procs)\n",
		pkg.Version, st.GetFromVer(), len(st.GetProcesses()))

	// fileOrNil wraps a carried fd (inherited at its fixed fdBase slot with
	// CLOEXEC cleared, from the outgoing envd's execve) as an *os.File, but first
	// RELOCATES it off the fdBase range onto a fresh fd with CLOEXEC set. This
	// frees the slot for the NEXT upgrade's relocation: a chained upgrade dup3's
	// the carried fds back onto the same fdBase slots, and dupKeep aborts if a
	// target is already in use — so leaving them parked at fdBase would break the
	// second swap. Setting CLOEXEC also stops the fd from leaking into processes
	// this envd spawns before the next upgrade (the next dupKeep re-clears it).
	fileOrNil := func(fd int, name string) *os.File {
		if fd < 0 {
			return nil
		}

		fresh, err := unix.Dup(fd)
		if err != nil {
			// Fall back to the slot as-is: the current re-adoption still works;
			// only a later chained upgrade might collide.
			s.logger.Warn().Err(err).Int("fd", fd).Msg("handover: relocate carried fd off fdBase failed")

			return os.NewFile(uintptr(fd), name)
		}
		unix.CloseOnExec(fresh)
		_ = unix.Close(fd)

		return os.NewFile(uintptr(fresh), name)
	}

	// Captured before re-adoption (while the workload is still frozen): closed
	// when the post-upgrade /init — or the fallback — next thaws the workload, so
	// each re-adopted process's carried kill-timer only starts counting once the
	// process can actually run again.
	thawed := s.workloadFreezer.Thawed()

	procsFailed := 0
	for _, hp := range st.GetProcesses() {
		// Native nested message — carried directly by proto, no protojson
		// round-trip. A malformed config would have failed the top-level
		// proto.Unmarshal above (schema-gated), so here it is either the real
		// config or absent; default an absent one so Readopt always gets non-nil.
		cfg := hp.GetConfig()
		if cfg == nil {
			cfg = &rpc.ProcessConfig{}
		}

		var tag *string
		if hp.GetHasTag() {
			t := hp.GetTag()
			tag = &t
		}

		var timeout time.Duration
		if hp.GetTimeoutMs() > 0 {
			timeout = time.Duration(hp.GetTimeoutMs()) * time.Millisecond
		}

		h := handler.Readopt(handler.ReadoptArgs{
			Pid:     hp.GetPid(),
			Tag:     tag,
			Config:  cfg,
			CgType:  cgroups.ProcessType(hp.GetCgType()),
			Stdout:  fileOrNil(int(hp.GetStdoutFd()), fmt.Sprintf("p%d-stdout", hp.GetPid())),
			Stderr:  fileOrNil(int(hp.GetStderrFd()), fmt.Sprintf("p%d-stderr", hp.GetPid())),
			Stdin:   fileOrNil(int(hp.GetStdinFd()), fmt.Sprintf("p%d-stdin", hp.GetPid())),
			Tty:     fileOrNil(int(hp.GetTtyFd()), fmt.Sprintf("p%d-tty", hp.GetPid())),
			Timeout: timeout,
			Thawed:  thawed,
		}, s.logger)

		pid := hp.GetPid()
		s.processes.Store(pid, h)
		// Retain the terminal event synchronously on exit, before the reaper
		// closes EndEvent, so a Connect arriving in the handover gap always
		// recovers the exit code even if it forks after the close. Set before
		// BeginReaping: a process whose timeout expired during the freeze can
		// exit the instant it is unfrozen, so the reaper — which invokes the
		// hook — must never start before the hook is in place.
		h.OnExit = func(end *rpc.ProcessEvent_EndEvent) {
			s.finalizeTermination(pid, h, end)
		}
		h.BeginReaping()

		s.logger.Info().
			Str("event_type", "process_readopted").
			Uint32("pid", hp.GetPid()).
			Msg("re-adopted process after envd self-upgrade")
	}

	// Restore the retention cache: terminal events of processes that exited
	// shortly before the upgrade, so a Connect can still recover their exit code.
	retainedFailed := s.restoreTerminated(st.GetTerminated())

	_ = os.Remove(HandoverPath)

	// Re-arm filesystem watchers while the workload is STILL frozen (it stays
	// frozen past this return until the post-upgrade /init thaws it), so no
	// filesystem event is lost in the gap between the thaw and the re-arm.
	watchersRearmed, watchersFailed := 0, 0
	if reArmWatchers != nil {
		watchersRearmed, watchersFailed = reArmWatchers(st.GetWatchers())
	}

	// Loki-queryable summary of what the handover carried + how it fared
	// (rollout observability; the counts also ride to the orchestrator via the
	// /init X-Envd-Handover header — see HandoverResult).
	s.logger.Info().
		Str("event_type", "handover_resumed").
		Str("from_ver", st.GetFromVer()).
		Int("procs", len(st.GetProcesses())).
		Int("procs_failed", procsFailed).
		Int("retained", len(st.GetTerminated())).
		Int("retained_failed", retainedFailed).
		Int("watchers", watchersRearmed+watchersFailed).
		Int("watchers_failed", watchersFailed).
		Msg("re-adopted workload after envd self-upgrade")

	// Handover succeeded: keep the workload frozen (see the deferred thaw above);
	// the orchestrator's post-upgrade /init thaws it once auth is restored.
	// Restore before returning: the incoming envd's /init thaws the workload, and
	// without this the thaw would clear cgroups the guest itself had frozen. An absent
	// or empty list leaves the record empty, which thaws everything -- the safe
	// direction, and what an older outgoing envd produces.
	s.workloadFreezer.SetGuestFrozenPaths(st.GetGuestFrozenCgroups())

	// The workload stays frozen past this point, so tell the freezer it owns a live freeze
	// again: neither the freeze-active state, nor the watchdog timer, nor the spawn barrier
	// crossed the execve, and without them a freeze before the post-upgrade /init would adopt
	// our own frozen cgroups as the guest's, an /init that never arrives would leave the guest
	// frozen with no backstop, and a process start would be refused only by the pre-spawn
	// probe, which a failed read or a race with the clone can get past.
	//
	// Logged rather than returned: only the barrier can fail here, after the watchdog is
	// armed, and only by failing to drain spawns that a just-execve'd image cannot have.
	// Failing the handover over it would trip the deferred thaw and release a re-adopted
	// workload before /init restores auth, which is a worse outcome than a window guarded
	// only by the probe before a thaw that is already due.
	if err := s.workloadFreezer.ResumeFrozen(context.Background()); err != nil {
		s.logger.Error().Err(err).Msg("inherited a frozen workload without the spawn barrier: only the pre-spawn probe guards process starts until /init")
	}

	keepFrozen = true

	return HandoverResult{
		Procs:          len(st.GetProcesses()),
		ProcsFailed:    procsFailed,
		Retained:       len(st.GetTerminated()),
		RetainedFailed: retainedFailed,
		Watchers:       watchersRearmed + watchersFailed,
		WatchersFailed: watchersFailed,
		// Handed back to the API service (mount ledger) and port forwarder, which
		// are constructed after this returns.
		Mounts:   st.GetMounts(),
		Forwards: st.GetForwards(),
		Helpers:  st.GetHelperPids(),
	}, nil
}

// restoreTerminated re-populates the terminal-event retention cache from the
// handover. It skips any PID that was already re-adopted as a *live* process
// above: a PID can't be both live and terminated, and caching a stale exit
// under a live PID could let a Connect (or a later reuse of that PID) be served
// the wrong exit code. Mirrors handleStart's clear-on-register.
func (s *Service) restoreTerminated(entries []*upgrade.HandoverExit) (failed int) {
	for _, he := range entries {
		if _, live := s.processes.Load(he.GetPid()); live {
			continue
		}

		// Native nested message — carried directly by proto (schema-gated at
		// decode). Default an absent event so retain always gets a non-nil End.
		end := he.GetEnd()
		if end == nil {
			end = &rpc.ProcessEvent_EndEvent{}
		}

		var tag *string
		if he.GetHasTag() {
			t := he.GetTag()
			tag = &t
		}

		s.retain(he.GetPid(), &retainedExit{
			pid:    he.GetPid(),
			tag:    tag,
			end:    end,
			expiry: time.Now().Add(time.Duration(he.GetRemainingMs()) * time.Millisecond),
		})
	}

	return failed
}

// ErrWorkloadNotFrozen means the pre-handover freeze was issued but the workload
// had not stopped by the deadline, so the swap was refused.
var ErrWorkloadNotFrozen = errors.New("workload was not frozen before the handover")

// FreezeWorkloadHold freezes the workload and keeps the shared freeze lock held,
// returning a release func, so the freeze stays uninterruptible across the
// handover: a concurrent /init or /unfreeze thaw blocks until release. The caller
// MUST release on any path that does not execve; a successful execve drops the
// lock with the process image.
// Unlike the pause path, this one is STRICT: if the workload did not fully stop
// the handover is refused. execve-ing over a still-running workload is exactly what
// the freeze exists to prevent, and unlike a pause the handover has a safe
// alternative -- abort and thaw, leaving a working envd in place.
func (s *Service) FreezeWorkloadHold() (release func(), err error) {
	release, res, err := s.workloadFreezer.FreezeHold(context.Background(), cgroups.FreezeOptions{MaxWait: s.handoverMaxWait})
	if err != nil {
		s.logger.Warn().Err(err).Msg("handover: freeze failed")

		return release, err
	}
	if !res.AllFrozen() {
		s.logger.Warn().
			Int("frozen", res.Frozen).
			Int("not_frozen", res.NotFrozen).
			Int("failed", res.Failed).
			Dur("wait_duration", res.WaitDuration).
			Msg("handover: refusing to swap over a workload that did not stop")

		return release, ErrWorkloadNotFrozen
	}

	return release, nil
}

// UnfreezeWorkload thaws the user/pty cgroups. Idempotent (thawing a non-frozen
// cgroup is a no-op), so it is safe to call on every upgrade outcome — success,
// failure, or panic — guaranteeing a failed swap never leaves the workload frozen.
func (s *Service) UnfreezeWorkload() {
	if err := s.workloadFreezer.Unfreeze(context.Background()); err != nil {
		s.logger.Warn().Err(err).Msg("handover: unfreeze failed")
	}
}

// handoverDefaults converts the live exec context into its wire form. Env vars are
// deliberately omitted: /init replaces them wholesale, so carrying them would add a
// second source of truth for state that is already restored reliably.
func handoverDefaults(d *execcontext.Defaults) *upgrade.HandoverDefaults {
	if d == nil {
		return nil
	}
	hd := &upgrade.HandoverDefaults{}
	// Only a DELIVERED user is worth carrying. Carrying the compiled-in fallback would
	// make the incoming image report that it was told, when it was not — and the incoming
	// binary has that same fallback already.
	if d.UserDelivered {
		hd.User = d.User
	}
	// Per field, not per blob. An /init can carry a workdir with an empty user — a USER step
	// whose argument expands to nothing is stored unvalidated — and the two are delivered
	// independently, so dropping the workdir along with the user would lose it permanently:
	// no later memory resume re-sends a workdir, and the next blob would faithfully carry
	// the absence forward. has_workdir and SetData already treat them separately.
	if d.Workdir != nil {
		hd.Workdir = *d.Workdir
		hd.HasWorkdir = true
	}
	if hd.GetUser() == "" && !hd.GetHasWorkdir() {
		return nil
	}

	return hd
}

// ApplyHandoverDefaults restores the carried exec context onto the incoming envd's
// defaults, and reports which fields it applied.
//
// Only ever widens: a blob with no record (an outgoing envd predating the field)
// leaves the compiled-in fallback in place, which is the behaviour that existed
// before this field. A later /init carrying a non-empty user still overrides it --
// the host owns this state and the blob is a backstop, not an authority.
//
// Per field, matching the writer: a blob can carry a workdir and no user, and applying
// neither because one is absent is how a delivered workdir gets lost for good.
func ApplyHandoverDefaults(st *upgrade.HandoverState, d *execcontext.Defaults) (user, workdir bool) {
	hd := st.GetDefaults()
	if hd == nil || d == nil {
		return false, false
	}
	if u := hd.GetUser(); u != "" {
		d.User = u
		d.UserDelivered = true
		user = true
	}
	if hd.GetHasWorkdir() {
		w := hd.GetWorkdir()
		d.Workdir = &w
		workdir = true
	}

	return user, workdir
}
