package cgroups

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ProcSelfCgroup is where a process learns its own cgroup.
const ProcSelfCgroup = "/proc/self/cgroup"

// livenessAllowlist names cgroups that must keep running across a pause, as paths
// relative to the cgroup2 root. Every name here is OURS, and each is justified by a code
// path in the resume, not by caution -- a list that grew with the customer population
// would defeat the point of walking the hierarchy in the first place.
//
//   - init.scope holds systemd (PID 1). Keeping it live is what lets envd's own
//     Restart=always fire if envd dies mid-resume, and PID 1 must never be frozen
//     regardless. (Volume mounts now carry nolock, so mount.nfs no longer needs
//     rpc.statd during setupNFS -- see the rpcbind note below.)
//   - systemd-journald.service drains the socket envd logs to (its unit has
//     Wants=systemd-journald.socket). Freeze it and once the socket buffer fills, envd's
//     own log writes block -- turning a slow resume into a wedged one.
//   - socats is envd's port forwarding. It is already excluded from the freeze today
//     (ProcessTypeSocat is absent from WorkloadProcessTypes); this preserves that.
//   - rpcbind.service holds the local portmapper, and rpcbind.socket is its activation
//     pair. Volume mounts now carry `nolock` (issue #3619), so mount.nfs no longer starts
//     rpc.statd and the resume mount path no longer depends on the local portmapper.
//     These entries are kept defensively: before nolock, a frozen rpcbind turned a v3
//     mount from a clean 3.2s error into a 25s hang (measured on a dev guest), so keeping
//     OUR own portmapper live across a pause is cheap insurance against any other resume
//     path that might come to rely on it. The socket unit holds no processes today, so
//     freezing it stops nothing; it rides along with its service for the same reason.
//
// Deliberately NOT here, so the list stays justified rather than merely cautious:
// run-rpc_pipefs.mount (a mount unit, 0 processes, so freezing its cgroup stops nothing)
// and systemd-resolved.service (1 process, but nothing in the resume path resolves a name
// -- the NFS target is an IP). Both are worth revisiting if a mount failure ever points
// at them; neither has a named code path today.
//
// envd's own ancestor chain is NOT here: the walk excludes it structurally, which is a
// stronger guarantee than a name match and cannot drift when the unit is renamed.
var livenessAllowlist = []string{
	"init.scope",
	"system.slice/systemd-journald.service",
	"system.slice/rpcbind.service",
	"system.slice/rpcbind.socket",
	"system.slice/rpc-statd.service",
	"socats",
}

// SelfCgroupPath returns envd's own cgroup as an absolute path under root, read from
// procFile (ProcSelfCgroup in production; a fixture in tests -- taking it as an argument
// rather than reading a package variable is what lets the tests run in parallel).
// On cgroup2 that file holds a single line of the form
//
//	0::/system.slice/envd.service
//
// The v1 lines that may accompany it carry a non-zero hierarchy ID and are ignored: this
// package refuses to start on anything but cgroup2 (NewCgroup2Manager statfs-checks it),
// so a missing 0:: line means the file is not what we think it is, which is worth an
// error rather than a guess at the root.
func SelfCgroupPath(procFile, root string) (string, error) {
	f, err := os.Open(procFile)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", procFile, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// hierarchy-ID:controller-list:cgroup-path
		parts := strings.SplitN(sc.Text(), ":", 3)
		if len(parts) == 3 && parts[0] == "0" {
			return filepath.Join(root, parts[2]), nil
		}
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("read %s: %w", procFile, err)
	}

	return "", fmt.Errorf("no cgroup2 (0::) entry in %s", procFile)
}

// AncestorChain returns root, then each directory between root and self, then self --
// the cgroups that must not be frozen because envd lives in them. Freezing any of them
// freezes envd, and a frozen envd cannot answer the /init that would thaw it.
//
// A self outside root yields just the root: that means we cannot locate ourselves in the
// tree we are about to walk, and the safe reading of that is "exclude nothing but the
// root", which leaves the caller to notice the chain is implausibly short.
func AncestorChain(root, self string) []string {
	root = filepath.Clean(root)
	self = filepath.Clean(self)

	rel, err := filepath.Rel(root, self)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return []string{root}
	}

	chain := []string{root}
	if rel == "." {
		return chain
	}

	cur := root
	for seg := range strings.SplitSeq(rel, string(filepath.Separator)) {
		cur = filepath.Join(cur, seg)
		chain = append(chain, cur)
	}

	return chain
}

// DescendSet returns every cgroup the walk must look INSIDE rather than freeze: envd's own
// ancestor chain, plus the ancestors of every allowlist entry.
//
// The second half is not an optimisation, it is what makes the allowlist mean anything.
// Freezing is hierarchical, so freezing a cgroup stops its whole subtree -- which means
// freezing the PARENT of an allowlisted cgroup stops it just as dead as freezing it
// directly, while passing the exact-path allowlist check. Concretely: with envd in the root
// cgroup, its chain is only the root, so `system.slice` is an ordinary child and gets
// frozen -- silently stopping `systemd-journald` and `rpcbind` underneath it, both
// allowlisted, and both of which the resume needs.
//
// Adding their ancestors here makes the walk descend into `system.slice` and freeze its
// non-allowlisted children individually instead. In the usual layout, where envd lives at
// system.slice/envd.service, this changes nothing: `system.slice` is already on the chain.
// It also future-proofs a deeper allowlist entry, where the intervening slice would
// otherwise defeat it the same way.
func DescendSet(root, self string) []string {
	seen := make(map[string]struct{})
	var order []string

	add := func(p string) {
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		order = append(order, p)
	}

	for _, c := range AncestorChain(root, self) {
		add(c)
	}
	for _, entry := range livenessAllowlist {
		// The entry itself is deliberately NOT added: it must be skipped, not descended
		// into. Only the path above it needs walking.
		full := filepath.Join(root, entry)
		for _, c := range AncestorChain(root, filepath.Dir(full)) {
			add(c)
		}
	}

	return order
}

// allowlisted reports whether an absolute cgroup path is one the resume depends on: an entry, or
// anything beneath one. Matching the subtree as well as the entry is what makes the exemption
// mean the same thing the freeze does -- freezing a cgroup freezes its descendants, so the sweep
// skips an allowlisted path without descending, and everything below it legitimately keeps
// running. An exact-path match would then report each of those children as an escape, i.e. report
// the exemption it was told to honour. Real trees have such children: a service with Delegate=,
// and a socket unit that activates into a sub-cgroup.
func allowlisted(root, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return false
	}

	for _, entry := range livenessAllowlist {
		if rel == entry || strings.HasPrefix(rel, entry+"/") {
			return true
		}
	}

	return false
}

// AuditResult is what the frozen state looked like at resume, before anything thawed it.
type AuditResult struct {
	// Applicable is false when the counts below mean nothing and must not be reported: the
	// last sweep this process performed was not the hierarchy one. Either it was the LEGACY
	// sweep, which freezes user and pty and nothing else -- scoring that against the walk's
	// expectation would report the rest of the guest as escaped on every resume, in the
	// default configuration -- or there was no sweep at all, which is a fresh create (every
	// workload cgroup legitimately running) or an envd that took over through a live upgrade
	// and cannot know what its predecessor did.
	//
	// Deliberately NOT conditioned on anything having been frozen: a hierarchy sweep whose
	// writes all failed is the case with the most escapes and the most need to be seen.
	Applicable bool
	// Visited is how many cgroups the audit examined.
	Visited int
	// Frozen is how many were EFFECTIVELY frozen -- stopped, whether because a freeze
	// wrote to them or because an ancestor was frozen. That is what the sweep achieved,
	// and it is deliberately not the same as the sweep's `requested` count: the walk writes
	// only the top of each subtree, so one write here shows up as a whole frozen subtree.
	Frozen int
	// Escaped is how many were NOT frozen even though they were the sweep's business --
	// not on envd's chain and not allowlisted. Each one ran through the snapshot, but the
	// count does NOT say why: a cgroup created after the sweep and one the sweep never
	// reached (truncated at the bound) or failed to write are indistinguishable from here.
	// The freeze result's Truncated and Failed are what separate them, so this must not be
	// reported as the creation race on its own.
	//
	// This is the residual the walk cannot close: a cgroup created after the sweep and
	// before the snapshot was never a candidate, and cgroup v2 offers no freeze-on-create
	// semantics to prevent it. Counting them turns "how often does that race actually
	// bite" from a guess into a number.
	Escaped int
	// Truncated is true when the bound stopped the walk rather than the tree running out,
	// so every count here is a floor and the unvisited remainder is unclassified. Reported
	// because an audit that quietly measured half the hierarchy reads exactly like one that
	// found nothing wrong -- the same reason the freeze and thaw report theirs.
	Truncated bool
	// ViolationPaths and EscapedPaths name the first few offenders, relative to the cgroup
	// root, so a non-zero count is actionable rather than merely alarming. Bounded: the counts
	// above are the totals, these are a sample.
	//
	// LOG ONLY. These are guest-chosen names and must never reach the response header --
	// putting attacker-controlled text in an HTTP header is a header-injection surface, and
	// the header exists to carry counts, which are not attacker-controlled.
	ViolationPaths []string
	EscapedPaths   []string
	// Violations is how many cgroups were frozen that must NOT have been -- allowlisted,
	// or on envd's own chain. Zero is the only acceptable value: a frozen allowlisted
	// cgroup means the resume is running without something it depends on.
	//
	// This is the arm that catches a bug rather than a race. Two have already been found
	// here by other means: an allowlist missing rpcbind, and an allowlisted cgroup frozen
	// transitively through an unwalked parent. Both would have shown up as a non-zero
	// count on the very first resume.
	Violations int
}

// auditPathSample bounds how many offender names an audit carries. A handful is enough to act
// on; the counts are the measurement.
const auditPathSample = 8

// samplePath appends path to a bounded sample, reusing the audit's bound. A handful of names
// is enough to reconstruct what a guest was tearing down, and the log exporter drops any line
// over 192 KiB outright -- so an unbounded list is the one shape that could make the line it
// belongs to disappear entirely.
func samplePath(into []string, path string) []string {
	if len(into) >= auditPathSample {
		return into
	}

	return append(into, path)
}

// sampleRel appends path relative to root, up to auditPathSample entries.
func sampleRel(into []string, root, path string) []string {
	if len(into) >= auditPathSample {
		return into
	}

	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return into
	}

	return append(into, rel)
}

// AuditFrozenSet runs AuditFrozenState against the sweep whose state is still in place, and
// discards the result if a thaw began while the walk was in flight.
//
// The walk is not atomic with respect to a thaw: it would see cgroups frozen ahead of the thaw
// and unfrozen behind it, and report the ones it reached later as escapes that never happened.
// The detection is a monotonic sweep generation rather than a re-read of the mode. The mode is a
// value, and comparing it before and after the walk is an ABA test: a thaw clears it and a fresh
// hierarchy freeze restores the same value, so a walk that spanned both passes an equality check
// while the tree changed underneath it. The generation advances on every freeze and every thaw,
// so a change is always visible. A partial count becomes no report, which is the right trade for
// an observer -- a wrong number here is worse than a missing one.
//
// The interleaving is reachable rather than theoretical: /init is retried, the in-place
// checkpoint thaws through POST /unfreeze itself before re-initialising envd, and on a
// checkpoint whose pause failed those two run concurrently, because the cleanup handlers run in
// reverse registration order and the envd re-init is launched before the unfreeze is sent.
//
// Holding the freeze lock across the walk would close the same window and is deliberately not
// done: it would make a thaw wait on an observer, and no observer may delay a thaw.
func (f *WorkloadFreezer) AuditFrozenSet(pm PathManager, procFile string, maxCgroups int) (AuditResult, error) {
	mode, gen := f.sweepState()

	res, err := AuditFrozenState(pm, procFile, maxCgroups, mode)
	if err != nil {
		return res, err
	}

	if _, now := f.sweepState(); now != gen {
		return AuditResult{}, nil
	}

	return res, nil
}

// AuditFrozenState classifies every cgroup's freeze state against what the sweep should
// have done. It MUST run before the thaw -- afterwards everything reads unfrozen and the
// audit says nothing.
//
// It reads cgroup.events (the SETTLED state) rather than cgroup.freeze, which is the
// opposite of what the thaw does, and the difference is the whole correctness argument.
// The thaw needs to know who to write 0 to, so it wants each cgroup's own request. The
// audit needs to know what is actually STOPPED, and the freeze is hierarchical: the walk
// writes only the top of each subtree, so every cgroup beneath it has cgroup.freeze=0 while
// being every bit as stopped. Reading the request here would call each of those an escape --
// hundreds of them on a container-runtime guest, which is exactly the layout this freeze
// exists to cover -- and would miss a violation whenever an allowlisted cgroup was stopped
// through its parent rather than directly. Attribution is not the question; being stopped is.
//
// Protection matches the WALK's notion, not just envd's chain: a cgroup in the descend set
// is one the walk deliberately does not freeze, so finding it unfrozen is correct rather
// than an escape.
func AuditFrozenState(pm PathManager, procFile string, maxCgroups int, sweep FreezeMode) (AuditResult, error) {
	var res AuditResult

	// Only a hierarchy sweep claims to have frozen everything outside envd's chain, so only
	// a hierarchy sweep can be audited for what it missed. An empty mode means this process
	// did not perform the freeze -- a live upgrade, most likely -- and an audit that cannot
	// be sure declines rather than guesses.
	if sweep != ModeHierarchy {
		return res, nil
	}

	root := pm.Root()
	self, err := SelfCgroupPath(procFile, root)
	if err != nil {
		return res, fmt.Errorf("locate envd cgroup for the audit: %w", err)
	}

	exempt := make(map[string]struct{})
	for _, c := range DescendSet(root, self) {
		exempt[c] = struct{}{}
	}

	queue := []string{root}
	for len(queue) > 0 {
		if res.Visited >= maxCgroups {
			res.Truncated = true

			break
		}

		cur := queue[0]
		queue = queue[1:]
		res.Visited++

		// Enumerate BEFORE reading state, so an unreadable cgroup costs its own
		// classification and not its whole subtree. A cgroup removed mid-walk is the
		// expected case here, and pruning everything below it would silently shrink the
		// audit exactly when the hierarchy is churning.
		children, e := pm.ChildrenOf(cur)
		if e == nil {
			queue = append(queue, children...)
		}

		// cgroup v2 creates neither cgroup.events nor cgroup.freeze on the mount root
		// ("this file exists on non-root cgroups"), so reading them there always fails.
		// The root is never frozen and never a candidate, so there is nothing to classify.
		if cur == root {
			continue
		}

		frozen, e := pm.FrozenAt(cur)
		if e != nil {
			// A cgroup that vanished mid-walk is a race, not a finding. Skipped silently:
			// this is an observer, and an observer that manufactures alarms is worse than
			// one with a small blind spot.
			continue
		}

		_, onChain := exempt[cur]
		protected := onChain || allowlisted(root, cur)
		switch {
		case frozen && protected:
			res.Violations++
			res.ViolationPaths = sampleRel(res.ViolationPaths, root, cur)
		case frozen:
			res.Frozen++
		case !protected:
			res.Escaped++
			res.EscapedPaths = sampleRel(res.EscapedPaths, root, cur)
		}
	}

	// Truncation alone makes the result reportable: "we could not see the whole hierarchy"
	// is a finding even when nothing in the part we did see was wrong.
	// Applicable once the mode guard is passed, unconditionally. Gating on "something was
	// frozen" predates that guard and is now not just redundant but harmful: it silenced the
	// audit exactly when it mattered most, because a hierarchy sweep whose writes all failed
	// -- or which truncated before writing anything -- reports zero frozen and the maximum
	// number of escapes. The guard already establishes that this process performed a
	// hierarchy sweep, which is the only thing "was this guest paused" needed to know.
	res.Applicable = true

	return res, nil
}

// ScanGuestFrozen records which cgroups the GUEST had frozen before our sweep ran, so the
// resume thaw can leave them alone. Without it the discovering thaw clears every
// cgroup.freeze it finds, which restarts processes the guest deliberately suspended --
// `docker pause` writes cgroup.freeze, so this is reachable from an ordinary container
// workflow.
//
// It walks the WHOLE hierarchy rather than the sweep's shallow set, because the thaw does:
// the sweep stops a subtree by freezing its top, while the thaw descends to every leaf. A
// guest freeze several levels down would otherwise be invisible here and cleared there.
//
// Cgroups that are OURS are never recorded, so they are always thawed: envd's own chain,
// the descend set above the allowlist, and the allowlist entries themselves. One found
// frozen is drift from an older envd, and leaving it that way would cost the resume its
// port forwarding or its journal.
//
// Errors are collected, never fatal. A cgroup that cannot be read is simply not recorded,
// which means the thaw will clear it -- the safe direction.
func ScanGuestFrozen(pm PathManager, procFile string, maxCgroups int) (map[string]struct{}, bool, []error) {
	var errs []error

	root := pm.Root()
	self, err := SelfCgroupPath(procFile, root)
	if err != nil {
		// Without our own path we cannot tell ours from the guest's. Record nothing, so
		// the thaw behaves as it did before this scan existed.
		return nil, false, append(errs, fmt.Errorf("locate envd cgroup for the guest-freeze scan: %w", err))
	}

	ours := make(map[string]struct{})
	for _, c := range DescendSet(root, self) {
		ours[c] = struct{}{}
	}

	found := make(map[string]struct{})
	visited := 0
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		if visited >= maxCgroups {
			// Truncated: the rest of the tree is unclassified, so the thaw will clear it.
			// Reported as a flag rather than an error, because unlike a truncated THAW
			// this degrades in the safe direction -- we thaw more than we meant to, never
			// less, so no guest is left frozen by it.
			return found, true, errs
		}
		visited++

		_, isOurs := ours[cur]
		if !isOurs && !allowlisted(root, cur) {
			requested, e := pm.FreezeRequestedAt(cur)
			switch {
			case vanished(e):
				// A cgroup that no longer exists froze nothing the thaw has to preserve.
				// Counted as a scan failure it would inflate ScanFailed, whose only
				// consumer warns that some cgroups could not be classified -- a warning
				// about a cgroup that no longer exists is one nobody can act on.
			case e != nil:
				errs = append(errs, fmt.Errorf("read freeze state of %s: %w", cur, e))
			case requested:
				found[cur] = struct{}{}
			}
		}

		children, e := pm.ChildrenOf(cur)
		if e != nil {
			if !vanished(e) {
				errs = append(errs, fmt.Errorf("list children of %s: %w", cur, e))
			}

			continue
		}
		queue = append(queue, children...)
	}

	return found, false, errs
}
