package cgroups

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// MemoryProtection is the memory protection configured on envd's own cgroup chain, as
// the cgroup2 interface files read it. It describes what was configured, which is a
// necessary condition for the kernel to grant it and not the grant itself: a sibling
// cgroup's claim on the same ancestor prorates envd's share without changing any of
// these files.
//
// The tags are the wire form of the X-Envd-Memory header. Carrying them here rather than
// on a copy leaves the envd side one declaration instead of two, so a field added to this
// struct ships. The orchestrator unmarshals into a type of its own, so making it read that
// field is a separate change there. Values stay in bytes as the cgroup files hold them;
// the orchestrator does any truncation.
type MemoryProtection struct {
	// Request is memory.min of envd's own cgroup.
	Request uint64 `json:"request"`
	// Low is memory.low of envd's own cgroup.
	Low uint64 `json:"low"`
	// Floor is the minimum of memory.min over every cgroup on the chain below the
	// root, envd's own included. Protection is granted top-down, so an ancestor
	// without a value leaves envd unprotected however large its own request is;
	// Floor is 0 in that case. A level holding "max" requests protection without a
	// bound and so leaves the floor to the rest of the chain.
	Floor uint64 `json:"floor"`
	// Partial is true when a value could not be read; that value is reported as 0.
	// A Floor of 0 with Partial false is a chain that is really unprotected.
	Partial bool `json:"partial"`
}

// ReadMemoryProtection walks envd's cgroup chain from the cgroup named in procFile
// (ProcSelfCgroup in production; a fixture in tests) up to root, reading each level's
// memory protection. It never fails: what cannot be read is reported as 0 with Partial
// set, because the result is a report and not an input to any decision.
//
// A cgroup that is the root itself has no memory.min by construction and is reported as
// all zeros with Partial false: that is a determinate "unprotected", not a read failure.
// A self outside root, or none at all, is a read failure.
func ReadMemoryProtection(procFile, root string) MemoryProtection {
	root = filepath.Clean(root)

	self, err := SelfCgroupPath(procFile, root)
	if err != nil {
		return MemoryProtection{Partial: true}
	}
	self = filepath.Clean(self)

	rel, err := filepath.Rel(root, self)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return MemoryProtection{Partial: true}
	}
	if rel == "." {
		return MemoryProtection{}
	}

	var p MemoryProtection

	// AncestorChain starts at root, which is excluded from the floor, and ends at self,
	// whose memory.min is also the request.
	levels := AncestorChain(root, self)[1:]
	for i, level := range levels {
		v, ok := readMemoryValue(filepath.Join(level, "memory.min"))
		if !ok {
			p.Partial = true
		}
		if i == 0 || v < p.Floor {
			p.Floor = v
		}
		if level == self {
			p.Request = v
		}
	}

	low, ok := readMemoryValue(filepath.Join(self, "memory.low"))
	if !ok {
		p.Partial = true
	}
	p.Low = low

	return p
}

// readMemoryValue reads one memory.{min,low} file. "max", which systemd writes for
// MemoryMin=infinity, is an unbounded request and reads as MaxInt64. Unbounded is the
// identity of the minimum the floor folds, so a level set that way leaves the floor to
// the rest of the chain; reading it as 0 would instead make it the annihilator and report
// the most protected chain there is as unprotected. MaxInt64 rather than MaxUint64
// because the orchestrator's decoder refuses anything above the int64 range it can carry
// as a span attribute, and refusing costs the whole report.
func readMemoryValue(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}

	s := strings.TrimSpace(string(b))
	if s == "max" {
		return math.MaxInt64, true
	}

	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}

	return v, true
}

// MemoryProtection reads the protection configured on envd's own cgroup chain, through
// the manager this freezer walks with. A manager that cannot address cgroups by path (the
// no-op manager, whatever the reason envd runs with it) yields a partial report of zeros:
// nothing could be read.
func (f *WorkloadFreezer) MemoryProtection() MemoryProtection {
	pm, ok := f.mgr.(PathManager)
	if !ok {
		return MemoryProtection{Partial: true}
	}

	return ReadMemoryProtection(f.procSelfCgroup, pm.Root())
}
