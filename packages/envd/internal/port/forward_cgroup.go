package port

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// socatCgroupPaths is the part of the cgroup manager that says where the socat cgroup is.
type socatCgroupPaths interface {
	Root() string
	PathOf(procType cgroups.ProcessType) (string, bool)
}

// socatCgroup is the cgroup envd spawns its socats into and the cgroup2 mount point it
// sits under, which a process's own cgroup file is relative to; ok is false where envd
// runs without cgroups.
func (f *Forwarder) socatCgroup() (root, path string, ok bool) {
	if f.freezer == nil {
		return "", "", false
	}
	paths, isPaths := f.freezer.Manager().(socatCgroupPaths)
	if !isPaths {
		return "", "", false
	}
	path, ok = paths.PathOf(cgroups.ProcessTypeSocat)

	return paths.Root(), path, ok
}

// cgroupProcs lists the live pids the cgroup at path reports. A pid may be reused by a
// process elsewhere once listed, so only inCgroup says one is still a member.
func cgroupProcs(path string) ([]int, error) {
	procs, err := os.ReadFile(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return nil, err
	}
	var pids []int
	for field := range strings.FieldsSeq(string(procs)) {
		pid, err := strconv.Atoi(field)
		if err == nil && pid > 0 && processAlive(pid) {
			pids = append(pids, pid)
		}
	}

	return pids, nil
}

// inCgroup reports whether pid, read under procRoot, lives in the cgroup at path.
func inCgroup(procRoot, root, path string, pid int) bool {
	at, err := cgroups.SelfCgroupPath(filepath.Join(procRoot, strconv.Itoa(pid), "cgroup"), root)

	return err == nil && at == path
}
