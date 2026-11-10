//go:build unix && !linux

package handler

import "syscall"

// applyCgroupFD is a no-op on non-Linux platforms; cgroup v2 is Linux-only.
func applyCgroupFD(_ *syscall.SysProcAttr, _ int, _ bool) {}
