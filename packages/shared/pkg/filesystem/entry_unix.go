//go:build unix

package filesystem

import (
	"syscall"
	"time"
)

func toTimestamp(spec syscall.Timespec) time.Time {
	if spec.Sec == 0 && spec.Nsec == 0 {
		return time.Time{}
	}

	return time.Unix(spec.Sec, spec.Nsec)
}

func getBase(sys any) *syscall.Stat_t {
	st, _ := sys.(*syscall.Stat_t)

	return st
}
