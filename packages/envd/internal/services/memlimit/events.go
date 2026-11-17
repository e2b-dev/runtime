//go:build linux

package memlimit

import "bytes"

// isMemoryOnlineEvent reports whether a kernel uevent says a memory block went online
// or offline, the two events that change MemTotal. A kernel uevent is
// "ACTION@DEVPATH" followed by NUL-separated KEY=VALUE fields.
func isMemoryOnlineEvent(msg []byte) bool {
	var memory, onlineChange bool
	for field := range bytes.SplitSeq(msg, []byte{0}) {
		switch string(field) {
		case "SUBSYSTEM=memory":
			memory = true
		case "ACTION=online", "ACTION=offline":
			onlineChange = true
		}
	}

	return memory && onlineChange
}
