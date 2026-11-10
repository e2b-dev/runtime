//go:build unix && !linux

package handler

import "errors"

// pidfdOpen is unsupported off Linux. envd only runs the live-upgrade reaper in
// the (Linux) guest; this stub keeps the package compiling on non-Linux dev
// machines (the reaper falls back to Wait4 when it errors).
func pidfdOpen(int) (int, error) {
	return -1, errors.New("pidfd_open is only supported on linux")
}
