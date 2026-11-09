//go:build unix

package port

import "syscall"

// checkForwarding passes: socat is spawned per port, which the images ship.
func checkForwarding() error { return nil }

// socatProcAttr puts every socat in its own process group so a forward can be
// killed as a whole.
func socatProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid: true,
	}
}

// killSocat kills the socat's process group.
func killSocat(pid int) error {
	return syscall.Kill(-pid, syscall.SIGKILL)
}

// processAlive reports whether pid still exists (kill -0).
func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}
