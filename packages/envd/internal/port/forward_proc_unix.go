//go:build unix

package port

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

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

// isForwarderProcess reports whether pid, read under root, is a forwarding socat onto bind.
func isForwarderProcess(root, bind string, pid int) bool {
	cmdline, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "cmdline"))

	return err == nil && isLoopbackForwarder(strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00"), bind)
}

// isLoopbackForwarder reports whether args is the command line startPortForwarding builds:
// listen on bind, target localhost on the same port. It tells whether a re-adopted pid,
// known by number alone, still belongs to the socat it was handed over as.
func isLoopbackForwarder(args []string, bind string) bool {
	if len(args) == 0 || filepath.Base(args[0]) != "socat" {
		return false
	}
	port := ""
	for _, arg := range args[1:] {
		rest, ok := strings.CutPrefix(arg, "TCP4-LISTEN:")
		if !ok || !strings.Contains(rest+",", ",bind="+bind+",") {
			continue
		}
		port, _, _ = strings.Cut(rest, ",")
	}
	if port == "" {
		return false
	}
	for _, arg := range args[1:] {
		if arg == "TCP4:localhost:"+port || arg == "TCP6:localhost:"+port {
			return true
		}
	}

	return false
}
