//go:build unix

package process

import (
	"os"
	"testing"

	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

// skipUnlessCredentialSwitchAllowed: handler.New sets SysProcAttr.Credential
// to switch uid/gid, which requires root.
func skipUnlessCredentialSwitchAllowed(t *testing.T) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("skipping: requires root (handler.New sets SysProcAttr.Credential)")
	}
}

func echoCommand() *rpc.ProcessConfig {
	return &rpc.ProcessConfig{Cmd: "echo", Args: []string{"hello"}}
}

func endlessOutputCommand() *rpc.ProcessConfig { return &rpc.ProcessConfig{Cmd: "yes"} }

// stdinEchoCommand copies stdin to stdout until EOF.
func stdinEchoCommand() *rpc.ProcessConfig { return &rpc.ProcessConfig{Cmd: "cat"} }

// grandchildHoldsStdoutCommand prints once and waits on a grandchild that
// holds stdout and stderr.
func grandchildHoldsStdoutCommand() *rpc.ProcessConfig {
	return &rpc.ProcessConfig{Cmd: "/bin/sh", Args: []string{"-c", "sleep 60 & echo started; wait"}}
}
