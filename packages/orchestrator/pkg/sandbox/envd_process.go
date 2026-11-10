//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"connectrpc.com/connect"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
)

// StartEnvdShell opens a streaming Process.Start call against the sandbox's
// envd. timeout > 0 sets Connect-Timeout-Ms so envd kills the process at
// the deadline. Caller owns the returned stream.
func (s *Sandbox) StartEnvdShell(
	ctx context.Context,
	shell string,
	shellArgs []string,
	user string,
	timeout time.Duration,
) (*connect.ServerStreamForClient[process.StartResponse], error) {
	return s.startEnvdProcess(ctx, shell, shellArgs, user, timeout, nil)
}

// StartEnvdSystemShell runs the process in envd's root cgroup via the SystemTag.
func (s *Sandbox) StartEnvdSystemShell(
	ctx context.Context,
	shell string,
	shellArgs []string,
	user string,
	timeout time.Duration,
) (*connect.ServerStreamForClient[process.StartResponse], error) {
	tag := consts.SystemTag

	return s.startEnvdProcess(ctx, shell, shellArgs, user, timeout, &tag)
}

func (s *Sandbox) startEnvdProcess(
	ctx context.Context,
	shell string,
	shellArgs []string,
	user string,
	timeout time.Duration,
	tag *string,
) (*connect.ServerStreamForClient[process.StartResponse], error) {
	addr := fmt.Sprintf("http://%s:%d", s.Slot.HostIPString(), consts.DefaultEnvdServerPort)
	pc := NewEnvdProcessClient(addr, 0)

	req := connect.NewRequest(&process.StartRequest{
		Process: &process.ProcessConfig{Cmd: shell, Args: shellArgs},
		Tag:     tag,
	})
	if timeout > 0 {
		req.Header().Set("Connect-Timeout-Ms", strconv.FormatInt(timeout.Milliseconds(), 10))
	}
	if s.Config.Envd.AccessToken != nil {
		req.Header().Set("X-Access-Token", *s.Config.Envd.AccessToken)
	}
	grpc.SetUserHeader(req.Header(), user)

	return pc.Start(WithEnvdSandbox(ctx, s), req)
}
