//go:build linux

package sandboxtools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

const (
	systemdRuntimeDir = "/run/systemd/system"
	initSettleTimeout = 30 * time.Second
)

// settleScript polls systemd, when it is PID 1, until it leaves the
// initializing and starting states or the limit runs out, and prints the last
// state. Polling rather than --wait also works on systemd older than 240 and
// needs no timeout(1). It exits 0 in every case and prints nothing without systemd.
func settleScript(runtimeDir string, limit time.Duration) string {
	return fmt.Sprintf(`if [ -d %q ] && command -v systemctl >/dev/null 2>&1; then
  i=0
  state=$(systemctl is-system-running 2>/dev/null)
  while { [ "$state" = initializing ] || [ "$state" = starting ]; } && [ "$i" -lt %d ]; do
    sleep 1
    i=$((i + 1))
    state=$(systemctl is-system-running 2>/dev/null)
  done
  echo "$state"
fi
exit 0`, runtimeDir, int(limit.Seconds()))
}

// StillBooting reports whether a state returned by WaitForInitToSettle means
// the wait gave up before systemd finished booting.
func StillBooting(state string) bool {
	state = strings.TrimSpace(state)

	return state == "initializing" || state == "starting"
}

// WaitForInitToSettle lets the guest's init system finish booting. It returns
// systemd's last state, empty for a guest without systemd.
func WaitForInitToSettle(ctx context.Context, proxy *proxy.SandboxProxy, sandboxID string) (state string, err error) {
	ctx, cancel := context.WithTimeout(ctx, initSettleTimeout+15*time.Second)
	defer cancel()

	var out strings.Builder
	err = RunCommandWithOutput(ctx, proxy, sandboxID, settleScript(systemdRuntimeDir, initSettleTimeout), metadata.Context{User: "root"},
		func(stdout, _ string) { out.WriteString(stdout) })

	return strings.TrimSpace(out.String()), err
}
