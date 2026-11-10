//go:build linux

package commands

import (
	"context"
	"testing"
	"time"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// A RUN step's error reaches the orchestrator's own logs as well as the
// user's build log, so it must not repeat the command: the command is the
// user's and may carry credentials, and the build log already shows it on
// the step's line.
func TestRunFailureDoesNotRepeatTheCommand(t *testing.T) {
	t.Parallel()

	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, flags.Close(t.Context())) })
	// Never started: the step cannot reach a sandbox, so the command fails
	// to start, the same error shape as a command that cannot run.
	sandboxProxy, err := proxy.NewSandboxProxy(noop.MeterProvider{}, 0, sandbox.NewSandboxesMap(), flags)
	require.NoError(t, err)

	const command = `git clone https://x-access-token:secret-token@github.com/org/repo`
	step := &templatemanager.TemplateStep{Type: "RUN", Args: []string{command}}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = (&Run{}).Execute(ctx, logger.NewNopLogger(), zapcore.InfoLevel, sandboxProxy, "sandbox-id", "builder 1/1", step, metadata.Context{})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret-token")
	assert.NotContains(t, err.Error(), command)
	assert.Contains(t, err.Error(), "failed to run command")
}
