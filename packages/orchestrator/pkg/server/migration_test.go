//go:build linux

package server

import (
	"context"
	"testing"
	"time"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// A kill or a pause of a sandbox being migrated is refused retryably with the
// checkpoint-in-progress detail: the API keeps the sandbox record only on
// that answer, and the migrating VM keeps running.
func TestMigratingSandboxRefusesKillAndPauseRetryably(t *testing.T) {
	t.Parallel()

	for name, call := range map[string]func(*Server, string) error{
		"kill": func(s *Server, sandboxID string) error {
			_, err := s.Delete(t.Context(), &orchestrator.SandboxDeleteRequest{SandboxId: sandboxID})

			return err
		},
		"pause": func(s *Server, sandboxID string) error {
			_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sandboxID})

			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, sbx, _, _ := slowResumeServer(t)
			_, err := s.sandboxFactory.Sandboxes.BeginMigration(sbx.Runtime.SandboxID, sbx.LifecycleID)
			require.NoError(t, err)

			err = call(s, sbx.Runtime.SandboxID)
			assert.Equal(t, codes.ResourceExhausted, status.Code(err))
			require.ErrorContains(t, err, "is being migrated, please retry")
			assert.NotNil(t, checkpointInProgressDetail(t, err), "the API keeps the record only with the detail")
			_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
			assert.True(t, live, "the migrating sandbox stays live")
		})
	}
}

// A restore under a lifecycle already tracked is refused before anything is
// assigned to it.
func TestRestorePausedRefusesATrackedLifecycle(t *testing.T) {
	t.Parallel()

	s := duplicateCreateTestServer()
	sbx := drainTestSandbox(t, "lifecycle-restored")
	s.sandboxFactory.Sandboxes.TrackLifecycle(t.Context(), sbx)

	_, err := s.RestorePaused(t.Context(), &orchestrator.SandboxConfig{SandboxId: sbx.Runtime.SandboxID}, "build-1", sbx.LifecycleID, time.Time{}, time.Time{})
	require.ErrorIs(t, err, errLifecycleTracked)
}

// A retained capture runs only under a migration.
func TestCaptureRetainedRefusesWithoutMigration(t *testing.T) {
	t.Parallel()

	_, err := (&Server{}).CaptureRetained(t.Context(), drainTestSandbox(t, "lifecycle-1"), "build-1")
	require.ErrorIs(t, err, errCaptureNotMigrating)
}

// A restore runs Create's egress proxy gate: a configuration naming a proxy
// is refused on a build without the dialer before anything is taken.
func TestRestorePausedRefusesAnEgressProxyTheBuildLacks(t *testing.T) {
	t.Parallel()

	ff, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })
	sandboxMap := sandbox.NewSandboxesMap()
	s := &Server{
		sandboxFactory: sandbox.NewFactory(t.Context(), cfg.BuilderConfig{}, nil, nil, ff, nil, nil, network.NewNoopEgressProxy(), nil, sandboxMap),
		featureFlags:   ff,
		info:           &service.ServiceInfo{},
	}
	config := &orchestrator.SandboxConfig{
		SandboxId: "sandbox-1",
		Network:   &orchestrator.SandboxNetworkConfig{Egress: &orchestrator.SandboxNetworkEgressConfig{EgressProxyAddress: "proxy.example:3128"}},
	}

	_, err = s.RestorePaused(t.Context(), config, "build-1", "lifecycle-1", time.Time{}, time.Time{})
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}
