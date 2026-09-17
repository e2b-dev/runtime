package dummyserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const (
	testSandboxID   = "sandbox-1"
	testExecutionID = "execution-1"
)

func createTestSandbox(t *testing.T, server *SandboxServer) {
	t.Helper()

	_, err := server.Create(context.Background(), &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId:   testSandboxID,
			ExecutionId: testExecutionID,
		},
	})
	require.NoError(t, err)
}

func TestDeleteLegacyRequestWithoutExecutionID(t *testing.T) {
	server := NewSandbox()
	createTestSandbox(t, server)

	response, err := server.Delete(context.Background(), &orchestrator.SandboxDeleteRequest{
		SandboxId: testSandboxID,
	})

	require.NoError(t, err)
	require.False(t, response.GetStopCompleted())
}

func TestDeleteEvidenceRequestRequiresExactExecutionID(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		server := NewSandbox()
		createTestSandbox(t, server)

		_, err := server.Delete(context.Background(), &orchestrator.SandboxDeleteRequest{
			SandboxId:   testSandboxID,
			WaitForStop: true,
		})

		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("stale", func(t *testing.T) {
		server := NewSandbox()
		createTestSandbox(t, server)

		_, err := server.Delete(context.Background(), &orchestrator.SandboxDeleteRequest{
			SandboxId:   testSandboxID,
			ExecutionId: "stale-execution",
			WaitForStop: true,
		})

		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("exact", func(t *testing.T) {
		server := NewSandbox()
		createTestSandbox(t, server)

		response, err := server.Delete(context.Background(), &orchestrator.SandboxDeleteRequest{
			SandboxId:   testSandboxID,
			ExecutionId: testExecutionID,
			WaitForStop: true,
		})

		require.NoError(t, err)
		require.True(t, response.GetStopCompleted())
	})
}

func TestPauseLegacyRequestWithoutExecutionID(t *testing.T) {
	server := NewSandbox()
	createTestSandbox(t, server)

	_, err := server.Pause(context.Background(), &orchestrator.SandboxPauseRequest{
		SandboxId: testSandboxID,
	})

	require.NoError(t, err)
}

func TestPauseEvidenceRequestRequiresExactExecutionID(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		server := NewSandbox()
		createTestSandbox(t, server)

		_, err := server.Pause(context.Background(), &orchestrator.SandboxPauseRequest{
			SandboxId:      testSandboxID,
			WaitForStorage: true,
		})

		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("stale", func(t *testing.T) {
		server := NewSandbox()
		createTestSandbox(t, server)

		_, err := server.Pause(context.Background(), &orchestrator.SandboxPauseRequest{
			SandboxId:      testSandboxID,
			ExecutionId:    "stale-execution",
			WaitForStorage: true,
		})

		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}
