//go:build linux

package volumes

import (
	"os"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const (
	volumeType = "test-vt"
)

func setupTestService(tb testing.TB) (*Service, string, *orchestrator.VolumeInfo) {
	tb.Helper()

	if os.Geteuid() != 0 {
		tb.Skip("Test requires root privileges")
	}

	tmpDir := tb.TempDir()
	teamID := uuid.New()
	volumeID := uuid.New()

	config := cfg.Config{
		PersistentVolumeMounts: map[string]string{
			volumeType: tmpDir,
		},
	}

	builder := chrooted.NewBuilder(config)
	s := New(config, builder)

	volumeInfo := &orchestrator.VolumeInfo{
		VolumeType: volumeType,
		TeamId:     teamID.String(),
		VolumeId:   volumeID.String(),
	}

	rootPath, err := s.getVolumeRootPath(tb.Context(), volumeInfo)
	require.NoError(tb, err)

	err = os.MkdirAll(rootPath, 0o755)
	require.NoError(tb, err)

	return s, rootPath, volumeInfo
}

func assertDir(t *testing.T, fs *chrooted.Chrooted, path string, uid, gid uint32, mode os.FileMode) {
	t.Helper()

	info, err := fs.Stat(path)
	require.NoError(t, err)

	assert.Equal(t, mode.Perm(), info.Mode().Perm())

	osInfo, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.NotNil(t, osInfo)
	assert.Equal(t, uid, osInfo.Uid)
	assert.Equal(t, gid, osInfo.Gid)
}

func requireGRPCError(t *testing.T, err error, expectedGRPCCode codes.Code, expectedUserErrorCode orchestrator.UserErrorCode) {
	t.Helper()

	require.Error(t, err)

	status, ok := status.FromError(err)
	require.Truef(t, ok, "expected error to be a gRPC status error, got %T: %s", err, err.Error())

	require.Equalf(t, expectedGRPCCode, status.Code(), "expected %s, got %s", expectedGRPCCode, status.Code())

	for _, detail := range status.Details() {
		if userError, ok := detail.(*orchestrator.UserError); ok {
			require.Equalf(t, expectedUserErrorCode, userError.GetCode(), "expected %s, got %s", expectedUserErrorCode, userError.GetCode())

			return
		}
	}

	require.Fail(t, "expected UserError detail not found")
}
