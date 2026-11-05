//go:build linux

package service

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
)

// newContainer builds a container that keeps its status record under dir.
func newContainer(t *testing.T, dir string) *ServiceInfo {
	t.Helper()

	return NewInfoContainer(t.Context(), "node", "version", "commit", "instance", machineinfo.MachineInfo{}, cfg.Config{BuilderConfig: cfg.BuilderConfig{OrchestratorBaseDir: dir}})
}

func TestNewInfoContainer_RestoresPersistedStandby(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	record := filepath.Join(dir, servicestatus.FileName)
	t0 := time.Unix(1, 0)
	require.NoError(t, servicestatus.NewFileStore(record).Save(servicestatus.Snapshot{Status: standby, Owner: "x", ChangedAt: t0}))

	info := newContainer(t, dir)
	require.Equal(t, standby, info.Snapshot().Status)
	require.True(t, t0.Equal(info.Snapshot().ChangedAt))
	require.Equal(t, "x", info.Snapshot().Owner)

	require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: healthy, Owner: "x"}))
	require.NoFileExists(t, record)
	require.Equal(t, healthy, newContainer(t, dir).Snapshot().Status)
}

func TestNewInfoContainer_CorruptFileStartsHealthy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, servicestatus.FileName), []byte("not json"), 0o600))

	info := newContainer(t, dir)
	require.Equal(t, healthy, info.Snapshot().Status)
	require.Empty(t, info.Snapshot().Owner)
}
