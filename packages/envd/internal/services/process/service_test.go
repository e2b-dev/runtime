package process

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/services/process/handler"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func newGetProcessTestService(t *testing.T) *Service {
	t.Helper()

	logger := zerolog.Nop()

	return newService(&logger, &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
	}, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()))
}

func tagSelector(tag string) *rpc.ProcessSelector {
	return &rpc.ProcessSelector{
		Selector: &rpc.ProcessSelector_Tag{Tag: tag},
	}
}

// TestGetProcessByTag_WithOtherTaggedProcesses guards against the
// regression where the Map.Range callback returned the wrong boolean and
// aborted the scan as soon as it hit a non-matching tagged process. Looking
// up every tag means at least one lookup is not satisfied by the first entry
// visited, whatever order the map iterates in.
func TestGetProcessByTag_WithOtherTaggedProcesses(t *testing.T) {
	t.Parallel()

	svc := newGetProcessTestService(t)

	tags := []string{"alpha", "beta", "gamma", "delta", "target"}
	for i, tag := range tags {
		svc.processes.Store(uint32(i+1), &handler.Handler{Tag: &tag})
	}

	for _, tag := range tags {
		proc, err := svc.getProcess(tagSelector(tag))
		require.NoError(t, err)
		require.NotNil(t, proc)
		require.NotNil(t, proc.Tag)
		assert.Equal(t, tag, *proc.Tag)
	}
}

// TestGetProcessByTag_SkipsUntaggedProcesses ensures processes without a
// tag never short-circuit the lookup of a tagged process.
func TestGetProcessByTag_SkipsUntaggedProcesses(t *testing.T) {
	t.Parallel()

	svc := newGetProcessTestService(t)

	svc.processes.Store(1, &handler.Handler{})
	svc.processes.Store(2, &handler.Handler{})
	wanted := "wanted"
	svc.processes.Store(3, &handler.Handler{Tag: &wanted})

	for range 200 {
		proc, err := svc.getProcess(tagSelector("wanted"))
		require.NoError(t, err)
		require.NotNil(t, proc)
		require.NotNil(t, proc.Tag)
		assert.Equal(t, "wanted", *proc.Tag)
	}
}

// TestGetProcessByTag_NotFound verifies a missing tag yields a NotFound
// error even when other tagged processes exist.
func TestGetProcessByTag_NotFound(t *testing.T) {
	t.Parallel()

	svc := newGetProcessTestService(t)

	existing := "existing"
	svc.processes.Store(1, &handler.Handler{Tag: &existing})

	proc, err := svc.getProcess(tagSelector("missing"))
	require.Error(t, err)
	assert.Nil(t, proc)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
