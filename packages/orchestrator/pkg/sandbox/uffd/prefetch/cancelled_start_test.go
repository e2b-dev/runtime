//go:build linux

package prefetch

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type cancelledStartSource struct{}

func (*cancelledStartSource) BlockSize() int64 { return 4096 }

func (*cancelledStartSource) Slice(ctx context.Context, _, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return make([]byte, length), nil
}

type cancelledStartBackend struct {
	uffd.MemoryBackend

	ready chan struct{}
}

func (b *cancelledStartBackend) Ready() chan struct{} { return b.ready }

func (*cancelledStartBackend) Prefault(context.Context, int64, []byte) (bool, error) {
	return true, nil
}

// Measure cumulative allocation in an isolated copy of this test binary, so
// concurrently running package tests cannot charge their allocations to Start.
// Input mapping and fixtures are outside the measured interval. A generous
// budget distinguishes fixed run overhead from multi-MiB per-page work queues;
// it is not a performance or RSS threshold for an active prefetch run.
func TestStartCancelledDoesNotAllocatePageQueues(t *testing.T) {
	t.Parallel()

	const childEnv = "PREFETCH_CANCELLED_START_CHILD"
	if os.Getenv(childEnv) == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		childCtx, cancelChild := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancelChild()
		cmd := exec.CommandContext(childCtx, executable,
			"-test.run=^TestStartCancelledDoesNotAllocatePageQueues$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)

		return
	}

	data := ldtestdata.DataSource()
	data.Update(data.Flag(featureflags.MemoryPrefetchMaxFetchWorkers.Key()).ValueForAll(ldvalue.Int(16)))
	data.Update(data.Flag(featureflags.MemoryPrefetchMaxCopyWorkers.Key()).ValueForAll(ldvalue.Int(8)))
	data.Update(data.Flag(featureflags.MemoryPrefetchCoalesceMaxMB.Key()).ValueForAll(ldvalue.Int(0)))
	flags, err := featureflags.NewClientWithDatasource(data)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })

	const pages = 262144
	indices := make([]uint64, pages)
	for i := range indices {
		indices[i] = uint64(i)
	}
	mapping := &metadata.MemoryPrefetchMapping{Indices: indices, BlockSize: 4096}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for _, prefault := range []bool{false, true} {
		p := New(logger.NewNopLogger(), &cancelledStartSource{},
			&cancelledStartBackend{ready: make(chan struct{})}, mapping, flags)
		p.Prefault = prefault
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		err := p.Start(ctx)
		runtime.ReadMemStats(&after)
		require.NoError(t, err)
		assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20),
			"already-cancelled Start allocated per-page work queues (prefault=%t)", prefault)
	}
	runtime.KeepAlive(mapping)
}
