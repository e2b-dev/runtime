package sandbox

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func BenchmarkCachedPercent(b *testing.B) {
	for _, name := range []string{"local", "fragmented"} {
		b.Run(name, func(b *testing.B) {
			const blocks = 256
			const blockSize = 2 << 20
			const size = blocks * blockSize

			store, err := build.NewDiffStore(cfg.Config{}, nil, b.TempDir(), time.Hour, time.Minute)
			require.NoError(b, err)
			b.Cleanup(store.Close)
			local, missing := uuid.New(), uuid.New()
			// A residency probe must never read the backing bytes.
			diff, err := build.NewLocalDiffFromCache(build.GetDiffStoreKey(local.String(), build.Memfile), nil)
			require.NoError(b, err)
			store.Add(diff)

			segmentSize := size
			if name == "fragmented" {
				segmentSize = header.PageSize
			}
			mappings := make([]header.BuildMap, size/segmentSize)
			for i := range mappings {
				id := local
				if i%2 == 1 {
					id = missing
				}
				mappings[i] = header.BuildMap{
					Offset:             uint64(i * segmentSize),
					Length:             uint64(segmentSize),
					BuildId:            id,
					BuildStorageOffset: uint64(i * segmentSize),
				}
			}
			h, err := header.NewHeader(header.NewTemplateMetadata(uuid.New(), header.PageSize, size), mappings)
			require.NoError(b, err)
			f := build.NewFile(h, store, build.Memfile, nil, blockmetrics.Metrics{})
			indices := make([]uint64, blocks)
			for i := range indices {
				indices[i] = uint64(i)
			}
			mapping := &metadata.MemoryPrefetchMapping{Indices: indices, BlockSize: blockSize}
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				if cachedPercent(ctx, f, mapping) < 0 {
					b.Fatal("expected known residency")
				}
			}
		})
	}
}
