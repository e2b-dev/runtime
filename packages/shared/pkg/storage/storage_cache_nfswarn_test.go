package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// The missing-cache-path warning must be emitted at most once per process, not
// on every build/launch/resume that hits the hot path.
//
//nolint:paralleltest // replaces the global logger and resets a process-wide guard
func TestWarnNFSCachePathUnsetOnce(t *testing.T) {
	// The guard is process-wide, so reset it to keep the test valid across -count runs.
	nfsCachePathUnsetWarnOnce = sync.Once{}
	t.Cleanup(func() { nfsCachePathUnsetWarnOnce = sync.Once{} })

	core, logs := observer.New(zap.WarnLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	for range 5 {
		WarnNFSCachePathUnsetOnce(t.Context())
	}

	assert.Equal(t, 1, logs.FilterMessageSnippet("NFS caching is disabled").Len())
}
