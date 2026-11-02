package storage

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// The missing-cache-path warning must be emitted at most once per process, not
// on every build/launch/resume that hits the hot path.
func TestWarnNFSCachePathUnsetOnce(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	for i := 0; i < 5; i++ {
		WarnNFSCachePathUnsetOnce(t.Context())
	}

	got := logs.FilterMessageSnippet("NFS caching is disabled").Len()
	if got != 1 {
		t.Fatalf("want exactly 1 warning, got %d", got)
	}
}