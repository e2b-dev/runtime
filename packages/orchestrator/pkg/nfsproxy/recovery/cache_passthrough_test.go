package recovery

import (
	iofs "io/fs"
	"testing"

	"github.com/stretchr/testify/require"
	nfs "github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/logged"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/tracing"
)

type cachingHandlerStub struct {
	nfs.Handler

	verifier         uint64
	contents         []iofs.FileInfo
	verifierPath     string
	verifierContents []iofs.FileInfo
	dataPath         string
	dataVerifier     uint64
}

func (h *cachingHandlerStub) VerifierFor(path string, contents []iofs.FileInfo) uint64 {
	h.verifierPath = path
	h.verifierContents = contents

	return h.verifier
}

func (h *cachingHandlerStub) DataForVerifier(path string, verifier uint64) []iofs.FileInfo {
	h.dataPath = path
	h.dataVerifier = verifier

	return h.contents
}

func TestCachingHandlerPassthroughThroughMiddlewareChain(t *testing.T) {
	t.Parallel()

	contents := []iofs.FileInfo{nil}
	inner := &cachingHandlerStub{verifier: 42, contents: contents}
	config := cfg.Config{}

	var handler nfs.Handler = inner
	handler = tracing.WrapWithTracing(handler, config)
	handler = metrics.WrapWithMetrics(handler, config)
	handler = logged.WrapWithLogging(t.Context(), handler, config)
	handler = WrapWithRecovery(t.Context(), handler)

	cachingHandler, ok := handler.(nfs.CachingHandler)
	require.True(t, ok)
	require.Equal(t, uint64(42), cachingHandler.VerifierFor("/dir", contents))
	require.Equal(t, contents, cachingHandler.DataForVerifier("/dir", 42))
	require.Equal(t, "/dir", inner.verifierPath)
	require.Equal(t, contents, inner.verifierContents)
	require.Equal(t, "/dir", inner.dataPath)
	require.Equal(t, uint64(42), inner.dataVerifier)
}
