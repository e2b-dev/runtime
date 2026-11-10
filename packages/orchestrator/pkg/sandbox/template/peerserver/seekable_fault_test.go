//go:build linux

package peerserver

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
)

func newLocalSeekableSource(tb testing.TB, payload []byte, blockSize int64) (*seekableSource, string) {
	tb.Helper()

	path := tb.TempDir() + "/peer-cache"
	cache, err := block.NewCache(int64(len(payload)), blockSize, path, true)
	require.NoError(tb, err)
	_, err = cache.WriteAt(payload, 0)
	require.NoError(tb, err)
	diff, err := build.NewLocalDiffFromCache("peer-test", cache)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, diff.Close()) })

	return &seekableSource{diff: diff}, path
}

func TestSeekableSourceStreamsRealLocalDiff(t *testing.T) {
	t.Parallel()

	const blockSize = int64(4096)
	payload := bytes.Repeat([]byte{0x61, 0x72}, int(3*blockSize/2))
	source, _ := newLocalSeekableSource(t, payload, blockSize)
	sender := &collectSender{}
	require.NoError(t, source.Stream(t.Context(), blockSize, blockSize+blockSize/2, sender))
	require.Equal(t, payload[blockSize:2*blockSize+blockSize/2], sender.data)
	require.Equal(t, []int{int(blockSize), int(blockSize / 2)}, sender.sends)
}

// A subprocess preserves the process-killing signal when the mmap guard is
// removed. The cache, local Diff and streaming consumer are production code.
func TestSeekableSourceMmapFault(t *testing.T) {
	t.Parallel()

	if os.Getenv("E2B_TEST_PEER_MMAP_FAULT_CHILD") != "1" {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSeekableSourceMmapFault$", "-test.count=1")
		cmd.Env = append(os.Environ(), "E2B_TEST_PEER_MMAP_FAULT_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "peer mmap fault child failed: %s", output)

		return
	}

	const blockSize = int64(4096)
	source, path := newLocalSeekableSource(t, bytes.Repeat([]byte{0x42}, int(blockSize)), blockSize)
	require.NoError(t, os.Truncate(path, 0))
	sender := &collectSender{}
	err := source.Stream(t.Context(), 0, blockSize, sender)
	var fault *block.MemoryFaultError
	require.ErrorAs(t, err, &fault)
	require.Empty(t, sender.data, "a partially faulted read must not be sent")
	require.Empty(t, sender.sends)
}

type discardSeekableChunks struct{}

func (discardSeekableChunks) Send([]byte) error { return nil }

func BenchmarkSeekableSourceStream(b *testing.B) {
	const blockSize = int64(4096)
	const length = 2*blockSize + blockSize/2
	source, _ := newLocalSeekableSource(b, bytes.Repeat([]byte{0x42}, int(4*blockSize)), blockSize)
	b.SetBytes(length)
	b.ReportAllocs()
	for b.Loop() {
		require.NoError(b, source.Stream(b.Context(), blockSize, length, discardSeekableChunks{}))
	}
}
