//go:build linux

package server

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver"
	peerservermocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// SendMsg is allowed to retain the original response for lazy consumers after
// returning. The generated stream and production sender must honor that lifetime.
type retainedSeekableMessages struct {
	grpc.ServerStream

	messages []*orchestrator.ReadAtBuildSeekableResponse
}

func (s *retainedSeekableMessages) SendMsg(message any) error {
	response, ok := message.(*orchestrator.ReadAtBuildSeekableResponse)
	if !ok {
		return fmt.Errorf("unexpected seekable response %T", message)
	}

	s.messages = append(s.messages, response)

	return nil
}

func TestSeekableStreamPreservesRetainedMessages(t *testing.T) {
	t.Parallel()

	const blockSize = int64(4096)
	payload := make([]byte, 0, 4*blockSize)
	for _, value := range []byte{0x10, 0x21, 0x32, 0x43} {
		payload = append(payload, bytes.Repeat([]byte{value}, int(blockSize))...)
	}
	cache, err := block.NewCache(int64(len(payload)), blockSize, t.TempDir()+"/peer-cache", true)
	require.NoError(t, err)
	_, err = cache.WriteAt(payload, 0)
	require.NoError(t, err)
	diff, err := build.NewLocalDiffFromCache("peer-test", cache)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, diff.Close())
		}
	})

	templateCache := peerservermocks.NewMockCache(t)
	templateCache.EXPECT().LookupDiff("peer-test", build.DiffType(storage.MemfileName)).Return(diff, true)
	source, err := peerserver.ResolveSeekable(templateCache, "peer-test", storage.MemfileName)
	require.NoError(t, err)

	retained := &retainedSeekableMessages{}
	stream := &grpc.GenericServerStream[orchestrator.ReadAtBuildSeekableRequest, orchestrator.ReadAtBuildSeekableResponse]{
		ServerStream: retained,
	}
	sender := &seekableStreamSender{stream: stream}
	require.NoError(t, source.Stream(t.Context(), blockSize, 2*blockSize+blockSize/2, sender))
	require.NoError(t, diff.Close())
	closed = true

	// Only inspect messages after all reads, sends and the mmap close. A sender
	// that eagerly copies would hide the next read overwriting a sent message.
	require.Len(t, retained.messages, 3)
	for i, size := range []int64{blockSize, blockSize, blockSize / 2} {
		start := blockSize + int64(i)*blockSize
		require.Equal(t, payload[start:start+size], retained.messages[i].GetData(), "retained message %d changed", i)
	}
}
