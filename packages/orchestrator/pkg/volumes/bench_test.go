//go:build linux

package volumes

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const (
	benchPackages = 64
	benchFileSize = 2048
)

// scriptedCreateFileStream replays a fixed CreateFile message sequence. The
// testify mock used by the tests reflects on every call, which would dominate
// a benchmark of the handler.
type scriptedCreateFileStream struct {
	grpc.ServerStream

	tb       testing.TB
	messages []*orchestrator.CreateFileRequest
	next     int
}

func (s *scriptedCreateFileStream) Recv() (*orchestrator.CreateFileRequest, error) {
	if s.next >= len(s.messages) {
		return nil, io.EOF
	}

	msg := s.messages[s.next]
	s.next++

	return msg, nil
}

func (s *scriptedCreateFileStream) SendAndClose(*orchestrator.CreateFileResponse) error {
	return nil
}

func (s *scriptedCreateFileStream) Context() context.Context {
	return s.tb.Context()
}

func newCreateFileStream(tb testing.TB, volume *orchestrator.VolumeInfo, path string, content []byte) *scriptedCreateFileStream {
	tb.Helper()

	return &scriptedCreateFileStream{
		tb: tb,
		messages: []*orchestrator.CreateFileRequest{
			{Message: &orchestrator.CreateFileRequest_Start{Start: &orchestrator.VolumeFileCreateStart{
				Volume: volume,
				Path:   path,
				Force:  true,
			}}},
			{Message: &orchestrator.CreateFileRequest_Content{Content: &orchestrator.VolumeFileCreateContent{
				Content: content,
			}}},
			{Message: &orchestrator.CreateFileRequest_Finish{Finish: &orchestrator.VolumeFileCreateFinish{}}},
		},
	}
}

func benchFilePath(writer string, i int) string {
	return fmt.Sprintf("%s/node_modules/pkg-%d/lib/index-%d.js", writer, i%benchPackages, i)
}

// BenchmarkService_CreateFile uploads a node_modules-shaped tree of small
// files through the CreateFile handler, the way a client syncing a build
// output or a checkout into a volume does.
func BenchmarkService_CreateFile(b *testing.B) {
	content := make([]byte, benchFileSize)

	b.Run("sequential", func(b *testing.B) {
		s, _, volume := setupTestService(b)

		b.ResetTimer()
		for i := range b.N {
			stream := newCreateFileStream(b, volume, benchFilePath("w0", i), content)
			require.NoError(b, s.CreateFile(stream))
		}
	})

	b.Run("parallel", func(b *testing.B) {
		s, _, volume := setupTestService(b)

		var writers atomic.Int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			writer := fmt.Sprintf("w%d", writers.Add(1))
			for i := 0; pb.Next(); i++ {
				stream := newCreateFileStream(b, volume, benchFilePath(writer, i), content)
				require.NoError(b, s.CreateFile(stream))
			}
		})
	})
}

// BenchmarkService_StatPath stats existing files, the cheapest request the
// service serves and so the one where per-request setup shows most.
func BenchmarkService_StatPath(b *testing.B) {
	s, _, volume := setupTestService(b)

	content := make([]byte, benchFileSize)
	for i := range benchPackages {
		stream := newCreateFileStream(b, volume, benchFilePath("w0", i), content)
		require.NoError(b, s.CreateFile(stream))
	}

	b.ResetTimer()
	for i := range b.N {
		_, err := s.StatPath(b.Context(), &orchestrator.StatPathRequest{
			Volume: volume,
			Path:   benchFilePath("w0", i%benchPackages),
		})
		require.NoError(b, err)
	}
}
