//go:build linux

package block

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// TestChunker_FetchSpanNamesBuildAndFileType verifies that the chunk.fetch span
// names the build and file type the chunker reads, so that together with the
// offset it identifies the fetched bytes.
func TestChunker_FetchSpanNamesBuildAndFileType(t *testing.T) {
	t.Parallel()

	for _, objType := range []storage.SeekableObjectType{storage.MemfileObjectType, storage.RootFSObjectType} {
		t.Run(objType.String(), func(t *testing.T) {
			t.Parallel()

			buildID := uuid.NewString()
			data := makeTestData(testFileSize)
			chunker, err := NewChunker(&featureflags.Client{}, int64(len(data)), testBlockSize, t.TempDir()+"/cache", newTestMetrics(t), objType, buildID)
			require.NoError(t, err)
			defer chunker.Close()

			_, err = chunker.Slice(t.Context(), 0, testBlockSize, &fakeSeekable{data: data}, nil)
			require.NoError(t, err)

			// Finding the span by build ID is the build.id check. The span ends on
			// the fetch goroutine, after the reader is released.
			require.Eventually(t, func() bool { return findFetchSpan(buildID) != nil }, 5*time.Second, 10*time.Millisecond)
			span := findFetchSpan(buildID)
			require.NotNil(t, span)

			attrs := make(map[attribute.Key]attribute.Value)
			for _, kv := range span.Attributes() {
				attrs[kv.Key] = kv.Value
			}
			assert.Equal(t, objType.String(), attrs[storage.AttrFileType].AsString())
			assert.Equal(t, int64(0), attrs["off"].AsInt64())
			assert.Positive(t, attrs["len"].AsInt64())
		})
	}
}

func findFetchSpan(buildID string) sdktrace.ReadOnlySpan {
	for _, s := range testSpanRecorder.Ended() {
		if s.Name() != "chunk.fetch" {
			continue
		}
		for _, kv := range s.Attributes() {
			if kv == telemetry.WithBuildID(buildID) {
				return s
			}
		}
	}

	return nil
}
