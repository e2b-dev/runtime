//go:build linux

package block

import (
	"context"
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

// TestChunker_FetchSpanNamesBuildFileTypeAndSource verifies that the chunk.fetch
// span names the build and file type the chunker reads, so that together with
// the offset it identifies the fetched bytes, and the tier that served them.
func TestChunker_FetchSpanNamesBuildFileTypeAndSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		objType storage.SeekableObjectType
		source  storage.Source
	}{
		{storage.MemfileObjectType, storage.SourceGCS},
		{storage.RootFSObjectType, storage.SourceNFS},
	}
	for _, tt := range tests {
		t.Run(tt.objType.String(), func(t *testing.T) {
			t.Parallel()

			buildID := uuid.NewString()
			data := makeTestData(testFileSize)
			chunker, err := NewChunker(&featureflags.Client{}, int64(len(data)), testBlockSize, t.TempDir()+"/cache", newTestMetrics(t), tt.objType, buildID)
			require.NoError(t, err)
			defer chunker.Close()

			upstream := sourceOpener{RangeOpener: &fakeSeekable{data: data}, source: tt.source}
			_, err = chunker.Slice(t.Context(), 0, testBlockSize, upstream, nil)
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
			assert.Equal(t, tt.objType.String(), attrs[storage.AttrFileType].AsString())
			assert.Equal(t, tt.source.String(), attrs[storage.AttrSource].AsString())
			assert.Equal(t, int64(0), attrs["off"].AsInt64())
			assert.Positive(t, attrs["len"].AsInt64())
		})
	}
}

// TestChunker_FetchSpanNamesSourceWhenReadPanics verifies that a fetch whose
// reader panics after the open still names the tier on its span.
func TestChunker_FetchSpanNamesSourceWhenReadPanics(t *testing.T) {
	t.Parallel()

	buildID := uuid.NewString()
	data := makeTestData(testFileSize)
	chunker, err := NewChunker(&featureflags.Client{}, int64(len(data)), testBlockSize, t.TempDir()+"/cache", newTestMetrics(t), storage.MemfileObjectType, buildID)
	require.NoError(t, err)
	defer chunker.Close()

	upstream := sourceOpener{RangeOpener: &panicSeekable{data: data}, source: storage.SourceGCS}
	_, err = chunker.Slice(t.Context(), 0, testBlockSize, upstream, nil)
	require.Error(t, err)

	require.Eventually(t, func() bool { return findFetchSpan(buildID) != nil }, 5*time.Second, 10*time.Millisecond)
	span := findFetchSpan(buildID)
	require.NotNil(t, span)

	var source string
	for _, kv := range span.Attributes() {
		if kv.Key == storage.AttrSource {
			source = kv.Value.AsString()
		}
	}
	assert.Equal(t, storage.SourceGCS.String(), source)
}

// TestChunker_FetchSpanNamesStoredLen verifies that the chunk.fetch span names
// the bytes the chunk occupies in storage: the compressed frame, or the chunk
// itself when the data is stored uncompressed. A peer read without a compressed
// table cannot know the stored size, so its span leaves it out.
func TestChunker_FetchSpanNamesStoredLen(t *testing.T) {
	t.Parallel()

	data := makeTestData(testFileSize)
	ft, compressed := makeCompressedTestData(t, data)
	frame, err := ft.LocateCompressed(0)
	require.NoError(t, err)

	tests := []struct {
		name     string
		upstream storage.RangeOpener
		ft       *storage.FrameTable
		want     func(chunkLen int64) (int64, bool)
	}{
		{"compressed", compressed, ft, func(int64) (int64, bool) { return int64(frame.Length), true }},
		{"compressed from peer", sourceOpener{RangeOpener: compressed, source: storage.SourcePeer}, ft, func(int64) (int64, bool) { return int64(frame.Length), true }},
		{"uncompressed", &fakeSeekable{data: data}, nil, func(chunkLen int64) (int64, bool) { return chunkLen, true }},
		{"uncompressed from peer", sourceOpener{RangeOpener: &fakeSeekable{data: data}, source: storage.SourcePeer}, nil, func(int64) (int64, bool) { return 0, false }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buildID := uuid.NewString()
			chunker, err := NewChunker(&featureflags.Client{}, int64(len(data)), testBlockSize, t.TempDir()+"/cache", newTestMetrics(t), storage.MemfileObjectType, buildID)
			require.NoError(t, err)
			defer chunker.Close()

			_, err = chunker.Slice(t.Context(), 0, testBlockSize, tt.upstream, tt.ft)
			require.NoError(t, err)

			require.Eventually(t, func() bool { return findFetchSpan(buildID) != nil }, 5*time.Second, 10*time.Millisecond)
			span := findFetchSpan(buildID)
			require.NotNil(t, span)

			attrs := make(map[attribute.Key]attribute.Value)
			for _, kv := range span.Attributes() {
				attrs[kv.Key] = kv.Value
			}
			want, ok := tt.want(attrs["len"].AsInt64())
			got, present := attrs["stored_len"]
			require.Equal(t, ok, present, "stored_len present")
			if ok {
				assert.Equal(t, want, got.AsInt64())
			}
		})
	}
}

// sourceOpener reports a chosen source for reads served by the wrapped opener.
type sourceOpener struct {
	storage.RangeOpener

	source storage.Source
}

func (o sourceOpener) OpenRangeReader(ctx context.Context, off, length int64, ft *storage.FrameTable) (storage.RangeReader, storage.Source, error) {
	r, _, err := o.RangeOpener.OpenRangeReader(ctx, off, length, ft)

	return r, o.source, err
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
