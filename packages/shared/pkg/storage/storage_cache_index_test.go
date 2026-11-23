package storage

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type fakeRecorder struct {
	mu     sync.Mutex
	builds []string
}

func (f *fakeRecorder) Touch(buildID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.builds = append(f.builds, buildID)
}

func (f *fakeRecorder) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.builds...)
}

func TestNewIndexToucher(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	build := uuid.NewString()

	assert.Nil(t, newIndexToucher(nil, build+"/memfile"))
	assert.Nil(t, newIndexToucher(rec, "not-a-build/memfile"))
	assert.Nil(t, newIndexToucher(rec, build))

	tc := newIndexToucher(rec, build+"/rootfs.ext4")
	require.NotNil(t, tc)
	assert.Equal(t, build, tc.buildID)

	var none *indexToucher
	none.touch() // nil-safe
}

func TestIndexToucherDedupes(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	build := uuid.NewString()
	tc := newIndexToucher(rec, build+"/memfile")
	require.NotNil(t, tc)

	tc.touch()
	tc.touch()
	assert.Equal(t, []string{build}, rec.got())

	tc.last.Add(-int64(indexTouchInterval))
	tc.touch()
	assert.Equal(t, []string{build, build}, rec.got())
}

func TestCachedSeekableTouchesOnFillAndHit(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	build := uuid.NewString()
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	inner := NewMockSeekable(t)
	inner.EXPECT().
		OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
		Return(NewRangeReader(io.NopCloser(bytes.NewReader(data))), SourceGCS, nil).Once()

	c := cachedSeekable{
		path:      t.TempDir(),
		chunkSize: 10,
		inner:     inner,
		tracer:    noopTracer,
		index:     newIndexToucher(rec, build+"/memfile"),
	}

	buff := make([]byte, 10)
	_, err := testReadAt(t.Context(), &c, buff, 0)
	require.NoError(t, err)
	c.wg.Wait()
	assert.Equal(t, []string{build}, rec.got(), "a fill is a use")

	c.index.last.Store(0)
	_, err = testReadAt(t.Context(), &c, buff, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{build, build}, rec.got(), "a hit is a use")
}

func TestCachedSeekableSkippedWritebackIsNotAUse(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	inner := NewMockSeekable(t)
	inner.EXPECT().
		OpenRangeReader(mock.Anything, mock.Anything, mock.Anything, (*FrameTable)(nil)).
		Return(NewRangeReader(io.NopCloser(bytes.NewReader(data))), SourceGCS, nil)

	c := cachedSeekable{
		path:      t.TempDir(),
		chunkSize: 10,
		inner:     inner,
		tracer:    noopTracer,
		index:     newIndexToucher(rec, uuid.NewString()+"/memfile"),
	}

	buff := make([]byte, 10)
	_, err := testReadAt(WithSkipCacheWriteback(t.Context()), &c, buff, 0)
	require.NoError(t, err)
	c.wg.Wait()
	assert.Empty(t, rec.got(), "a miss that does not fill the cache does not use it")
}
