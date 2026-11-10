package userfaultfd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestBalloonModeString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "unknown", BalloonModeUnknown.String())
	assert.Equal(t, "reporting", BalloonModeReporting.String())
	assert.Equal(t, "hinting", BalloonModeHinting.String())
	assert.Equal(t, "none", BalloonModeNone.String())
	assert.Equal(t, "unknown", BalloonMode(200).String(), "out of range reads unknown, never indexes past the table")
}

// A handler starts unknown and carries the mode it was given; deferred serve
// attempts are counted on their own, not as needed pages.
func TestServeStats_ModeAndDeferred(t *testing.T) {
	t.Parallel()
	u := &Userfaultfd{}
	assert.Equal(t, BalloonModeUnknown, u.balloonMode())
	u.SetBalloonMode(BalloonModeHinting)
	assert.Equal(t, BalloonModeHinting, u.balloonMode())

	u.recordServeStats(pageClassNew, faultResultDeferred, 0, time.Millisecond, storage.UnknownSource)
	u.recordServeStats(pageClassNew, faultResultInstalled, 4096, time.Millisecond, storage.UnknownSource)
	u.recordServeStats(pageClassNew, faultResultDeduped, 0, 0, storage.UnknownSource)
	st := u.ServeStats()
	assert.Equal(t, int64(1), st.Deferred)
	assert.Equal(t, int64(1), st.Deduped)
	assert.Equal(t, int64(1), st.Pages, "a deduped fault is not a served page")
	assert.Equal(t, int64(4096), st.Bytes)
}
