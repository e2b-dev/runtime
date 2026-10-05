package userfaultfd

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

	u.recordServeStats(pageClassNew, faultResultDeferred, 0)
	u.recordServeStats(pageClassNew, faultResultInstalled, 4096)
	st := u.ServeStats()
	assert.Equal(t, int64(1), st.Deferred)
	assert.Equal(t, int64(1), st.Pages)
	assert.Equal(t, int64(4096), st.Bytes)
}
