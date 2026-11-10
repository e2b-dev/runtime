package sandboxtypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRefusalEpisodeStart(t *testing.T) {
	t.Parallel()

	now := time.Now()

	assert.Equal(t, now, Sandbox{}.RefusalEpisodeStart(now), "never refused: the episode starts now")

	alive := Sandbox{RefusedSince: now.Add(-time.Minute), RefusedUntil: now.Add(-5 * time.Second)}
	assert.Equal(t, alive.RefusedSince, alive.RefusalEpisodeStart(now), "the window ended seconds ago: same episode")

	edge := Sandbox{RefusedSince: now.Add(-time.Minute), RefusedUntil: now.Add(-RefusalEpisodeGap)}
	assert.Equal(t, edge.RefusedSince, edge.RefusalEpisodeStart(now), "exactly at the gap: still the same episode")

	stale := Sandbox{RefusedSince: now.Add(-3 * time.Hour), RefusedUntil: now.Add(-3*time.Hour + 10*time.Second)}
	assert.Equal(t, now, stale.RefusalEpisodeStart(now), "a refusal hours after the window: a new episode")
}

func TestEnvdRefusalEpisodeStart(t *testing.T) {
	t.Parallel()

	now := time.Now()

	assert.Equal(t, now, Sandbox{}.EnvdRefusalEpisodeStart(now), "never refused by envd: the episode starts now")

	alive := Sandbox{EnvdRefusedSince: now.Add(-time.Minute), EnvdRefusedUntil: now.Add(-5 * time.Second)}
	assert.Equal(t, alive.EnvdRefusedSince, alive.EnvdRefusalEpisodeStart(now), "envd's window ended seconds ago: same episode")

	// Another refusal class kept the node's window fresh for minutes while
	// envd itself was last refused long ago: the guest agent's episode is over.
	heldByOthers := Sandbox{
		EnvdRefusedSince: now.Add(-10 * time.Minute), EnvdRefusedUntil: now.Add(-9 * time.Minute),
		RefusedSince: now.Add(-8 * time.Minute), RefusedUntil: now.Add(-2 * time.Second),
	}
	assert.Equal(t, now, heldByOthers.EnvdRefusalEpisodeStart(now), "other refusals do not keep envd's episode alive")

	nodeOnly := Sandbox{RefusedSince: now.Add(-time.Minute), RefusedUntil: now.Add(-5 * time.Second)}
	assert.Equal(t, now, nodeOnly.EnvdRefusalEpisodeStart(now), "a live node episode is not an envd episode")
}
