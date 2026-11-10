package redis_utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Slot values a Redis Cluster node returns for CLUSTER KEYSLOT.
func TestHashSlotKnownValues(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uint16(12182), HashSlot("foo"))
	assert.Equal(t, uint16(5061), HashSlot("bar"))
	assert.Equal(t, HashSlot("foo"), HashSlot("{foo}bar"))

	assert.Equal(t, "{}foo", HashTag("{}foo"), "an empty tag hashes the whole key")
	assert.Equal(t, "{foo", HashTag("{{foo}}"))
	assert.Equal(t, "foo", HashTag("{foo}bar"))
}

func TestAllSameSlot(t *testing.T) {
	t.Parallel()

	assert.True(t, AllSameSlot("{a}:x", "{a}:y"))
	assert.False(t, AllSameSlot("{a}:x", "{b}:x"))
	assert.True(t, AllSameSlot("{a}:x"))
}
