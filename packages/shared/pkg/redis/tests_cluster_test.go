package redis_utils

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func skipUnlessClusterLane(t *testing.T) {
	t.Helper()

	if testing.Short() || !ClusterLane() {
		t.Skip("needs the cluster-mode server " + EnvTestClusterURL + " names")
	}
}

// clusterAdmin returns a client on the cluster-mode server without a key
// namespace, closed when the test ends.
func clusterAdmin(t *testing.T) *redis.ClusterClient {
	t.Helper()

	admin := newClusterAdmin()
	t.Cleanup(func() { assert.NoError(t, admin.Close()) })

	return admin
}

func TestClusterLaneEnforcesSlots(t *testing.T) {
	t.Parallel()
	skipUnlessClusterLane(t)

	client := SetupInstance(t)

	require.ErrorContains(t, client.MGet(t.Context(), "{a}:1", "{b}:1").Err(), "CROSSSLOT")
	require.NoError(t, client.MGet(t.Context(), "{a}:1", "{a}:2").Err())
}

func TestClusterLaneRemovesKeysAcrossSlots(t *testing.T) {
	t.Parallel()
	skipUnlessClusterLane(t)

	admin := clusterAdmin(t)
	marker := strconv.FormatUint(rand.Uint64(), 36)
	// Registered before SetupInstance, so it runs after its cleanups; KEYS
	// goes to every primary, as the keys do.
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		assert.NoError(t, admin.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			left, err := node.Keys(ctx, "*"+marker).Result()
			assert.Empty(t, left)

			return err
		}))
	})

	client := SetupInstance(t)
	require.NoError(t, client.Set(t.Context(), "{a}:"+marker, "v", 0).Err())
	require.NoError(t, client.Set(t.Context(), "{b}:"+marker, "v", 0).Err())
}

func TestHashSlotMatchesServer(t *testing.T) {
	t.Parallel()
	skipUnlessClusterLane(t)

	admin := clusterAdmin(t)
	for i := range 200 {
		key := randomKey(i%2 == 0)

		slot, err := admin.ClusterKeySlot(t.Context(), key).Result()
		require.NoError(t, err)
		assert.Equal(t, uint16(slot), HashSlot(key), "key %q", key)
	}
}

// randomKey returns a key of random bytes, built around a "{tag}" when tagged.
func randomKey(tagged bool) string {
	part := func() string {
		b := make([]byte, rand.IntN(12))
		for i := range b {
			b[i] = byte(rand.Uint32())
		}

		return string(b)
	}

	if tagged {
		return part() + "{" + part() + "t}" + part()
	}

	return part() + "k"
}
