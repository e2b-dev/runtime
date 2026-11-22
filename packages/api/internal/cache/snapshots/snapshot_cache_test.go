package snapshotcache

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// An older replica writes entries without the format mark, under the same key. The
// cache must refetch those from the database instead of serving them, serve entries
// that carry the mark, and keep invalidating the one shared key.
func TestSnapshotCacheFormatMark(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	c := NewSnapshotCache(db.SqlcClient, redisClient)
	key := func(id string) string { return snapshotCacheKeyPrefix + ":" + id }
	seed := func(t *testing.T, id string, info SnapshotInfo) {
		t.Helper()
		data, err := json.Marshal(info) //nolint:musttag // Snapshot rows use their Go field names in the shared cache format.
		require.NoError(t, err)
		require.NoError(t, redisClient.Set(t.Context(), key(id), data, time.Minute).Err())
	}

	t.Run("an entry without the mark is refetched and rewritten", func(t *testing.T) {
		t.Parallel()

		// The database has no snapshot for it, so a refetch answers not found.
		seed(t, "old-format", SnapshotInfo{Snapshot: queries.Snapshot{SandboxID: "old-format"}})
		_, err := c.Get(t.Context(), "old-format")
		require.ErrorIs(t, err, ErrSnapshotNotFound)

		data, err := redisClient.Get(t.Context(), key("old-format")).Bytes()
		require.NoError(t, err)
		var stored SnapshotInfo
		require.NoError(t, json.Unmarshal(data, &stored)) //nolint:musttag // Snapshot rows use their Go field names in the shared cache format.
		assert.Equal(t, SnapshotInfoFormat, stored.Format)
		assert.True(t, stored.NotFound)
	})

	t.Run("an entry with the mark is served", func(t *testing.T) {
		t.Parallel()

		seed(t, "current", SnapshotInfo{Snapshot: queries.Snapshot{SandboxID: "current"}, Format: SnapshotInfoFormat})
		info, err := c.Get(t.Context(), "current")
		require.NoError(t, err)
		assert.Equal(t, "current", info.Snapshot.SandboxID)
	})

	t.Run("invalidation deletes the shared key", func(t *testing.T) {
		t.Parallel()

		seed(t, "invalidated", SnapshotInfo{Snapshot: queries.Snapshot{SandboxID: "invalidated"}, Format: SnapshotInfoFormat})
		c.Invalidate(t.Context(), "invalidated")
		assert.Equal(t, int64(0), redisClient.Exists(t.Context(), key("invalidated")).Val())
		assert.Equal(t, "snapshot:last:v2", snapshotCacheKeyPrefix, "the key stays shared with older replicas")
	})
}
