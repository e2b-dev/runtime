package redis_utils

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.segfaultmedaddy.com/redistest"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// EnvTestDatabaseURL names the environment variable carrying the redis:// URL
// of the Redis server the tests share.
const EnvTestDatabaseURL = "REDIS_TEST_DATABASE_URL"

// EnvTestClusterURL names the environment variable carrying the redis:// URL
// of a cluster-mode server. When it is set, the tests share that server
// instead, so a command whose keys span slots fails as it does on a cluster.
const EnvTestClusterURL = "REDIS_TEST_CLUSTER_URL"

// ClusterLane reports whether the tests share the server EnvTestClusterURL
// names.
func ClusterLane() bool {
	return os.Getenv(EnvTestClusterURL) != ""
}

// testOptions returns the options of the server the tests share.
func testOptions() *redis.UniversalOptions {
	url := os.Getenv(EnvTestDatabaseURL)
	if ClusterLane() {
		url = os.Getenv(EnvTestClusterURL)
	}

	opts := utils.Must(redis.ParseURL(url))

	return &redis.UniversalOptions{
		Addrs:         []string{opts.Addr},
		DB:            opts.DB,
		Username:      opts.Username,
		Password:      opts.Password,
		TLSConfig:     opts.TLSConfig,
		IsClusterMode: ClusterLane(),
	}
}

// testFactory is built by the first test that asks for a client, so a binary
// that never does needs no server.
var testFactory = sync.OnceValue(func() *redistest.RedisFactory {
	return utils.Must(redistest.NewFactory(redistest.WithOptions(testOptions())))
})

// newClusterAdmin returns a client on the cluster-mode server without a key
// namespace; the caller closes it.
func newClusterAdmin() *redis.ClusterClient {
	return redis.NewClusterClient(testOptions().Cluster())
}

// SetupInstance returns a client on the server the tests share whose keys are
// namespaced to the test, so parallel tests share the server without seeing
// each other's keys. The server is the one EnvTestClusterURL names when it is
// set, else the one EnvTestDatabaseURL names. The keys are removed and the
// client closed when the test ends.
//
// Commands that carry no key are sent unchanged and are not isolated: SCAN,
// KEYS, FLUSHDB and pub/sub. A test that depends on them uses
// SetupContainerInstance.
func SetupInstance(t *testing.T) redis.UniversalClient {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client, prefix := testFactory().Client(t)

	// The factory removes the test's keys with one UNLINK, which a cluster
	// rejects when they span slots. Registered after the factory's cleanup,
	// this one runs first and leaves it nothing to remove.
	if ClusterLane() {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 15*time.Second)
			defer cancel()

			assert.NoError(t, unlinkEach(ctx, prefix+"*"))
		})
	}

	return client
}

// unlinkEach removes the keys matching pattern from the cluster-mode server,
// one key per command.
func unlinkEach(ctx context.Context, pattern string) error {
	admin := newClusterAdmin()
	err := admin.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		iter := node.Scan(ctx, 0, pattern, 500).Iterator()
		for iter.Next(ctx) {
			if err := node.Unlink(ctx, iter.Val()).Err(); err != nil {
				return err
			}
		}

		return iter.Err()
	})

	return errors.Join(err, admin.Close())
}

// SetupContainerInstance starts a Redis container of the test's own, for tests
// whose commands SetupInstance cannot isolate.
func SetupContainerInstance(t *testing.T) redis.UniversalClient {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "redis:8-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := container.Terminate(context.WithoutCancel(t.Context()))
		assert.NoError(t, err)
	})

	host, err := container.Host(t.Context())
	require.NoError(t, err)

	port, err := container.MappedPort(t.Context(), "6379")
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr: host + ":" + port.Port(),
	})

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}
