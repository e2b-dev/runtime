package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/clean-nfs-cache/cleaner"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/cacheindex"
)

// A Redis endpoint that accepts and never answers must not hold the run past
// indexConnectTimeout, even when the URL asks for a longer read timeout.
//
//nolint:paralleltest // sets REDIS_URL (t.Setenv) and the package-level indexConnectTimeout
func TestOpenIndexClientGivesUpOnAStalledRedis(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
		}
	}()
	t.Setenv("REDIS_URL", "redis://"+ln.Addr().String()+"/0?read_timeout=60s")
	old := indexConnectTimeout
	indexConnectTimeout = 300 * time.Millisecond
	t.Cleanup(func() { indexConnectTimeout = old })

	start := time.Now()
	client := openIndexClient(t.Context(), logger.NewNopLogger(), cleaner.Options{IndexMode: cleaner.IndexOn, MaxConcurrentDelete: 1})

	assert.Nil(t, client)
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestIndexSettingsFromTheFlag(t *testing.T) {
	t.Parallel()
	flag := func(mode string) ldvalue.ValueMap {
		b := ldvalue.ValueMapBuild()
		if mode != "" {
			b.Set("indexMode", ldvalue.String(mode))
		}

		return b.Build()
	}

	mode, ok := indexModeSetting(flag("index"))
	assert.True(t, ok)
	assert.Equal(t, cleaner.IndexOn, mode)

	_, ok = indexModeSetting(flag("shadow"))
	assert.False(t, ok, "shadow was removed")

	mode, ok = indexModeSetting(flag("Index"))
	assert.False(t, ok, "an unknown mode is reported so the run samples instead of failing")
	assert.Equal(t, "Index", mode)

	mode, ok = indexModeSetting(flag(""))
	assert.True(t, ok)
	assert.Empty(t, mode, "absent: keep the command-line value")
}

// With Redis configured, a real run creates the marker even with the index off
// and nothing to delete, so writers can fill the index before it is used.
func TestRunCreatesTheMarkerWhenRedisIsConfigured(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		hasRedis   bool
		dryRun     bool
		wantMarker bool
	}{
		{name: "redis configured", hasRedis: true, wantMarker: true},
		{name: "no redis", hasRedis: false},
		{name: "dry run", hasRedis: true, dryRun: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := newCacheRoot(t)
			opts := runOptions(root)
			opts.DryRun = tc.dryRun

			require.NoError(t, run(t.Context(), opts, logger.NewNopLogger(), cleaner.NoopMetrics(), nil, tc.hasRedis))

			id, err := cacheindex.ReadID(root)
			if tc.wantMarker {
				require.NoError(t, err)
				assert.NotEmpty(t, id)
			} else {
				assert.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestRunKeepsAMalformedMarker(t *testing.T) {
	t.Parallel()
	root := newCacheRoot(t)
	marker := filepath.Join(root, cacheindex.MarkerName)
	require.NoError(t, os.WriteFile(marker, []byte("not-a-uuid\n"), 0o644))

	require.NoError(t, run(t.Context(), runOptions(root), logger.NewNopLogger(), cleaner.NoopMetrics(), nil, true))

	b, err := os.ReadFile(marker)
	require.NoError(t, err)
	assert.Equal(t, "not-a-uuid\n", string(b))
}

// A run that only came for the marker must not fail, or write it, on a path
// the cleaner refuses as a chunks-cache root (subdirectories, none of them builds).
func TestRunSkipsTheMarkerOutsideACache(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "not-a-build"), 0o755))

	require.NoError(t, run(t.Context(), runOptions(root), logger.NewNopLogger(), cleaner.NoopMetrics(), nil, true))

	_, err := os.Stat(filepath.Join(root, cacheindex.MarkerName))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

//nolint:paralleltest // sets REDIS_URL and REDIS_CLUSTER_URL (t.Setenv)
func TestRedisConfigured(t *testing.T) {
	t.Setenv("REDIS_URL", "")
	t.Setenv("REDIS_CLUSTER_URL", "")
	assert.False(t, redisConfigured())

	t.Setenv("REDIS_CLUSTER_URL", "redis-cluster:6379")
	assert.True(t, redisConfigured())

	t.Setenv("REDIS_CLUSTER_URL", "")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")
	assert.True(t, redisConfigured())
}

// newCacheRoot returns a directory the cleaner accepts as a chunks-cache root.
func newCacheRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, uuid.NewString(), storage.MemfileName), 0o755))

	return root
}

// runOptions targets a disk percentage the run never computes, so there is
// nothing to delete.
func runOptions(root string) cleaner.Options {
	return cleaner.Options{
		Path:                   root,
		TargetDiskUsagePercent: 90,
		SampleMinFiles:         1,
		SampleMaxFiles:         1,
		MaxConcurrentScan:      1,
		MaxConcurrentStat:      1,
		MaxConcurrentDelete:    1,
		IndexMode:              cleaner.IndexOff,
	}
}
