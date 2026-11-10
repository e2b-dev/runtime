package featureflags

import (
	"context"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogWriteConfigResolverNilClient(t *testing.T) {
	t.Parallel()

	resolver := NewLogWriteConfigResolver(nil, collectorDefaults)
	got := resolver.Resolve(t.Context())

	assert.Equal(t, fallbackCollector, got.PrimaryURL)
	assert.Nil(t, got.ShadowURLs)
	// Legacy fallback leaves Timeout at 0 to rely on the HTTP client timeout.
	assert.Equal(t, time.Duration(0), got.Timeout)
	assert.Equal(t, int64(defaultMaxInflightShadowWrites), got.MaxInflightShadowWrites)
}

func TestLogWriteConfigResolverCachesUntilTTL(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	client, err := NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close(context.WithoutCancel(t.Context())))
	})

	source.Update(source.Flag(LogsWriteConfigFlag.Key()).ValueForAll(ldvalue.FromJSONMarshal(map[string]any{
		"mode":        LogsWriteModePrimaryOnly,
		"primary_url": "http://127.0.0.1:11111",
	})))

	// Long TTL so the change below is not observed until we force expiry.
	resolver := newLogWriteConfigResolverWithTTL(client, collectorDefaults, time.Hour)

	first := resolver.Resolve(t.Context())
	assert.Equal(t, "http://127.0.0.1:11111", first.PrimaryURL)

	// Change the flag: the resolver must keep returning the cached value.
	source.Update(source.Flag(LogsWriteConfigFlag.Key()).ValueForAll(ldvalue.FromJSONMarshal(map[string]any{
		"mode":        LogsWriteModePrimaryOnly,
		"primary_url": "http://127.0.0.1:22222",
	})))

	cached := resolver.Resolve(t.Context())
	assert.Equal(t, "http://127.0.0.1:11111", cached.PrimaryURL, "should serve cached config before TTL expiry")
}

func TestLogWriteConfigResolverRefreshesAfterTTL(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	client, err := NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close(context.WithoutCancel(t.Context())))
	})

	source.Update(source.Flag(LogsWriteConfigFlag.Key()).ValueForAll(ldvalue.FromJSONMarshal(map[string]any{
		"mode":        LogsWriteModePrimaryOnly,
		"primary_url": "http://127.0.0.1:11111",
	})))

	// Zero TTL forces a refresh on every Resolve, so a flag change is observed
	// deterministically without a real-time sleep.
	resolver := newLogWriteConfigResolverWithTTL(client, collectorDefaults, 0)

	first := resolver.Resolve(t.Context())
	assert.Equal(t, "http://127.0.0.1:11111", first.PrimaryURL)

	source.Update(source.Flag(LogsWriteConfigFlag.Key()).ValueForAll(ldvalue.FromJSONMarshal(map[string]any{
		"mode":        LogsWriteModePrimaryOnly,
		"primary_url": "http://127.0.0.1:22222",
	})))

	refreshed := resolver.Resolve(t.Context())
	assert.Equal(t, "http://127.0.0.1:22222", refreshed.PrimaryURL, "should refresh after TTL expiry")
}

func TestLogWriteConfigResolverDefaultTTL(t *testing.T) {
	t.Parallel()

	resolver := NewLogWriteConfigResolver(nil, collectorDefaults)
	assert.Equal(t, logWriteConfigCacheTTL, resolver.ttl)
}
