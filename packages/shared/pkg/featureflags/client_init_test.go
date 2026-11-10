package featureflags

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	ldclient "github.com/launchdarkly/go-server-sdk/v7"
	"github.com/launchdarkly/go-server-sdk/v7/ldcomponents"
	"github.com/launchdarkly/go-server-sdk/v7/subsystems"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// gatedSource is an offline data source that holds its start, and reports
// itself uninitialized, until release is closed: LaunchDarkly not answering
// in time. Released, it serves data or, with none, ends its start
// uninitialized, as the SDK's own sources do for a rejected key.
type gatedSource struct {
	data    *ldtestdata.TestDataSource
	release chan struct{}
	inner   subsystems.DataSource
	closed  atomic.Bool
}

func (g *gatedSource) Build(ctx subsystems.ClientContext) (subsystems.DataSource, error) {
	var err error
	if g.data != nil {
		g.inner, err = g.data.Build(ctx)
	}

	return g, err
}

func (g *gatedSource) Start(ready chan<- struct{}) {
	go func() {
		<-g.release
		if g.inner != nil {
			g.inner.Start(ready)
		} else {
			close(ready)
		}
	}()
}

func (g *gatedSource) IsInitialized() bool {
	select {
	case <-g.release:
		return g.inner != nil
	default:
		return false
	}
}

func (g *gatedSource) Close() error {
	g.closed.Store(true)

	return nil
}

// offlineConfig sends nothing: no events, no diagnostics, no SDK log lines.
func offlineConfig(source *gatedSource) ldclient.Config {
	return ldclient.Config{DataSource: source, Events: ldcomponents.NoEvents(), DiagnosticOptOut: true, Logging: ldcomponents.NoLogging()}
}

func TestConnectFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		failed bool // the start fails at once; otherwise LaunchDarkly does not answer in time
		opts   []Option
	}{
		{name: "timeout fails by default"},
		{name: "permanent failure", failed: true},
		{name: "permanent failure when opted in", failed: true, opts: []Option{WithStartOnInitTimeout()}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := &gatedSource{release: make(chan struct{})}
			wantErr, wait := ldclient.ErrInitializationTimeout, 20*time.Millisecond
			if tt.failed {
				close(source.release)
				wantErr, wait = ldclient.ErrInitializationFailed, time.Minute
			} else {
				t.Cleanup(func() { close(source.release) })
			}

			client, err := connect("", offlineConfig(source), wait, "test-environment", "orchestrator", tt.opts...)
			require.ErrorIs(t, err, wantErr)
			assert.Nil(t, client)
			assert.True(t, source.closed.Load(), "the client the SDK handed back is closed")
		})
	}
}

// The reason to keep the client: it is live on fallbacks while the stream
// connects, then serves the flag without a restart.
//
//nolint:paralleltest // typed reads log through the global logger, which this test replaces
func TestConnectStartsOnFallbacksWhenOptedIn(t *testing.T) {
	data := ldtestdata.DataSource()
	data.Update(data.Flag("mode-flag").ValueForAll(ldvalue.String("permissive")))
	source := &gatedSource{data: data, release: make(chan struct{})}

	client, err := connect("", offlineConfig(source), 20*time.Millisecond, "test-environment", "orchestrator", WithStartOnInitTimeout())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })

	assert.True(t, client.Live(), "the stream is still connecting, so values can change")
	assert.False(t, client.ld.Initialized(), "the SDK reports the client not initialized until the stream connects")
	core, logs := observer.New(zap.DebugLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))
	assert.Equal(t, "fallback", client.StringFlag(t.Context(), StringFlag{name: "mode-flag", fallback: "fallback"}))
	assert.Empty(t, logs.All(), "a typed read while the stream connects logs nothing")
	reader := client.StringReader()
	assert.Equal(t, "env", reader.String(t.Context(), "mode-flag", "env"), "before the stream connects, the fallback")

	close(source.release)
	require.Eventually(t, func() bool {
		return reader.String(t.Context(), "mode-flag", "env") == "permissive"
	}, 5*time.Second, 10*time.Millisecond, "once the stream connects, the flag value, without a restart")
}
