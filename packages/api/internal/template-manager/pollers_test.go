package template_manager

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestPollerGroup_CloseLeavesAnInFlightBuildInProgress(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var group pollerGroup
		pollCtx, done, ok := group.start(t.Context())
		require.True(t, ok)

		inFlight := make(chan struct{})
		var entered sync.Once
		client := &recordingClient{
			// What the status RPC answers once shutdown has closed the builder
			// connection under it.
			getStatusErr: grpcstatus.Error(codes.Canceled, "grpc: the client connection is closing"),
			onGetStatus: func() {
				entered.Do(func() { close(inFlight) })
				select {
				case <-pollCtx.Done():
				case <-time.After(time.Minute):
				}
			},
		}

		returned := make(chan struct{})
		go func() {
			defer done()
			defer close(returned)
			newChecker(client).poll(pollCtx)
		}()

		<-inFlight
		require.NoError(t, group.close(t.Context()))

		select {
		case <-returned:
		default:
			require.Fail(t, "close returned while the poller was still running")
		}

		statusWrites, finishedWrites := client.writes()
		assert.Empty(t, statusWrites, "a build outliving its poller must not be recorded as failed")
		assert.Empty(t, finishedWrites)
		assert.Empty(t, client.deletes())
	})
}

func TestTemplateManager_NoBuildStatusSyncAfterClose(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// No database: a sync that got past Close would panic on it.
		tm := &TemplateManager{}

		periodical := make(chan struct{})
		go func() {
			defer close(periodical)
			tm.BuildsStatusPeriodicalSync(t.Context())
		}()

		require.NoError(t, tm.Close(t.Context()))
		<-periodical

		require.NoError(t, tm.BuildStatusSync(t.Context(), uuid.New(), "tmpl-1", uuid.New(), nil))
	})
}
