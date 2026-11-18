//go:build linux

package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func TestWaitReturnsErrorWhileBuildsInFlight(t *testing.T) {
	t.Parallel()

	s := &ServerStore{
		logger: logger.NewNopLogger(),
		wg:     &sync.WaitGroup{},
	}
	s.wg.Add(1)
	defer s.wg.Done()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, s.Wait(ctx), context.Canceled)
}

func TestWaitDeadlineWithQueuedBuild(t *testing.T) {
	t.Parallel()

	s, provider, req := newWorkTrackingServer(t, featureflags.DefaultFirecrackerVersion, "", 1)
	req.Template.BuildID = "running"
	started, release := blockTemplateCleanup(t, provider, req.Template.BuildID, nil)
	_, err := s.TemplateCreate(t.Context(), req)
	require.NoError(t, err)
	<-started

	queued := *req
	queuedConfig := *req.Template
	queuedConfig.BuildID = "queued"
	queued.Template = &queuedConfig
	queuedStarted := make(chan struct{})
	provider.EXPECT().DeleteObjectsWithPrefix(mock.Anything, queuedConfig.BuildID).
		RunAndReturn(func(context.Context, string) error {
			close(queuedStarted)
			return nil
		}).Once()
	_, err = s.TemplateCreate(t.Context(), &queued)
	require.NoError(t, err)
	require.Equal(t, int64(2), s.info.OutstandingWork())

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.Wait(ctx), context.DeadlineExceeded)
	require.Equal(t, int64(2), s.info.OutstandingWork())
	select {
	case <-queuedStarted:
		t.Fatal("queued build started before the running build released its slot")
	default:
	}

	release()
	s.wg.Wait()
	require.Zero(t, s.info.OutstandingWork())
	select {
	case <-queuedStarted:
	default:
		t.Fatal("accepted queued build did not run after shutdown wait timed out")
	}
}

func TestWaitStopsAtContextDeadlineDuringGracePeriod(t *testing.T) {
	t.Parallel()

	s := &ServerStore{
		logger: logger.NewNopLogger(),
		wg:     &sync.WaitGroup{},
	}

	// No builds in flight, so the wait passes straight to the consumer grace
	// period, which must be bounded by ctx rather than the full sleep.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, s.Wait(ctx), context.DeadlineExceeded)
}
