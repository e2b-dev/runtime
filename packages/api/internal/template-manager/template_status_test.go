package template_manager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// grpcDeadlineExceeded is a status-RPC timeout as the API sees it: a gRPC
// status error, not context.DeadlineExceeded.
func grpcDeadlineExceeded() error {
	return grpcstatus.Error(codes.DeadlineExceeded, "context deadline exceeded")
}

// builderLookupError is a failed builder lookup as GetStatus wraps it — the
// chain produced when the instance flaps Unhealthy.
func builderLookupError() error {
	return fmt.Errorf("failed to get builder client: %w",
		fmt.Errorf("failed to get builder by id 'node-1': %w", clusters.ErrTemplateBuilderNotFound))
}

// TestPollBuildStatus_builderLookupErrorKeepsBuildAlive: a builder that flaps
// Unhealthy fails the lookup before any RPC is made; the build must ride it out
// like an RPC-level transient error.
func TestPollBuildStatus_builderLookupErrorKeepsBuildAlive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		script := make([]getStatusResult, 0, 61)
		for range 60 {
			script = append(script, getStatusResult{err: builderLookupError()})
		}
		script = append(script, getStatusResult{resp: completedStatus()})

		client := &scriptedClient{script: script}
		c := &PollBuildStatus{
			client:  client,
			logger:  logger.NewNopLogger(),
			buildID: uuid.New(),
		}

		ctx, cancel := context.WithTimeout(t.Context(), buildTimeout)
		defer cancel()

		c.poll(ctx)

		failedReasons, finished, calls := client.snapshot()
		require.Empty(t, failedReasons, "build was failed after %d status calls", calls)
		require.True(t, finished, "build was not finished after %d status calls", calls)
	})
}

type getStatusResult struct {
	resp *templatemanagergrpc.TemplateBuildStatusResponse
	err  error
}

// scriptedClient replays a canned sequence of GetStatus outcomes (the last one
// repeats forever) and records what the poller persisted for the build.
type scriptedClient struct {
	mu sync.Mutex

	script []getStatusResult
	calls  int

	failedReasons []string
	finished      bool
	deleted       bool
}

func (s *scriptedClient) GetStatus(context.Context, uuid.UUID, string, uuid.UUID, string) (*templatemanagergrpc.TemplateBuildStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := s.script[min(s.calls, len(s.script)-1)]
	s.calls++

	return result.resp, result.err
}

func (s *scriptedClient) SetTerminalStatus(_ context.Context, _ uuid.UUID, statusGroup types.BuildStatusGroup, reason *templatemanagergrpc.TemplateBuildStatusReason) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	recorded := len(s.failedReasons) == 0 && !s.finished
	if statusGroup == types.BuildStatusGroupFailed {
		s.failedReasons = append(s.failedReasons, reason.GetMessage())
	}

	return recorded, nil
}

func (s *scriptedClient) DeleteBuild(context.Context, uuid.UUID, string, uuid.UUID, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleted = true

	return nil
}

func (s *scriptedClient) SetFinished(context.Context, uuid.UUID, int64, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.finished = true

	return nil
}

func (s *scriptedClient) wasDeleted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleted
}

func (s *scriptedClient) snapshot() ([]string, bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.failedReasons...), s.finished, s.calls
}

func completedStatus() *templatemanagergrpc.TemplateBuildStatusResponse {
	return &templatemanagergrpc.TemplateBuildStatusResponse{
		Status: templatemanagergrpc.TemplateBuildState_Completed,
		Metadata: &templatemanagergrpc.TemplateBuildMetadata{
			RootfsSizeKey:  100,
			EnvdVersionKey: "1.0.0",
		},
	}
}

// TestPollBuildStatus_transientRPCErrorKeepsBuildAlive: a status
// RPC that times out is a backend hiccup, not a build failure, and must not
// terminate an otherwise healthy build.
func TestPollBuildStatus_transientRPCErrorKeepsBuildAlive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		script := make([]getStatusResult, 0, 61)
		for range 60 {
			script = append(script, getStatusResult{err: grpcDeadlineExceeded()})
		}
		script = append(script, getStatusResult{resp: completedStatus()})

		client := &scriptedClient{script: script}
		c := &PollBuildStatus{
			client:  client,
			logger:  logger.NewNopLogger(),
			buildID: uuid.New(),
		}

		ctx, cancel := context.WithTimeout(t.Context(), buildTimeout)
		defer cancel()

		c.poll(ctx)

		failedReasons, finished, calls := client.snapshot()
		require.Empty(t, failedReasons, "build was failed after %d status calls", calls)
		require.True(t, finished, "build was not finished after %d status calls", calls)
	})
}

// TestPollBuildStatus_transientRPCErrorEventuallyFailsBuild makes sure the
// tolerance above stays bounded: a builder that never answers again still fails
// the build, it just takes the grace period instead of a single hiccup.
func TestPollBuildStatus_transientRPCErrorEventuallyFailsBuild(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		client := &scriptedClient{script: []getStatusResult{{err: grpcDeadlineExceeded()}}}
		c := &PollBuildStatus{
			client:  client,
			logger:  logger.NewNopLogger(),
			buildID: uuid.New(),
		}

		ctx, cancel := context.WithTimeout(t.Context(), buildTimeout)
		defer cancel()

		start := time.Now()
		c.poll(ctx)
		elapsed := time.Since(start)

		failedReasons, _, _ := client.snapshot()
		require.Len(t, failedReasons, 1)
		assert.Contains(t, failedReasons[0], "DeadlineExceeded", "failure reason should keep the underlying error")
		assert.GreaterOrEqual(t, elapsed, transientErrorGracePeriod, "the poller should retry for at least the grace period")
		assert.Less(t, elapsed, buildTimeout, "build failed only once the build timed out")
		assert.False(t, client.wasDeleted(), "the build was cancelled on the node; only the build deadline does that")
	})
}

// flappingClient fails status lookups in two runs, each shorter than the grace
// period and separated by answered polls, then reports the build completed.
type flappingClient struct {
	scriptedClient

	start time.Time
}

func (f *flappingClient) GetStatus(context.Context, uuid.UUID, string, uuid.UUID, string) (*templatemanagergrpc.TemplateBuildStatusResponse, error) {
	run := transientErrorGracePeriod * 4 / 5

	switch elapsed := time.Since(f.start); {
	case elapsed < run:
		return nil, builderLookupError()
	case elapsed < run+3*time.Second:
		return &templatemanagergrpc.TemplateBuildStatusResponse{Status: templatemanagergrpc.TemplateBuildState_Building}, nil
	case elapsed < 2*run+3*time.Second:
		return nil, builderLookupError()
	default:
		return completedStatus(), nil
	}
}

// TestPollBuildStatus_answeredPollResetsTheGracePeriod: the grace period covers
// one unbroken run of transient errors. Two runs that together exceed it, split
// by an answered poll, must not fail the build.
func TestPollBuildStatus_answeredPollResetsTheGracePeriod(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		client := &flappingClient{start: time.Now()}
		c := &PollBuildStatus{
			client:  client,
			logger:  logger.NewNopLogger(),
			buildID: uuid.New(),
		}

		ctx, cancel := context.WithTimeout(t.Context(), buildTimeout)
		defer cancel()

		c.poll(ctx)

		failedReasons, finished, _ := client.snapshot()
		require.Empty(t, failedReasons)
		require.True(t, finished, "build was not finished")
	})
}

// TestPollBuildStatus_transientRunLogsOnce: a run of transient errors logs one
// warning when it starts and one line when polling recovers, however many ticks
// it spans.
func TestPollBuildStatus_transientRunLogsOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		script := make([]getStatusResult, 0, 61)
		for range 60 {
			script = append(script, getStatusResult{err: builderLookupError()})
		}
		script = append(script, getStatusResult{resp: completedStatus()})

		core, logs := observer.New(zapcore.DebugLevel)
		client := &scriptedClient{script: script}
		c := &PollBuildStatus{
			client:  client,
			logger:  logger.NewTracedLoggerFromCore(core),
			buildID: uuid.New(),
		}

		ctx, cancel := context.WithTimeout(t.Context(), buildTimeout)
		defer cancel()

		c.poll(ctx)

		assert.Equal(t, 1, logs.FilterMessage("Build status polling received a transient error, keeping the build alive").Len(), "transient warning log count")
		assert.Equal(t, 1, logs.FilterMessage("Build status polling recovered").Len(), "recovery log count")
	})
}

// TestPollBuildStatus_terminalRPCErrorFailsBuild: errors that will not fix
// themselves fail the build immediately, with no grace period.
func TestPollBuildStatus_terminalRPCErrorFailsBuild(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		client := &scriptedClient{script: []getStatusResult{
			{err: errors.New("error while getting build info, maybe already expired")},
		}}
		c := &PollBuildStatus{
			client:  client,
			logger:  logger.NewNopLogger(),
			buildID: uuid.New(),
		}

		ctx, cancel := context.WithTimeout(t.Context(), buildTimeout)
		defer cancel()

		start := time.Now()
		c.poll(ctx)
		elapsed := time.Since(start)

		failedReasons, _, _ := client.snapshot()
		require.Len(t, failedReasons, 1)
		assert.Contains(t, failedReasons[0], "polling received unrecoverable error")
		assert.Less(t, elapsed, transientErrorGracePeriod, "terminal error took too long to fail the build")
	})
}

// TestPollBuildStatus_terminalErrorInMixedRetryBatchFailsBuild: a terminal
// error must fail the build even when later attempts in the same retry batch
// fail transiently — the batch is classified by the terminal error, not by
// whichever error lands on the last attempt.
func TestPollBuildStatus_terminalErrorInMixedRetryBatchFailsBuild(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		script := []getStatusResult{
			{err: grpcstatus.Error(codes.NotFound, "no such build")},
			{err: builderLookupError()},
		}

		client := &scriptedClient{script: script}
		c := &PollBuildStatus{
			client:  client,
			logger:  logger.NewNopLogger(),
			buildID: uuid.New(),
		}

		ctx, cancel := context.WithTimeout(t.Context(), buildTimeout)
		defer cancel()

		start := time.Now()
		c.poll(ctx)
		elapsed := time.Since(start)

		failedReasons, _, calls := client.snapshot()
		require.Len(t, failedReasons, 1, "after %d status calls", calls)
		assert.Contains(t, failedReasons[0], "polling received unrecoverable error")
		assert.Contains(t, failedReasons[0], "no such build", "failure reason should keep the terminal error")
		assert.Less(t, elapsed, transientErrorGracePeriod, "terminal error took too long to fail the build")
	})
}

func TestIsTransientStatusError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "grpc deadline exceeded", err: grpcDeadlineExceeded(), want: true},
		{name: "wrapped grpc deadline exceeded", err: fmt.Errorf("polling: %w", grpcDeadlineExceeded()), want: true},
		{name: "grpc unavailable", err: grpcstatus.Error(codes.Unavailable, "connection refused"), want: true},
		{name: "grpc resource exhausted", err: grpcstatus.Error(codes.ResourceExhausted, "too many builds"), want: true},
		{name: "bare context deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "wrapped context deadline exceeded", err: fmt.Errorf("polling: %w", context.DeadlineExceeded), want: true},
		{name: "builder lookup while instance unhealthy", err: builderLookupError(), want: true},
		{name: "grpc not found", err: grpcstatus.Error(codes.NotFound, "no such build"), want: false},
		{name: "grpc internal", err: grpcstatus.Error(codes.Internal, "boom"), want: false},
		{name: "plain error", err: errors.New("build info expired"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isTransientStatusError(tt.err))
		})
	}
}
