package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

type createRetryReservations struct {
	sandbox.ReservationStorage

	err error
}

func (r createRetryReservations) Reserve(context.Context, uuid.UUID, string, int) (func(sandbox.Sandbox, error), func(context.Context) (sandbox.Sandbox, error), error) {
	return func(sandbox.Sandbox, error) {}, nil, r.err
}

type createRetryStorage struct {
	sandbox.Storage
}

func (createRetryStorage) Add(context.Context, sandbox.Sandbox) error {
	return errors.New("sandbox store unavailable")
}

type createRetryClient struct {
	orchestrator.SandboxServiceClient

	creates     int
	cleanupDone chan struct{}
}

func (c *createRetryClient) Create(context.Context, *orchestrator.SandboxCreateRequest, ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	c.creates++

	return &orchestrator.SandboxCreateResponse{}, nil
}

func (c *createRetryClient) Delete(context.Context, *orchestrator.SandboxDeleteRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	defer close(c.cleanupDone)

	return nil, errors.New("node unreachable during cleanup")
}

func TestCreateSandboxRetryBeforeAndAfterCreation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		reservationErr error
		wantCode       int
		wantRetry      apierrors.Retryability
		wantCreates    int
	}{
		{name: "concurrency limit", reservationErr: &sandbox.LimitExceededError{}, wantCode: http.StatusTooManyRequests, wantRetry: apierrors.CanRetry},
		{name: "reservation failure", reservationErr: errors.New("redis unavailable"), wantCode: http.StatusInternalServerError, wantRetry: apierrors.CanRetry},
		{name: "store failure after successful create", wantCode: http.StatusInternalServerError, wantRetry: apierrors.NoRetry, wantCreates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &createRetryClient{cleanupDone: make(chan struct{})}
			o := newCreateSandboxTestOrchestrator(t, func(node *nodemanager.TestNode) { node.SetSandboxClient(client) })
			o.sandboxStore = sandbox.NewStore(createRetryStorage{}, createRetryReservations{err: tc.reservationErr}, sandbox.Callbacks{})
			now := time.Now()
			_, apiErr := o.CreateSandbox(t.Context(), "sbx-retry", uuid.NewString(), testTeam(), successFetcher(), now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
			require.NotNil(t, apiErr)
			assert.Equal(t, tc.wantCode, apiErr.Code)
			assert.Equal(t, tc.wantRetry, apiErr.Retryable)
			assert.Equal(t, tc.wantCreates, client.creates)
			if tc.wantCreates > 0 {
				require.ErrorContains(t, apiErr.Err, "sandbox store unavailable")
				select {
				case <-client.cleanupDone:
				case <-time.After(5 * time.Second):
					t.Fatal("sandbox cleanup did not start")
				}
			}
		})
	}
}
