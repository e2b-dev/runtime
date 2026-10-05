package handlers

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	webhookevents "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/events"
	webhookmanagement "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management"
)

// The backend is another service, so these cover what this side owns: that the
// caller's team and cluster reach the contract, that a contract answer becomes
// the documented response, and that a backend failure is translated rather
// than passed through. What the backend itself decides is tested where it is
// implemented.
type fakeWebhooksBackend struct {
	webhookmanagement.UnimplementedWebhookManagementServiceServer
	webhookevents.UnimplementedSandboxEventsServiceServer

	mu sync.Mutex

	createRequests     []*webhookmanagement.CreateWebhookRequest
	listRequests       []*webhookmanagement.ListWebhooksRequest
	getRequests        []*webhookmanagement.GetWebhookRequest
	updateRequests     []*webhookmanagement.UpdateWebhookRequest
	deleteRequests     []*webhookmanagement.DeleteWebhookRequest
	deliveriesRequests []*webhookmanagement.ListWebhookDeliveriesRequest
	statsRequests      []*webhookmanagement.GetWebhookDeliveryStatsRequest
	teamEventsRequests []*webhookevents.ListTeamSandboxEventsRequest
	eventsRequests     []*webhookevents.ListSandboxEventsRequest

	webhook    *webhookmanagement.Webhook
	events     []*webhookevents.SandboxEvent
	deliveries []*webhookmanagement.WebhookDeliveryGroup

	// err, when set, is returned by every method.
	err error
}

func (f *fakeWebhooksBackend) record(mutate func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mutate()
}

func (f *fakeWebhooksBackend) CreateWebhook(
	_ context.Context, req *webhookmanagement.CreateWebhookRequest,
) (*webhookmanagement.CreateWebhookResponse, error) {
	f.record(func() { f.createRequests = append(f.createRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookmanagement.CreateWebhookResponse_builder{Webhook: f.webhook}.Build(), nil
}

func (f *fakeWebhooksBackend) ListWebhooks(
	_ context.Context, req *webhookmanagement.ListWebhooksRequest,
) (*webhookmanagement.ListWebhooksResponse, error) {
	f.record(func() { f.listRequests = append(f.listRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookmanagement.ListWebhooksResponse_builder{
		Webhooks: []*webhookmanagement.Webhook{f.webhook},
	}.Build(), nil
}

func (f *fakeWebhooksBackend) GetWebhook(
	_ context.Context, req *webhookmanagement.GetWebhookRequest,
) (*webhookmanagement.GetWebhookResponse, error) {
	f.record(func() { f.getRequests = append(f.getRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookmanagement.GetWebhookResponse_builder{Webhook: f.webhook}.Build(), nil
}

func (f *fakeWebhooksBackend) UpdateWebhook(
	_ context.Context, req *webhookmanagement.UpdateWebhookRequest,
) (*webhookmanagement.UpdateWebhookResponse, error) {
	f.record(func() { f.updateRequests = append(f.updateRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookmanagement.UpdateWebhookResponse_builder{Webhook: f.webhook}.Build(), nil
}

func (f *fakeWebhooksBackend) DeleteWebhook(
	_ context.Context, req *webhookmanagement.DeleteWebhookRequest,
) (*webhookmanagement.DeleteWebhookResponse, error) {
	f.record(func() { f.deleteRequests = append(f.deleteRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookmanagement.DeleteWebhookResponse_builder{}.Build(), nil
}

func (f *fakeWebhooksBackend) ListWebhookDeliveries(
	_ context.Context, req *webhookmanagement.ListWebhookDeliveriesRequest,
) (*webhookmanagement.ListWebhookDeliveriesResponse, error) {
	f.record(func() { f.deliveriesRequests = append(f.deliveriesRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookmanagement.ListWebhookDeliveriesResponse_builder{Groups: f.deliveries}.Build(), nil
}

func (f *fakeWebhooksBackend) GetWebhookDeliveryStats(
	_ context.Context, req *webhookmanagement.GetWebhookDeliveryStatsRequest,
) (*webhookmanagement.GetWebhookDeliveryStatsResponse, error) {
	f.record(func() { f.statsRequests = append(f.statsRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookmanagement.GetWebhookDeliveryStatsResponse_builder{
		Stats: webhookmanagement.WebhookDeliveryStats_builder{Total: 2, Failed: 1}.Build(),
	}.Build(), nil
}

func (f *fakeWebhooksBackend) ListTeamSandboxEvents(
	_ context.Context, req *webhookevents.ListTeamSandboxEventsRequest,
) (*webhookevents.ListTeamSandboxEventsResponse, error) {
	f.record(func() { f.teamEventsRequests = append(f.teamEventsRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookevents.ListTeamSandboxEventsResponse_builder{Events: f.events}.Build(), nil
}

func (f *fakeWebhooksBackend) ListSandboxEvents(
	_ context.Context, req *webhookevents.ListSandboxEventsRequest,
) (*webhookevents.ListSandboxEventsResponse, error) {
	f.record(func() { f.eventsRequests = append(f.eventsRequests, req) })
	if f.err != nil {
		return nil, f.err
	}

	return webhookevents.ListSandboxEventsResponse_builder{Events: f.events}.Build(), nil
}

// startWebhooksBackend serves the fake over an in-process connection and
// returns a store holding the same generated clients production uses.
func startWebhooksBackend(t *testing.T, backend *fakeWebhooksBackend) *APIStore {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	webhookmanagement.RegisterWebhookManagementServiceServer(server, backend)
	webhookevents.RegisterSandboxEventsServiceServer(server, backend)

	go func() { _ = server.Serve(listener) }()

	// The production options carry the call limits, so a page the backend is
	// allowed to send fails here too if they stop accommodating it.
	conn, err := grpc.NewClient(
		"passthrough:///webhooks-backend",
		append(webhooksDialOptions(), grpc.WithContextDialer(
			func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			},
		))...,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})

	return &APIStore{
		webhookManagement: webhookmanagement.NewWebhookManagementServiceClient(conn),
		sandboxEvents:     webhookevents.NewSandboxEventsServiceClient(conn),
	}
}

func newWebhooksRequest(t *testing.T, method, target, body string) (*gin.Context, *httptest.ResponseRecorder, uuid.UUID) {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	teamID := uuid.New()
	sharedauth.SetTeamInfoForTest(t, ginCtx, &types.Team{Team: &authqueries.Team{ID: teamID}})

	return ginCtx, recorder, teamID
}

func sampleWebhook(teamID uuid.UUID) *webhookmanagement.Webhook {
	return webhookmanagement.Webhook_builder{
		WebhookId: uuid.NewString(),
		TeamId:    teamID.String(),
		CreatedAt: timestamppb.New(time.Unix(1700000000, 0).UTC()),
		Name:      "alerts",
		Url:       "https://example.com/hook",
		Enabled:   true,
		Events:    []string{"sandbox.lifecycle.created"},
	}.Build()
}

func TestPostEventsWebhooksSendsTheCallersTeam(t *testing.T) {
	t.Parallel()

	backend := &fakeWebhooksBackend{}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, teamID := newWebhooksRequest(t, http.MethodPost, "/events/webhooks",
		`{"name":"alerts","url":"https://example.com/hook","events":["sandbox.lifecycle.created"]}`)
	backend.webhook = sampleWebhook(teamID)

	store.PostEventsWebhooks(ginCtx)

	require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
	require.Len(t, backend.createRequests, 1)

	// The team is never taken from the request body; it comes from the context
	// the authenticators populated.
	assert.Equal(t, teamID.String(), backend.createRequests[0].GetTeamId())
	assert.False(t, backend.createRequests[0].HasClusterId(), "a team with no cluster selects the local one")

	var created api.WebhookCreation
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &created))
	assert.Equal(t, "alerts", created.Name)
	assert.Equal(t, teamID.String(), created.TeamId)
}

func TestGetEventsWebhooksProjectsTheContractAnswer(t *testing.T) {
	t.Parallel()

	backend := &fakeWebhooksBackend{}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, teamID := newWebhooksRequest(t, http.MethodGet, "/events/webhooks", "")
	backend.webhook = sampleWebhook(teamID)

	store.GetEventsWebhooks(ginCtx)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Len(t, backend.listRequests, 1)
	assert.Equal(t, teamID.String(), backend.listRequests[0].GetTeamId())

	var webhooks []api.WebhookDetail
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &webhooks))
	require.Len(t, webhooks, 1)
	assert.Equal(t, "https://example.com/hook", webhooks[0].Url)
	assert.True(t, webhooks[0].Enabled)
}

func TestDeleteEventsWebhooksWebhookIDAnswersOK(t *testing.T) {
	t.Parallel()

	backend := &fakeWebhooksBackend{}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, teamID := newWebhooksRequest(t, http.MethodDelete, "/events/webhooks/x", "")
	webhookID := uuid.New()

	store.DeleteEventsWebhooksWebhookID(ginCtx, webhookID)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Len(t, backend.deleteRequests, 1)
	assert.Equal(t, teamID.String(), backend.deleteRequests[0].GetTeamId())
	assert.Equal(t, webhookID.String(), backend.deleteRequests[0].GetWebhookId())
}

// An omitted events list means "leave them alone" and an empty one means
// "reject this"; a repeated field cannot carry that difference, so the
// contract wraps it and the handler must preserve which was sent.
func TestPatchEventsWebhooksWebhookIDDistinguishesOmittedFromEmptyEvents(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		body       string
		hasEvents  bool
		wantValues []string
	}{
		{name: "omitted", body: `{"name":"renamed"}`, hasEvents: false},
		{name: "explicitly empty", body: `{"events":[]}`, hasEvents: true},
		{
			name: "provided", body: `{"events":["sandbox.lifecycle.paused"]}`, hasEvents: true,
			wantValues: []string{"sandbox.lifecycle.paused"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeWebhooksBackend{}
			store := startWebhooksBackend(t, backend)
			ginCtx, recorder, teamID := newWebhooksRequest(t, http.MethodPatch, "/events/webhooks/x", tc.body)
			backend.webhook = sampleWebhook(teamID)

			store.PatchEventsWebhooksWebhookID(ginCtx, uuid.New())

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Len(t, backend.updateRequests, 1)

			// The wrapper's presence is what carries the distinction: a
			// repeated field reads back as nil whether it was sent empty or
			// not sent at all, which is why the contract wraps it.
			sent := backend.updateRequests[0]
			assert.Equal(t, tc.hasEvents, sent.HasEvents())
			assert.Equal(t, tc.wantValues, sent.GetEvents().GetValues())
		})
	}
}

// A failure the caller can do nothing about must not describe itself to them.
func TestWebhookBackendFailuresMapToStatuses(t *testing.T) {
	t.Parallel()

	limit, err := status.New(codes.ResourceExhausted, "Webhook limit reached").WithDetails(
		webhookmanagement.WebhookManagementErrorDetail_builder{
			Reason: webhookmanagement.
				WebhookManagementErrorReason_WEBHOOK_MANAGEMENT_ERROR_REASON_WEBHOOK_LIMIT_REACHED,
		}.Build(),
	)
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		err      error
		wantCode int
		wantBody string
	}{
		{
			name: "not found", err: status.Error(codes.NotFound, "webhook not found"),
			wantCode: http.StatusNotFound, wantBody: "Webhook not found",
		},
		{
			name: "invalid argument keeps its message", err: status.Error(codes.InvalidArgument, "url: must be a valid http or https URL"),
			wantCode: http.StatusBadRequest, wantBody: "url: must be a valid http or https URL",
		},
		{
			name: "webhook limit", err: limit.Err(),
			wantCode: http.StatusTooManyRequests, wantBody: "Webhook limit reached",
		},
		{
			name: "exhaustion without the reason is not a limit", err: status.Error(codes.ResourceExhausted, "quota"),
			wantCode: http.StatusInternalServerError, wantBody: "Failed to create webhook",
		},
		{
			name: "deadline", err: status.Error(codes.DeadlineExceeded, "too slow"),
			wantCode: http.StatusGatewayTimeout, wantBody: "Failed to create webhook",
		},
		{
			name: "internal is not passed through", err: status.Error(codes.Internal, "pg: connection refused"),
			wantCode: http.StatusInternalServerError, wantBody: "Failed to create webhook",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeWebhooksBackend{err: tc.err}
			store := startWebhooksBackend(t, backend)
			ginCtx, recorder, _ := newWebhooksRequest(t, http.MethodPost, "/events/webhooks",
				`{"name":"alerts","url":"https://example.com/hook","events":["sandbox.lifecycle.created"]}`)

			store.PostEventsWebhooks(ginCtx)

			assert.Equal(t, tc.wantCode, recorder.Code)
			assert.Contains(t, recorder.Body.String(), tc.wantBody)
			assert.NotContains(t, recorder.Body.String(), "pg: connection refused")
		})
	}
}

// Without an address the routes stay registered, so they must answer rather
// than panic on a nil client.
func TestWebhookRoutesAnswerWhenNoBackendIsConfigured(t *testing.T) {
	t.Parallel()

	store := &APIStore{}
	ginCtx, recorder, _ := newWebhooksRequest(t, http.MethodGet, "/events/webhooks", "")

	store.GetEventsWebhooks(ginCtx)

	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.Contains(t, recorder.Body.String(), webhooksUnavailable)
}

// A full deliveries page is larger than gRPC's default 4 MiB receive limit,
// so a request the spec allows would fail with ResourceExhausted — reaching
// the caller as a 500 — unless the connection is sized for it.
func TestGetEventsWebhooksWebhookIDDeliveriesAcceptsAFullPage(t *testing.T) {
	t.Parallel()

	const attempts = 70

	backend := &fakeWebhooksBackend{deliveries: oversizedDeliveries(attempts)}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, _ := newWebhooksRequest(t, http.MethodGet, "/events/webhooks/x/deliveries", "")

	store.GetEventsWebhooksWebhookIDDeliveries(ginCtx, uuid.New(), api.GetEventsWebhooksWebhookIDDeliveriesParams{})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var payload api.WebhookDeliveriesListPayload
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &payload))
	require.Len(t, payload.Data, 1)
	assert.Len(t, payload.Data[0].Attempts, attempts)
}

// oversizedDeliveries builds one group whose attempts together exceed 4 MiB,
// each body at the cap the backend stores.
func oversizedDeliveries(attempts int) []*webhookmanagement.WebhookDeliveryGroup {
	body := strings.Repeat("x", maxDeliveryBodyBytes)

	group := make([]*webhookmanagement.WebhookDelivery, 0, attempts)
	for range attempts {
		group = append(group, webhookmanagement.WebhookDelivery_builder{
			DeliveryId:     uuid.NewString(),
			TeamId:         uuid.NewString(),
			WebhookId:      uuid.NewString(),
			EventId:        uuid.NewString(),
			EventType:      "sandbox.lifecycle.created",
			Status:         webhookmanagement.WebhookDeliveryStatus_WEBHOOK_DELIVERY_STATUS_SUCCESS,
			RequestBody:    body,
			RequestHeaders: "{}",
			RequestUrl:     "https://example.com/hook",
			Timestamp:      timestamppb.New(time.Unix(1700000000, 0).UTC()),
		}.Build())
	}

	return []*webhookmanagement.WebhookDeliveryGroup{
		webhookmanagement.WebhookDeliveryGroup_builder{
			EventId:   uuid.NewString(),
			EventType: "sandbox.lifecycle.created",
			SandboxId: "sandbox",
			Attempts:  group,
		}.Build(),
	}
}

// The connection has to accept whatever the deliveries route can produce, and
// gRPC's own default does not.
func TestDeliveriesPageBudgetExceedsTheGRPCDefault(t *testing.T) {
	t.Parallel()

	const grpcDefaultMaxRecvMsgSize = 4 << 20

	assert.Greater(t, maxDeliveriesPageBytes, grpcDefaultMaxRecvMsgSize)
}

// The contract attaches its violations to the status, and the status message
// renders them as a multi-line dump. The response carries the rendered form,
// which names the field the caller sent.
func TestWebhookValidationRejectionsRenderTheirViolations(t *testing.T) {
	t.Parallel()

	rejected, err := status.New(codes.InvalidArgument, "validation error:\n - events: value must contain at least 1 item(s)").
		WithDetails(&validate.Violations{
			Violations: []*validate.Violation{{
				Field: &validate.FieldPath{
					Elements: []*validate.FieldPathElement{{FieldName: new("events")}},
				},
				Message: new("must specify at least one event type"),
			}},
		})
	require.NoError(t, err)

	backend := &fakeWebhooksBackend{err: rejected.Err()}
	store := startWebhooksBackend(t, backend)
	ginCtx, recorder, _ := newWebhooksRequest(t, http.MethodPost, "/events/webhooks",
		`{"name":"alerts","url":"https://example.com/hook","events":[]}`)

	store.PostEventsWebhooks(ginCtx)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "events: must specify at least one event type")
	assert.NotContains(t, recorder.Body.String(), "value must contain at least 1 item(s)")
	assert.NotContains(t, recorder.Body.String(), `\n`, "the response reads as one line")
}

// The two families share the error mapping but not the resource it names.
func TestNotFoundNamesTheResourceTheCallerAskedAbout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		call     func(store *APIStore, ginCtx *gin.Context)
		wantBody string
	}{
		{
			name:     "webhook",
			call:     func(store *APIStore, c *gin.Context) { store.GetEventsWebhooksWebhookID(c, uuid.New()) },
			wantBody: webhookNotFound,
		},
		{
			name: "team sandbox events",
			call: func(store *APIStore, c *gin.Context) {
				store.GetEventsSandboxes(c, api.GetEventsSandboxesParams{})
			},
			wantBody: sandboxEventsNotFound,
		},
		{
			name: "one sandbox's events",
			call: func(store *APIStore, c *gin.Context) {
				store.GetEventsSandboxesSandboxID(c, "sandbox", api.GetEventsSandboxesSandboxIDParams{})
			},
			wantBody: sandboxEventsNotFound,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := &fakeWebhooksBackend{err: status.Error(codes.NotFound, "absent")}
			store := startWebhooksBackend(t, backend)
			ginCtx, recorder, _ := newWebhooksRequest(t, http.MethodGet, "/events/sandboxes", "")

			tc.call(store, ginCtx)

			require.Equal(t, http.StatusNotFound, recorder.Code)
			assert.Contains(t, recorder.Body.String(), tc.wantBody)
		})
	}
}
