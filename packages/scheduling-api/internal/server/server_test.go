package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/id"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const sandboxID = "i5f3k9q2m8x1c7v4b6n0p"

var endTime = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

func TestStartAndRead(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	project, rid := uuid.NewString(), uuid.NewString()

	reserved, err := client.Reserve(ctx, reserveRequest(project, sandboxID, rid, 10))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	require.False(t, reserved.HasSandbox())

	got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.False(t, got.HasSandbox(), "a starting sandbox is not visible")

	stored, err := client.Put(ctx, putRequest(project, rid))
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_STORED, stored.GetOutcome())

	got, err = client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	requireSandbox(t, project, got.GetSandbox())

	actorID, err := id.SandboxIDToUUID(sandboxID)
	require.NoError(t, err)
	var state string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT state FROM public.scheduling_actor_run WHERE actor_id = $1 AND project_id = $2`, actorID, project).Scan(&state))
	require.Equal(t, "running", state, "stored under the internal actor UUID as a readable state")

	running, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 10))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_ALREADY_RUNNING, running.GetOutcome())
	requireSandbox(t, project, running.GetSandbox())

	finished, err := client.FinishStart(ctx, finishStartRequest(project, rid))
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishStartOutcome_FINISH_START_OUTCOME_ALREADY_STORED, finished.GetOutcome())
}

func TestFailedStart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	project := uuid.NewString()
	rid := uuid.New()
	rid[15] = 0x37

	reserved, err := client.Reserve(ctx, reserveRequest(project, sandboxID, rid.String(), 50_000))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	require.Equal(t, int32(1), slotCount(t, pool, project, 7))

	for range 2 {
		finished, err := client.FinishStart(ctx, finishStartRequest(project, rid.String()))
		require.NoError(t, err)
		require.Equal(t, scheduling.FinishStartOutcome_FINISH_START_OUTCOME_FINISHED, finished.GetOutcome())
		require.Equal(t, int32(0), slotCount(t, pool, project, 7))
	}

	delayed, err := client.Reserve(ctx, reserveRequest(project, sandboxID, rid.String(), 50_000))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_FINISHED, delayed.GetOutcome())
	require.Equal(t, "node failed", delayed.GetFailure().GetMessage())
	require.Equal(t, int32(13), delayed.GetFailure().GetCode())
	require.Equal(t, int32(0), slotCount(t, pool, project, 7))

	stored, err := client.Put(ctx, putRequest(project, rid.String()))
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_NOT_FOUND, stored.GetOutcome())
	got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.False(t, got.HasSandbox())
	finished, err := client.FinishStart(ctx, finishStartRequest(project, uuid.NewString()))
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishStartOutcome_FINISH_START_OUTCOME_NOT_FOUND, finished.GetOutcome())

	next := uuid.NewString()
	reserved, err = client.Reserve(ctx, reserveRequest(project, sandboxID, next, 10))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	stored, err = client.Put(ctx, putRequest(project, rid.String()))
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_NOT_FOUND, stored.GetOutcome(), "the old reservation stays ended")
	stored, err = client.Put(ctx, putRequest(project, next))
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_STORED, stored.GetOutcome())
	require.Equal(t, int32(1), slotCount(t, pool, project, 0))
}

func TestReserveOutcomes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, _ := newClient(t)
	project, other := uuid.NewString(), uuid.NewString()

	reserved, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 1))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())

	pending, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 1))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_ALREADY_PENDING, pending.GetOutcome())

	exceeded, err := client.Reserve(ctx, reserveRequest(project, "i00000000000000000001", uuid.NewString(), 1))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED, exceeded.GetOutcome())

	_, err = client.Reserve(ctx, reserveRequest(other, sandboxID, uuid.NewString(), 1))
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: other, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.False(t, got.HasSandbox())
}

func TestInvalidRequests(t *testing.T) {
	t.Parallel()
	client, _ := newClient(t)
	project, rid := uuid.NewString(), uuid.NewString()

	reserve := func(change func(*scheduling.ReserveRequest)) func(context.Context) error {
		return func(ctx context.Context) error {
			req := reserveRequest(project, sandboxID, rid, 10)
			change(req)
			_, err := client.Reserve(ctx, req)

			return err
		}
	}
	put := func(change func(*scheduling.PutRequest)) func(context.Context) error {
		return func(ctx context.Context) error {
			req := putRequest(project, rid)
			change(req)
			_, err := client.Put(ctx, req)

			return err
		}
	}

	for name, tc := range map[string]struct {
		call       func(context.Context) error
		violations []string
	}{
		"UUID as sandbox ID":    {reserve(func(r *scheduling.ReserveRequest) { r.SetSandboxId(uuid.NewString()) }), []string{"sandbox_id"}},
		"upper-case sandbox ID": {reserve(func(r *scheduling.ReserveRequest) { r.SetSandboxId("I5F3K9Q2M8X1C7V4B6N0P") }), []string{"sandbox_id"}},
		"short sandbox ID":      {reserve(func(r *scheduling.ReserveRequest) { r.SetSandboxId("i5f3k9q2m8x1c7v4b6n0") }), []string{"sandbox_id"}},
		"project ID":            {reserve(func(r *scheduling.ReserveRequest) { r.SetProjectId("project") }), []string{"project_id"}},
		"every field": {reserve(func(r *scheduling.ReserveRequest) {
			r.SetProjectId("")
			r.SetSandboxId("")
			r.SetReservationId("reservation")
			r.SetLimit(0)
			r.ClearEndTime()
		}), []string{"project_id", "sandbox_id", "reservation_id", "limit", "end_time"}},
		"end time out of range": {reserve(func(r *scheduling.ReserveRequest) {
			r.SetEndTime(&timestamppb.Timestamp{Seconds: 253402300800})
		}), []string{"end_time"}},
		"put fields": {put(func(r *scheduling.PutRequest) {
			r.SetExecutionId("")
			r.ClearStartTime()
			r.SetMaxInstanceLength(durationpb.New(0))
			r.SetNodeId("")
			r.SetClusterId("cluster")
			r.SetMetadata(map[string]string{"key": "a\x00b"})
			r.SetRecord([]byte("{"))
		}), []string{"execution_id", "start_time", "max_instance_length", "node_id", "cluster_id", "metadata[\"key\"]", "record"}},
		"NUL in stored text": {put(func(r *scheduling.PutRequest) {
			r.SetExecutionId("a\x00")
			r.SetNodeId("a\x00")
			r.SetBaseTemplateId("a\x00")
			r.SetTemplateId("a\x00")
		}), []string{"execution_id", "node_id", "base_template_id", "template_id"}},
		"max instance length below a microsecond": {put(func(r *scheduling.PutRequest) {
			r.SetMaxInstanceLength(durationpb.New(500 * time.Nanosecond))
		}), []string{"max_instance_length"}},
		"max instance length too long": {put(func(r *scheduling.PutRequest) {
			r.SetMaxInstanceLength(&durationpb.Duration{Seconds: 9223372037})
		}), []string{"max_instance_length"}},
		"negative max instance length": {put(func(r *scheduling.PutRequest) {
			r.SetMaxInstanceLength(durationpb.New(-time.Hour))
		}), []string{"max_instance_length"}},
		"finish start without failure": {func(ctx context.Context) error {
			req := finishStartRequest(project, rid)
			req.ClearFailure()
			_, err := client.FinishStart(ctx, req)

			return err
		}, []string{"failure"}},
		"empty failure message": {func(ctx context.Context) error {
			req := finishStartRequest(project, rid)
			req.GetFailure().SetMessage("")
			_, err := client.FinishStart(ctx, req)

			return err
		}, []string{"failure.message"}},
		"begin fields": {func(ctx context.Context) error {
			_, err := client.BeginTransition(ctx, scheduling.BeginTransitionRequest_builder{
				ProjectId:           "",
				SandboxId:           "",
				TransitionId:        "transition",
				ExpectedExecutionId: "a\x00",
			}.Build())

			return err
		}, []string{"project_id", "sandbox_id", "transition_id", "action", "expected_execution_id"}},
		"undefined action": {func(ctx context.Context) error {
			req := beginRequest(project, uuid.NewString(), scheduling.TransitionAction(99))
			_, err := client.BeginTransition(ctx, req)

			return err
		}, []string{"action"}},
		"finish transition fields": {func(ctx context.Context) error {
			_, err := client.FinishTransition(ctx, scheduling.FinishTransitionRequest_builder{
				ProjectId:    project,
				SandboxId:    sandboxID,
				TransitionId: "",
				Failure:      scheduling.Failure_builder{Message: ""}.Build(),
			}.Build())

			return err
		}, []string{"transition_id", "failure.message"}},
		"restore without retry_after": {func(ctx context.Context) error {
			req := restoreRequest(project, uuid.NewString(), time.Second)
			req.ClearRetryAfter()
			_, err := client.RestoreRunning(ctx, req)

			return err
		}, []string{"retry_after"}},
		"negative retry_after": {func(ctx context.Context) error {
			_, err := client.RestoreRunning(ctx, restoreRequest(project, uuid.NewString(), -time.Second))

			return err
		}, []string{"retry_after"}},
		"wait fields": {func(ctx context.Context) error {
			req := waitRequest(project, "", time.Second)
			req.ClearMaxWait()
			_, err := client.WaitForTransition(ctx, req)

			return err
		}, []string{"transition_id", "max_wait"}},
		"zero max_wait": {func(ctx context.Context) error {
			_, err := client.WaitForTransition(ctx, waitRequest(project, uuid.NewString(), 0))

			return err
		}, []string{"max_wait"}},
		"get": {func(ctx context.Context) error {
			_, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: "sandbox"}.Build())

			return err
		}, []string{"sandbox_id"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tc.call(t.Context())
			require.Equal(t, codes.InvalidArgument, status.Code(err), err)
			var fields []string
			for _, detail := range status.Convert(err).Details() {
				violations, ok := detail.(*validate.Violations)
				require.True(t, ok)
				for _, violation := range violations.GetViolations() {
					fields = append(fields, protovalidate.FieldPathString(violation.GetField()))
				}
			}
			require.ElementsMatch(t, tc.violations, fields)
		})
	}

	for name, record := range map[string]string{
		"array":         `[]`,
		"null":          `null`,
		"string":        `"{}"`,
		"invalid":       `{"a":}`,
		"invalid UTF-8": "{\"a\":\"\xff\"}",
	} {
		t.Run("record "+name, func(t *testing.T) {
			t.Parallel()

			err := put(func(r *scheduling.PutRequest) { r.SetRecord([]byte(record)) })(t.Context())
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Equal(t, "record: must be a JSON object that PostgreSQL jsonb can store", status.Convert(err).Message())
			require.Empty(t, status.Convert(err).Details())
		})
	}

	t.Run("retry_after below microsecond precision", func(t *testing.T) {
		t.Parallel()

		_, err := client.RestoreRunning(t.Context(), restoreRequest(project, uuid.NewString(), time.Microsecond+1))
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Equal(t, "retry_after: must be a whole number of microseconds", status.Convert(err).Message())
	})

	t.Run("max_wait above the limit", func(t *testing.T) {
		t.Parallel()

		_, err := client.WaitForTransition(t.Context(), waitRequest(project, uuid.NewString(), 5*time.Second+time.Nanosecond))
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Equal(t, "max_wait: must be at most 5s", status.Convert(err).Message())
	})

	t.Run("max instance length below microsecond precision", func(t *testing.T) {
		t.Parallel()

		err := put(func(r *scheduling.PutRequest) { r.SetMaxInstanceLength(durationpb.New(time.Microsecond + 1)) })(t.Context())
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Equal(t, "max_instance_length: must be a whole number of microseconds", status.Convert(err).Message())
	})
}

// PostgreSQL rejects these records although they are JSON objects. Put must
// answer INVALID_ARGUMENT and leave the start as it was.
func TestRecordsJSONBRejects(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	project, rid := uuid.NewString(), uuid.NewString()

	reserved, err := client.Reserve(ctx, reserveRequest(project, sandboxID, rid, 10))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())

	for name, record := range map[string]string{
		"lone surrogate":  `{"a":"\ud800"}`,
		"number overflow": `{"a":1e1000000}`,
		"NUL escape":      `{"a":"\u0000"}`,
	} {
		req := putRequest(project, rid)
		req.SetRecord([]byte(record))
		_, err := client.Put(ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
		require.Equal(t, "record: must be a JSON object that PostgreSQL jsonb can store", status.Convert(err).Message(), name)
	}

	got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.False(t, got.HasSandbox(), "still starting")
	require.Equal(t, int32(1), slotCount(t, pool, project, 0))

	stored, err := client.Put(ctx, putRequest(project, rid))
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_STORED, stored.GetOutcome())
	require.Equal(t, int32(1), slotCount(t, pool, project, 0))
}

func TestStoredValuesRoundTrip(t *testing.T) {
	t.Parallel()
	client, _ := newClient(t)

	for name, tc := range map[string]struct {
		sandboxID         string
		record            string
		maxInstanceLength *durationpb.Duration
	}{
		"surrogate pair":          {"i00000000000000000001", `{"a":"\ud83d\ude00"}`, durationpb.New(time.Hour)},
		"escaped backslash u0000": {"i00000000000000000002", `{"a":"\\u0000"}`, durationpb.New(time.Hour)},
		"shortest length":         {"i00000000000000000003", `{}`, durationpb.New(time.Microsecond)},
		"longest length":          {"i00000000000000000004", `{}`, &durationpb.Duration{Seconds: 9223372036}},
		"sub-second length":       {"i00000000000000000005", `{}`, durationpb.New(90*time.Minute + 123456*time.Microsecond)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			project, rid := uuid.NewString(), uuid.NewString()

			reserved, err := client.Reserve(ctx, reserveRequest(project, tc.sandboxID, rid, 10))
			require.NoError(t, err)
			require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
			req := putRequest(project, rid)
			req.SetSandboxId(tc.sandboxID)
			req.SetRecord([]byte(tc.record))
			req.SetMaxInstanceLength(tc.maxInstanceLength)
			stored, err := client.Put(ctx, req)
			require.NoError(t, err)
			require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_STORED, stored.GetOutcome())

			got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: tc.sandboxID}.Build())
			require.NoError(t, err)
			assert.JSONEq(t, tc.record, string(got.GetSandbox().GetRecord()))
			assert.Equal(t, tc.maxInstanceLength.AsDuration(), got.GetSandbox().GetMaxInstanceLength().AsDuration())
		})
	}
}

func TestBackendFailure(t *testing.T) {
	t.Parallel()
	client, pool := newClient(t)
	pool.Close()

	_, err := client.Reserve(t.Context(), reserveRequest(uuid.NewString(), sandboxID, uuid.NewString(), 10))
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, "internal error", status.Convert(err).Message())
	require.Empty(t, status.Convert(err).Details())

	_, err = client.Get(t.Context(), scheduling.GetRequest_builder{ProjectId: uuid.NewString(), SandboxId: sandboxID}.Build())
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, "internal error", status.Convert(err).Message())

	project, tid := uuid.NewString(), uuid.NewString()
	for name, call := range map[string]func(context.Context) error{
		"begin": func(ctx context.Context) error {
			_, err := client.BeginTransition(ctx, beginRequest(project, tid, scheduling.TransitionAction_TRANSITION_ACTION_KILL))

			return err
		},
		"finish": func(ctx context.Context) error {
			_, err := client.FinishTransition(ctx, finishTransitionRequest(project, tid))

			return err
		},
		"restore": func(ctx context.Context) error {
			_, err := client.RestoreRunning(ctx, restoreRequest(project, tid, time.Second))

			return err
		},
		"wait": func(ctx context.Context) error {
			_, err := client.WaitForTransition(ctx, waitRequest(project, tid, time.Second))

			return err
		},
	} {
		err := call(t.Context())
		require.Equal(t, codes.Internal, status.Code(err), name)
		require.Equal(t, "internal error", status.Convert(err).Message(), name)
		require.Empty(t, status.Convert(err).Details(), name)
	}
}

func newClient(t *testing.T) (scheduling.SchedulingServiceClient, *pgxpool.Pool) {
	t.Helper()

	pool := dbtest.MigratedPool(t)

	return serve(t, pool), pool
}

func serve(t *testing.T, pool *pgxpool.Pool) scheduling.SchedulingServiceClient {
	t.Helper()

	grpcServer, err := server.New(telemetry.NewNoopClient(), store.New(pool, pool))
	require.NoError(t, err)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	return scheduling.NewSchedulingServiceClient(conn)
}

func reserveRequest(project, sandbox, rid string, limit int64) *scheduling.ReserveRequest {
	return scheduling.ReserveRequest_builder{
		ProjectId:     project,
		SandboxId:     sandbox,
		ReservationId: rid,
		Limit:         limit,
		EndTime:       timestamppb.New(endTime),
	}.Build()
}

func putRequest(project, rid string) *scheduling.PutRequest {
	return scheduling.PutRequest_builder{
		ProjectId:               project,
		SandboxId:               sandboxID,
		ReservationId:           rid,
		ExecutionId:             "execution-1",
		StartTime:               timestamppb.New(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
		MaxInstanceLength:       durationpb.New(time.Hour),
		NodeId:                  "node-1",
		ClusterId:               "8b8c8d3e-7f45-4f0e-9a43-5c2a1c6d9e10",
		BaseTemplateId:          "base",
		TemplateId:              "template",
		Metadata:                map[string]string{"owner": "tests"},
		AutoPause:               true,
		AutoPauseFilesystemOnly: true,
		Record:                  []byte(`{"vcpu": 2, "alias": "a"}`),
	}.Build()
}

func finishStartRequest(project, rid string) *scheduling.FinishStartRequest {
	return scheduling.FinishStartRequest_builder{
		ProjectId:     project,
		SandboxId:     sandboxID,
		ReservationId: rid,
		Failure:       scheduling.Failure_builder{Message: "node failed", Code: 13}.Build(),
	}.Build()
}

func requireSandbox(t *testing.T, project string, got *scheduling.Sandbox) {
	t.Helper()

	want := putRequest(project, "")
	require.NotNil(t, got)
	assert.Equal(t, sandboxID, got.GetSandboxId())
	assert.Equal(t, project, got.GetProjectId())
	assert.Equal(t, scheduling.SandboxState_SANDBOX_STATE_RUNNING, got.GetState())
	assert.True(t, endTime.Equal(got.GetEndTime().AsTime()))
	assert.Equal(t, want.GetExecutionId(), got.GetExecutionId())
	assert.True(t, want.GetStartTime().AsTime().Equal(got.GetStartTime().AsTime()))
	assert.Equal(t, want.GetMaxInstanceLength().AsDuration(), got.GetMaxInstanceLength().AsDuration())
	assert.Equal(t, want.GetNodeId(), got.GetNodeId())
	assert.Equal(t, want.GetClusterId(), got.GetClusterId())
	assert.Equal(t, want.GetBaseTemplateId(), got.GetBaseTemplateId())
	assert.Equal(t, want.GetTemplateId(), got.GetTemplateId())
	assert.Equal(t, want.GetMetadata(), got.GetMetadata())
	assert.Equal(t, want.GetAutoPause(), got.GetAutoPause())
	assert.Equal(t, want.GetAutoPauseFilesystemOnly(), got.GetAutoPauseFilesystemOnly())
	assert.JSONEq(t, string(want.GetRecord()), string(got.GetRecord()))
}

func slotCount(t *testing.T, pool *pgxpool.Pool, project string, slot int16) int32 {
	t.Helper()

	var live int32
	require.NoError(t, pool.QueryRow(t.Context(),
		`SELECT live FROM public.scheduling_project_live_count WHERE project_id = $1 AND slot = $2`, project, slot).Scan(&live))

	return live
}
