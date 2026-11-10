package store

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	spanRecorder   = tracetest.NewSpanRecorder()
	tracerProvider = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
)

func TestMain(m *testing.M) {
	otel.SetTracerProvider(tracerProvider)
	m.Run()
}

func TestAPIMethodSpans(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	project, actor, claim := uuid.New(), uuid.New(), uuid.New()

	ctx, parent := tracerProvider.Tracer("github.com/e2b-dev/infra/packages/scheduling-api/internal/store").Start(t.Context(), "test")
	_, err := st.ListLive(ctx, &project, nil, false, uuid.Nil, 10)
	require.NoError(t, err)
	_, err = st.InspectReservation(ctx, project, actor)
	require.NoError(t, err)
	_, err = st.UpdateDeadline(ctx, project, actor, "execution", endTime, endTime.Add(-time.Hour))
	require.ErrorIs(t, err, ErrConflict)
	claimed, err := st.ClaimSnapshotDeletion(ctx, project, actor, claim)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, st.ReleaseSnapshotDeletionClaim(ctx, project, actor, claim))
	parent.End()

	statuses := map[string]codes.Code{}
	for _, span := range spanRecorder.Ended() {
		if span.Parent().SpanID() == parent.SpanContext().SpanID() {
			statuses[span.Name()] = span.Status().Code
		}
	}
	require.Equal(t, map[string]codes.Code{
		"store.ListLive":                     codes.Ok,
		"store.InspectReservation":           codes.Ok,
		"store.UpdateDeadline":               codes.Error,
		"store.ClaimSnapshotDeletion":        codes.Ok,
		"store.ReleaseSnapshotDeletionClaim": codes.Ok,
	}, statuses)
}
