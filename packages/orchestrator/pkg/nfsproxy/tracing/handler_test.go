//go:build linux

package tracing

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
)

type identityHandler struct {
	fs billy.Filesystem
}

func (h *identityHandler) Mount(context.Context, net.Conn, nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	return nfs.MountStatusOk, h.fs, nil
}

func (*identityHandler) Change(context.Context, billy.Filesystem) billy.Change { return nil }

func (*identityHandler) FSStat(context.Context, billy.Filesystem, *nfs.FSStat) error { return nil }

func (*identityHandler) ToHandle(context.Context, billy.Filesystem, []string) []byte {
	return []byte{1}
}

func (h *identityHandler) FromHandle(context.Context, []byte) (billy.Filesystem, []string, error) {
	return h.fs, nil, nil
}

func (*identityHandler) InvalidateHandle(context.Context, billy.Filesystem, []byte) error { return nil }

func (*identityHandler) HandleLimit() int { return 2 }

func TestFromHandleUsesCurrentRequestTrace(t *testing.T) { //nolint:paralleltest // replaces the package tracer with an isolated exporter
	for _, recordHandles := range []bool{false, true} { //nolint:paralleltest // package tracer replacement
		t.Run(strconv.FormatBool(recordHandles), func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			previous := tracer
			tracer = provider.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/tracing")
			t.Cleanup(func() {
				tracer = previous
				require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
			})
			filesystem := memfs.New()
			file, err := filesystem.Create("file")
			require.NoError(t, err)
			require.NoError(t, file.Close())
			handler := WrapWithTracing(&identityHandler{fs: filesystem}, cfg.Config{RecordHandleCalls: recordHandles, RecordStatCalls: true})

			oldCtx, oldRequest := tracer.Start(t.Context(), "old-request")
			oldCtx, cancelOld := context.WithCancel(oldCtx)
			oldFS, _, err := handler.FromHandle(oldCtx, []byte{1})
			require.NoError(t, err)
			cancelOld()
			oldRequest.End()
			exporter.Reset()

			ctx, request := tracer.Start(t.Context(), "current-request")
			defer request.End()
			fs, _, err := handler.FromHandle(ctx, []byte{1})
			require.NoError(t, err)
			require.NotSame(t, oldFS, fs, "each lookup needs its own request context")
			wrapped, ok := fs.(*tracingFS)
			require.True(t, ok, "filesystem tracing must remain enabled without handle-call spans")
			require.NoError(t, wrapped.ctx.Err(), "a cancelled earlier request must not poison this lookup")
			require.Same(t, filesystem, wrapped.Unwrap())
			_, err = fs.Stat("file")
			require.NoError(t, err)

			spans := exporter.GetSpans()
			expectedParent := trace.SpanContextFromContext(ctx)
			if recordHandles {
				require.Len(t, spans, 2)
				require.Equal(t, "NFS.FromHandle", spans[0].Name)
				require.Equal(t, expectedParent.SpanID(), spans[0].Parent.SpanID())
				expectedParent = spans[0].SpanContext
			} else {
				require.Len(t, spans, 1)
			}
			stat := spans[len(spans)-1]
			require.Equal(t, "FS.Stat", stat.Name)
			require.Equal(t, expectedParent.SpanID(), stat.Parent.SpanID())
			require.Equal(t, request.SpanContext().TraceID(), stat.SpanContext.TraceID())
			require.NotEqual(t, oldRequest.SpanContext().TraceID(), stat.SpanContext.TraceID())
		})
	}
}
