//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// guestMemoryChain is the error a create right after a kill returns: the uffd
// handler's memfd mmap failed inside the snapshot load, and every wrap on the
// way to Create keeps the value.
func guestMemoryChain() error {
	mmapErr := fmt.Errorf("mmap memfd: %w", sandbox.GuestMemoryError{Err: unix.ENOMEM})
	uffdExit := fmt.Errorf("uffd process exited: %w", errors.Join(fmt.Errorf("failed to wrap memfd: %w", mmapErr)))
	load := &url.Error{Op: "Put", URL: "http://localhost/snapshot/load", Err: uffdExit}

	return fmt.Errorf("failed to start FC: %w", fmt.Errorf("error loading snapshot: %w", load))
}

// hasCreateRefusalDetail reports whether the status carries the typed detail
// that stops placement on every node; neither memory answer may carry it.
func hasCreateRefusalDetail(st *status.Status) bool {
	for _, detail := range st.Details() {
		if userErr, ok := detail.(*orchestrator.UserError); ok && userErr.GetCode() == orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED {
			return true
		}
	}

	return false
}

func memoryRefusalServer(t *testing.T, flagOn bool) *Server {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.CreateMemoryRefusalFlag.Key()).VariationForAll(flagOn))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	s := duplicateCreateTestServer()
	s.featureFlags = ff

	return s
}

// The pool could not back the guest. While a sandbox on the node is tearing
// down, now or when the create began, its pages are on their way back, so the
// create is refused for placement to retry here; otherwise the pool is too
// small or held by running sandboxes, and the create fails at once with that
// said. Anything that is not the guest-memory mmap is left to the caller,
// errno text included, and so is everything with the flag off.
func TestMemoryRefusal(t *testing.T) {
	t.Parallel()

	runtime := sandboxtypes.RuntimeMetadata{SandboxID: "sandbox-2", TemplateID: "template-1", BuildID: "build-1"}
	s := memoryRefusalServer(t, true)

	r, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	stopping := drainTestSandbox(t, "lifecycle-1")
	require.NoError(t, r.MarkRunning(t.Context(), stopping))
	r.Release()
	require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), "sandbox-1", "lifecycle-1"))

	refusal := s.memoryRefusal(t.Context(), guestMemoryChain(), 8192, 0, runtime)
	st, ok := status.FromError(refusal)
	require.True(t, ok, "%v", refusal)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	assert.Contains(t, st.Message(), "please retry")
	assert.False(t, hasCreateRefusalDetail(st), "a retryable refusal carries no for-good detail")

	s.sandboxFactory.Sandboxes.MarkStopped(t.Context(), stopping)
	failure := s.memoryRefusal(t.Context(), guestMemoryChain(), 8192, 0, runtime)
	st, ok = status.FromError(failure)
	require.True(t, ok, "%v", failure)
	assert.Equal(t, codes.FailedPrecondition, st.Code(), "nothing is tearing down: fail at once")
	assert.Contains(t, st.Message(), "8192 MiB")
	assert.NotContains(t, st.Message(), "snapshot/load", "the client gets the cause, not the chain")
	assert.False(t, hasCreateRefusalDetail(st), "another node may have a larger pool")

	// The teardown that held the pages ended while the failed start ran its
	// own teardown; the count read before the start still says retry.
	ended := s.memoryRefusal(t.Context(), guestMemoryChain(), 8192, 1, runtime)
	assert.Equal(t, codes.ResourceExhausted, status.Code(ended), "a teardown in flight when the create began counts")

	forkExec := fmt.Errorf("failed to start FC: %w", fmt.Errorf("error starting fc process: %w", &fs.PathError{Op: "fork/exec", Path: "/fc", Err: unix.ENOMEM}))
	assert.NoError(t, s.memoryRefusal(t.Context(), forkExec, 8192, 1, runtime), "an ENOMEM elsewhere is not the pool")
	assert.NoError(t, s.memoryRefusal(t.Context(), errors.New("mmap memfd: cannot allocate memory"), 8192, 1, runtime), "the text alone does not count")
	assert.NoError(t, s.memoryRefusal(t.Context(), errors.New("envd did not start"), 8192, 1, runtime))

	off := memoryRefusalServer(t, false)
	assert.NoError(t, off.memoryRefusal(t.Context(), guestMemoryChain(), 8192, 1, runtime), "flag off: today's path")
}
