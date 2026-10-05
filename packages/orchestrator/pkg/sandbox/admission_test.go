//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// admissionDevice is a ReadonlyDevice whose durable header is a real SetOnce
// future, like the real template.Storage wrapper over build.File.
type admissionDevice struct {
	*inPlaceRODevice

	durable *utils.SetOnce[*header.Header]
}

func (d *admissionDevice) DurableHeaderNow() (*header.Header, bool) {
	select {
	case <-d.durable.Done:
		h, err := d.durable.Wait()

		return h, err == nil
	default:
		return nil, false
	}
}

func (d *admissionDevice) DurableHeader(ctx context.Context) (*header.Header, error) {
	return d.durable.WaitWithContext(ctx)
}

// admissionTemplate serves only Memfile; the admission pre-flight touches
// nothing else on the template.
type admissionTemplate struct {
	template.Template

	memfile    block.ReadonlyDevice
	memfileErr error
}

func (t admissionTemplate) Memfile(context.Context) (block.ReadonlyDevice, error) {
	return t.memfile, t.memfileErr
}

func admissionHeader(t *testing.T) *header.Header {
	t.Helper()

	h, err := header.NewHeader(&header.Metadata{Version: 3, BlockSize: 4096, Size: 4096, BaseBuildId: uuid.New()}, nil)
	require.NoError(t, err)

	return h
}

func admissionSandbox(durable *utils.SetOnce[*header.Header]) *Sandbox {
	return &Sandbox{
		Template: admissionTemplate{
			memfile: &admissionDevice{inPlaceRODevice: &inPlaceRODevice{}, durable: durable},
		},
	}
}

func TestAwaitSnapshotAdmission_ReadyImmediately(t *testing.T) {
	t.Parallel()

	durable := utils.NewSetOnce[*header.Header]()
	require.NoError(t, durable.SetValue(admissionHeader(t)))

	outcome, waited, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), 5*time.Second, true)
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionReady, outcome)
	assert.Zero(t, waited)
}

func TestAwaitSnapshotAdmission_NoDurableMachineryAdmits(t *testing.T) {
	t.Parallel()

	sbx := &Sandbox{Template: admissionTemplate{memfile: &inPlaceRODevice{}}}

	outcome, _, err := sbx.AwaitSnapshotAdmission(t.Context(), 5*time.Second, true)
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionReady, outcome)
}

func TestAwaitSnapshotAdmission_MemfileErrorAdmits(t *testing.T) {
	t.Parallel()

	sbx := &Sandbox{Template: admissionTemplate{memfileErr: errors.New("no memfile object")}}

	outcome, _, err := sbx.AwaitSnapshotAdmission(t.Context(), 5*time.Second, true)
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionReady, outcome)
}

// A swap resolving mid-grace admits with ready_after_wait.
func TestAwaitSnapshotAdmission_ReadyAfterWait(t *testing.T) {
	t.Parallel()

	durable := utils.NewSetOnce[*header.Header]()
	hdr := admissionHeader(t)
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = durable.SetValue(hdr)
	}()

	outcome, waited, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), 5*time.Second, true)
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionReadyAfterWait, outcome)
	assert.Greater(t, waited, time.Duration(0))
	assert.Less(t, waited, 5*time.Second)
}

// Still pending when the grace elapses refuses with the
// retryable sentinel, on the pre-flight's own timer.
func TestAwaitSnapshotAdmission_RefusedAfterGrace(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		durable := utils.NewSetOnce[*header.Header]()

		start := time.Now()
		outcome, waited, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), 40*time.Millisecond, true)
		require.ErrorIs(t, err, ErrSnapshotAdmissionPending)
		assert.Equal(t, SnapshotAdmissionRefused, outcome)
		assert.Equal(t, 40*time.Millisecond, waited)
		assert.Less(t, time.Since(start), 5*time.Second, "the wait must use its own timer, not the caller's deadline")
	})
}

func TestAwaitSnapshotAdmission_InstantProbeRefuses(t *testing.T) {
	t.Parallel()

	durable := utils.NewSetOnce[*header.Header]()

	outcome, waited, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), 0, true)
	require.ErrorIs(t, err, ErrSnapshotAdmissionPending)
	assert.Equal(t, SnapshotAdmissionRefused, outcome)
	assert.Less(t, waited, time.Second, "an instant probe must not wait")
}

// A filesystem-only snapshot has no memory parent: never refused, even with a
// pending swap.
func TestAwaitSnapshotAdmission_FilesystemOnlySkipsWait(t *testing.T) {
	t.Parallel()

	durable := utils.NewSetOnce[*header.Header]()

	outcome, waited, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), 5*time.Second, false)
	require.NoError(t, err)
	assert.Equal(t, SnapshotAdmissionReady, outcome)
	assert.Zero(t, waited)
}

// The EnsurePausable latched checks are folded into the same pre-flight.
func TestAwaitSnapshotAdmission_LatchedErrorFolded(t *testing.T) {
	t.Parallel()

	sealErr := errors.New("in-place seal failed")
	sealDone := utils.NewSetOnce[struct{}]()
	require.NoError(t, sealDone.SetError(sealErr))

	sbx := admissionSandbox(utils.NewSetOnce[*header.Header]())
	sbx.rootfsSealDone = sealDone

	outcome, _, err := sbx.AwaitSnapshotAdmission(t.Context(), 5*time.Second, true)
	require.ErrorIs(t, err, sealErr)
	assert.Equal(t, SnapshotAdmissionLatchedError, outcome)
	assert.NotErrorIs(t, err, ErrSnapshotAdmissionPending)
}

// A durable future resolved with an error means no valid memory snapshot can
// ever be produced: permanent, not retryable.
func TestAwaitSnapshotAdmission_DedupErrorIsLatched(t *testing.T) {
	t.Parallel()

	dedupErr := errors.New("dedup failed")
	durable := utils.NewSetOnce[*header.Header]()
	require.NoError(t, durable.SetError(dedupErr))

	outcome, _, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), 5*time.Second, true)
	require.ErrorIs(t, err, dedupErr)
	assert.Equal(t, SnapshotAdmissionLatchedError, outcome)
}

// The instant probe must make the same classification: an error-resolved
// future is latched, never retryable-pending.
func TestAwaitSnapshotAdmission_InstantProbeDedupErrorIsLatched(t *testing.T) {
	t.Parallel()

	dedupErr := errors.New("dedup failed")
	durable := utils.NewSetOnce[*header.Header]()
	require.NoError(t, durable.SetError(dedupErr))

	outcome, _, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), 0, true)
	require.ErrorIs(t, err, dedupErr)
	assert.Equal(t, SnapshotAdmissionLatchedError, outcome)
	assert.NotErrorIs(t, err, ErrSnapshotAdmissionPending)
}

// A stored error that WRAPS a context error is still a permanent dedup
// failure: refusal is judged by the grace timer, never by matching the
// returned error against context errors.
func TestAwaitSnapshotAdmission_StoredDeadlineErrorIsLatched(t *testing.T) {
	t.Parallel()

	dedupErr := fmt.Errorf("dedup aborted: %w", context.DeadlineExceeded)
	durable := utils.NewSetOnce[*header.Header]()
	require.NoError(t, durable.SetError(dedupErr))

	for _, grace := range []time.Duration{0, 5 * time.Second} {
		outcome, _, err := admissionSandbox(durable).AwaitSnapshotAdmission(t.Context(), grace, true)
		require.ErrorIs(t, err, dedupErr, "grace %v", grace)
		assert.Equal(t, SnapshotAdmissionLatchedError, outcome, "grace %v", grace)
		assert.NotErrorIs(t, err, ErrSnapshotAdmissionPending, "grace %v", grace)
	}
}

// The caller's context ending mid-wait is not an admission decision.
func TestAwaitSnapshotAdmission_CallerContextEnds(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	outcome, _, err := admissionSandbox(utils.NewSetOnce[*header.Header]()).AwaitSnapshotAdmission(ctx, 5*time.Second, true)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, outcome)
}
