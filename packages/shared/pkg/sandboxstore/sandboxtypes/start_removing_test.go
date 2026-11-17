package sandboxtypes

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/joined"
)

type beginResult struct {
	transition  StateTransition
	alreadyDone bool
	err         error
}

type waitCall struct {
	teamID       uuid.UUID
	sandboxID    string
	transitionID string
}

type fakeTransitions struct {
	StateTransitions

	begins []beginResult
	waits  []error

	beginOpts []RemoveOpts
	waitCalls []waitCall
}

func (f *fakeTransitions) BeginRemoving(_ context.Context, _ uuid.UUID, _ string, opts RemoveOpts) (StateTransition, bool, error) {
	f.beginOpts = append(f.beginOpts, opts)
	r := f.begins[0]
	f.begins = f.begins[1:]

	return r.transition, r.alreadyDone, r.err
}

func (f *fakeTransitions) WaitForTransition(_ context.Context, teamID uuid.UUID, sandboxID string, transitionID string) (string, error) {
	f.waitCalls = append(f.waitCalls, waitCall{teamID: teamID, sandboxID: sandboxID, transitionID: transitionID})
	err := f.waits[0]
	f.waits = f.waits[1:]

	return transitionID, err
}

func TestStartRemoving(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	pausing := Sandbox{SandboxID: "sbx", TeamID: teamID, State: StatePausing}
	killing := Sandbox{SandboxID: "sbx", TeamID: teamID, State: StateKilling}
	inFlight := func(sbx Sandbox, id string) beginResult {
		return beginResult{transition: StateTransition{Sandbox: sbx, InFlightID: id}}
	}
	pause := RemoveOpts{Action: StateActionPause}
	fsPause := RemoveOpts{Action: StateActionPause, FilesystemOnly: true}
	failure := errors.New("transition failed: boom")
	started := StateTransition{Sandbox: killing, ID: "started"}

	tests := []struct {
		name   string
		opts   RemoveOpts
		begins []beginResult
		waits  []error

		want        StateTransition
		alreadyDone bool
		check       func(t *testing.T, err error)
		waitedFor   []string
		joined      bool
	}{
		{
			name:   "begin error",
			opts:   pause,
			begins: []beginResult{{transition: StateTransition{Sandbox: pausing}, err: ErrLifecycleHeld}},
			want:   StateTransition{Sandbox: pausing},
			check:  func(t *testing.T, err error) { t.Helper(); require.Equal(t, ErrLifecycleHeld, err) },
		},
		{
			name:   "begin error with a transition in flight",
			opts:   pause,
			begins: []beginResult{{transition: StateTransition{Sandbox: pausing, InFlightID: "open"}, err: ErrEvictionInProgress}},
			want:   StateTransition{Sandbox: pausing, InFlightID: "open"},
			check:  func(t *testing.T, err error) { t.Helper(); require.Equal(t, ErrEvictionInProgress, err) },
		},
		{
			name:   "started",
			opts:   pause,
			begins: []beginResult{{transition: started}},
			want:   started,
			check:  func(t *testing.T, err error) { t.Helper(); require.NoError(t, err) },
		},
		{
			name:        "already done",
			opts:        pause,
			begins:      []beginResult{{transition: StateTransition{Sandbox: pausing}, alreadyDone: true}},
			want:        StateTransition{Sandbox: pausing},
			alreadyDone: true,
			check:       func(t *testing.T, err error) { t.Helper(); require.NoError(t, err) },
		},
		{
			name:        "join succeeds",
			opts:        pause,
			begins:      []beginResult{inFlight(pausing, "open")},
			waits:       []error{nil},
			want:        StateTransition{Sandbox: pausing},
			alreadyDone: true,
			check:       func(t *testing.T, err error) { t.Helper(); require.NoError(t, err) },
			waitedFor:   []string{"open"},
			joined:      true,
		},
		{
			name:   "join fails",
			opts:   pause,
			begins: []beginResult{inFlight(pausing, "open")},
			waits:  []error{failure},
			want:   StateTransition{Sandbox: pausing},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.EqualError(t, err, "failed waiting for transition: transition failed: boom")
				require.ErrorIs(t, err, failure)
			},
			waitedFor: []string{"open"},
			joined:    true,
		},
		{
			name:   "join interrupted",
			opts:   pause,
			begins: []beginResult{inFlight(pausing, "open")},
			waits:  []error{context.Canceled},
			want:   StateTransition{Sandbox: pausing},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.EqualError(t, err, "failed waiting for transition: context canceled")
				require.ErrorIs(t, err, context.Canceled)
			},
			waitedFor: []string{"open"},
			joined:    true,
		},
		{
			name:   "memory pause joins an unsupported pause",
			opts:   pause,
			begins: []beginResult{inFlight(pausing, "open")},
			waits:  []error{&RestoredTransitionError{Refusal: PauseUnsupportedError{}}},
			want:   StateTransition{Sandbox: pausing},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Equal(t, PauseQueueExhaustedError{}, err)
			},
			waitedFor: []string{"open"},
			joined:    true,
		},
		{
			name:   "filesystem pause joins an unsupported pause",
			opts:   fsPause,
			begins: []beginResult{inFlight(pausing, "open")},
			waits:  []error{&RestoredTransitionError{Refusal: PauseUnsupportedError{}}},
			want:   StateTransition{Sandbox: pausing},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Equal(t, PauseUnsupportedError{}, err)
			},
			waitedFor: []string{"open"},
			joined:    true,
		},
		{
			name:   "filesystem pause joins a refused pause",
			opts:   fsPause,
			begins: []beginResult{inFlight(pausing, "open")},
			waits:  []error{&RestoredTransitionError{Refusal: PauseRefusedCheckpointInProgressError{}}},
			want:   StateTransition{Sandbox: pausing},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.Equal(t, PauseRefusedCheckpointInProgressError{}, err)
			},
			waitedFor: []string{"open"},
			joined:    true,
		},
		{
			name:      "other target begins again after the wait",
			opts:      RemoveOpts{Action: StateActionKill},
			begins:    []beginResult{inFlight(pausing, "open"), {transition: started}},
			waits:     []error{nil},
			want:      started,
			check:     func(t *testing.T, err error) { t.Helper(); require.NoError(t, err) },
			waitedFor: []string{"open"},
		},
		{
			name:      "other target begins again after a restored transition",
			opts:      RemoveOpts{Action: StateActionKill},
			begins:    []beginResult{inFlight(pausing, "open"), {transition: started}},
			waits:     []error{&RestoredTransitionError{Refusal: PauseQueueExhaustedError{}}},
			want:      started,
			check:     func(t *testing.T, err error) { t.Helper(); require.NoError(t, err) },
			waitedFor: []string{"open"},
		},
		{
			name:      "other target begins again after a plain restore",
			opts:      RemoveOpts{Action: StateActionKill},
			begins:    []beginResult{inFlight(pausing, "open"), {transition: started}},
			waits:     []error{ErrTransitionRestored},
			want:      started,
			check:     func(t *testing.T, err error) { t.Helper(); require.NoError(t, err) },
			waitedFor: []string{"open"},
		},
		{
			name:   "other target wait fails",
			opts:   RemoveOpts{Action: StateActionKill},
			begins: []beginResult{inFlight(pausing, "open")},
			waits:  []error{failure},
			want:   StateTransition{Sandbox: pausing},
			check: func(t *testing.T, err error) {
				t.Helper()
				require.EqualError(t, err, "failed waiting for transition: transition failed: boom")
				require.ErrorIs(t, err, failure)
			},
			waitedFor: []string{"open"},
		},
		{
			name:        "other target joins the transition that began meanwhile",
			opts:        RemoveOpts{Action: StateActionKill},
			begins:      []beginResult{inFlight(pausing, "open"), inFlight(killing, "next")},
			waits:       []error{nil, nil},
			want:        StateTransition{Sandbox: killing},
			alreadyDone: true,
			check:       func(t *testing.T, err error) { t.Helper(); require.NoError(t, err) },
			waitedFor:   []string{"open", "next"},
			joined:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fakeTransitions{begins: tt.begins, waits: tt.waits}
			ctx := joined.WithHolder(t.Context())
			transition, alreadyDone, err := StartRemoving(ctx, f, teamID, "sbx", tt.opts)

			tt.check(t, err)
			assert.Equal(t, tt.want, transition)
			assert.Equal(t, tt.alreadyDone, alreadyDone)
			assert.Empty(t, f.begins, "every begin is used")
			assert.Empty(t, f.waits, "every wait is used")
			var waits []waitCall
			for _, id := range tt.waitedFor {
				waits = append(waits, waitCall{teamID: teamID, sandboxID: "sbx", transitionID: id})
			}
			assert.Equal(t, waits, f.waitCalls)
			for _, opts := range f.beginOpts {
				assert.Equal(t, tt.opts, opts)
			}
			assert.Equal(t, attribute.Bool(joined.AttributeKey, tt.joined), joined.Attribute(ctx))
		})
	}
}

func TestStartRemoving_BeginsAgainWithTheCallersOpts(t *testing.T) {
	t.Parallel()

	opts := RemoveOpts{
		Action:             StateActionKill,
		Eviction:           true,
		Reason:             KillReasonRequest,
		FilesystemOnly:     true,
		RequireEnvdHealthy: true,
		ExpectExecutionID:  "execution",
		BeforeEndTime:      true,
	}
	f := &fakeTransitions{
		begins: []beginResult{
			{transition: StateTransition{Sandbox: Sandbox{State: StatePausing}, InFlightID: "open"}},
			{transition: StateTransition{ID: "started"}},
		},
		waits: []error{nil},
	}

	transition, _, err := StartRemoving(t.Context(), f, uuid.New(), "sbx", opts)
	require.NoError(t, err)
	assert.Equal(t, "started", transition.ID)
	require.Len(t, f.beginOpts, 2)
	for i, begun := range f.beginOpts {
		assert.Equal(t, opts, begun, "begin %d", i)
	}
}
