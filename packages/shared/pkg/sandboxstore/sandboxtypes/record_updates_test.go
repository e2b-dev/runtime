package sandboxtypes

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fakeToken UpdateToken = "token"

// fakeRecordUpdates records the calls UpdateRecord makes.
type fakeRecordUpdates struct {
	record   Sandbox
	beginErr error

	commits   []Sandbox
	committed []UpdateToken
	aborted   []UpdateToken
}

func (f *fakeRecordUpdates) BeginUpdate(context.Context, uuid.UUID, string) (Sandbox, UpdateToken, error) {
	if f.beginErr != nil {
		return Sandbox{}, "", f.beginErr
	}

	return f.record, fakeToken, nil
}

func (f *fakeRecordUpdates) CommitUpdate(_ context.Context, _ uuid.UUID, _ string, token UpdateToken, updated Sandbox) (Sandbox, error) {
	f.committed = append(f.committed, token)
	f.commits = append(f.commits, updated)

	return updated, nil
}

func (f *fakeRecordUpdates) AbortUpdate(_ context.Context, _ uuid.UUID, _ string, token UpdateToken) error {
	f.aborted = append(f.aborted, token)

	return nil
}

func TestUpdateRecord(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()

	t.Run("commits the function's result", func(t *testing.T) {
		t.Parallel()

		f := &fakeRecordUpdates{record: Sandbox{SandboxID: "sbx", TemplateID: "before"}}
		got, err := UpdateRecord(t.Context(), f, teamID, "sbx", func(sbx Sandbox) (Sandbox, error) {
			assert.Equal(t, "before", sbx.TemplateID)
			sbx.TemplateID = "after"

			return sbx, nil
		})
		require.NoError(t, err)
		assert.Equal(t, "after", got.TemplateID)
		assert.Equal(t, []UpdateToken{fakeToken}, f.committed)
		assert.Equal(t, []Sandbox{got}, f.commits)
		assert.Empty(t, f.aborted)
	})

	t.Run("aborts on the function's error", func(t *testing.T) {
		t.Parallel()

		f := &fakeRecordUpdates{}
		fnErr := errors.New("refused")
		_, err := UpdateRecord(t.Context(), f, teamID, "sbx", func(Sandbox) (Sandbox, error) {
			return Sandbox{}, fnErr
		})
		require.ErrorIs(t, err, fnErr)
		require.EqualError(t, err, "failed to update sandbox: refused")
		assert.Equal(t, []UpdateToken{fakeToken}, f.aborted)
		assert.Empty(t, f.committed)
	})

	t.Run("a failed begin skips the function", func(t *testing.T) {
		t.Parallel()

		f := &fakeRecordUpdates{beginErr: ErrLifecycleHeld}
		called := false
		_, err := UpdateRecord(t.Context(), f, teamID, "sbx", func(sbx Sandbox) (Sandbox, error) {
			called = true

			return sbx, nil
		})
		require.ErrorIs(t, err, ErrLifecycleHeld)
		assert.False(t, called)
		assert.Empty(t, f.committed)
		assert.Empty(t, f.aborted)
	})

	t.Run("aborts on the function's panic and lets it through", func(t *testing.T) {
		t.Parallel()

		f := &fakeRecordUpdates{}
		assert.PanicsWithValue(t, "boom", func() {
			_, _ = UpdateRecord(t.Context(), f, teamID, "sbx", func(Sandbox) (Sandbox, error) {
				panic("boom")
			})
		})
		assert.Equal(t, []UpdateToken{fakeToken}, f.aborted)
		assert.Empty(t, f.committed)
	})
}
