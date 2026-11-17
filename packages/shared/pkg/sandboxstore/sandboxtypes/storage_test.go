package sandboxtypes

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransitionOutcomeOf(t *testing.T) {
	t.Parallel()

	restored := &RestoredTransitionError{Refusal: PauseUnsupportedError{}}
	tests := []struct {
		name string
		err  error
		want TransitionOutcome
	}{
		{"nil", nil, TransitionOutcome{}},
		{"skip restore", ErrSkipRestore, TransitionOutcome{SkipRestore: true}},
		{"wrapped skip restore", fmt.Errorf("checkpoint: %w", ErrSkipRestore), TransitionOutcome{SkipRestore: true}},
		{"failure", errors.New("boom"), TransitionOutcome{Failed: true, Error: "boom"}},
		{"restored with a refusal", fmt.Errorf("pause: %w", restored), TransitionOutcome{
			Failed:       true,
			Error:        "pause: " + restored.Error(),
			Restored:     true,
			RefusalClass: "unsupported",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, TransitionOutcomeOf(tt.err))
		})
	}
}
