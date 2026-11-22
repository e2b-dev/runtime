//go:build unix

package handler

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The OOM score prefix must only raise the child's score: writing a lower
// value is denied to an unprivileged process and would abort the wrapper
// before exec, failing every command in a container whose score was raised
// above the target by the runtime (e.g. a Kubernetes Burstable pod).
func TestOomScoreAdjPrefix(t *testing.T) {
	t.Parallel()

	const raise = "echo 100 > /proc/$$/oom_score_adj && "

	tests := []struct {
		name string
		read func() (int, error)
		want string
	}{
		{"protected envd score is raised", fixedOomScore(-1000), raise},
		{"default score is raised", fixedOomScore(0), raise},
		{"just below target is raised", fixedOomScore(defaultOomScore - 1), raise},
		{"target is kept", fixedOomScore(defaultOomScore), ""},
		{"score above target is kept", fixedOomScore(993), ""},
		{"unreadable score keeps the unconditional write", failedOomScore, raise},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, oomScoreAdjPrefix(defaultOomScore, tt.read))
		})
	}
}

func fixedOomScore(value int) func() (int, error) {
	return func() (int, error) { return value, nil }
}

func failedOomScore() (int, error) {
	return 0, errors.New("unreadable")
}
