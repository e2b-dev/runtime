//go:build linux

package sandbox

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
)

// mappedMemory is a memory backend that holds a mapped guest memory file.
type mappedMemory struct{ uffd.MemoryBackend }

func (mappedMemory) PeekMemfd(context.Context) *block.Memfd { return &block.Memfd{} }

// A hugepage start stops counting toward the node's admission once its guest
// memory is reported reserved, so the report must come only when the pool
// shows that memory and no later boot can take more without reserving it.
func TestGuestMemoryReservedReport(t *testing.T) {
	t.Parallel()

	bootAgain := WithRebootOnGuestReset(func(context.Context, GuestRebootResult, time.Time) {})

	for name, test := range map[string]struct {
		memory  uffd.MemoryBackend
		opts    []ResumeOption
		reports bool
	}{
		"guest memory file mapped":             {memory: mappedMemory{}, reports: true},
		"no guest memory file":                 {memory: uffd.NewNoopMemory(0, 0)},
		"mapped, but the guest may boot again": {memory: mappedMemory{}, opts: []ResumeOption{bootAgain}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reported := false
			var o resumeOptions
			for _, opt := range append(test.opts, WithGuestMemoryReserved(func() { reported = true })) {
				opt(&o)
			}

			o.reportGuestMemoryReserved(t.Context(), test.memory)
			assert.Equal(t, test.reports, reported)
		})
	}
}
