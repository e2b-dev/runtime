//go:build linux

package userfaultfd

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// Every page of the registered guest memory lands in exactly one state, and
// the pages the tracker has not seen count as not present.
func TestPageStateBytes(t *testing.T) {
	t.Parallel()

	const page = header.PageSize
	tracker := block.NewTracker()
	u := &Userfaultfd{
		pageSize:    page,
		pageTracker: tracker,
		ma: memory.NewMapping([]memory.Region{
			{BaseHostVirtAddr: 0x10000000, Size: 8 * page, Offset: 0, PageSize: page},
			{BaseHostVirtAddr: 0x20000000, Size: 8 * page, Offset: 8 * page, PageSize: page},
		}),
	}

	tracker.BeginInstall(0)
	tracker.MarkInstalled(1, 3, block.Clean)
	tracker.MarkInstalled(3, 4, block.Zero)
	tracker.SetRange(4, 7, block.Dirty)
	tracker.MarkWritten(1)
	tracker.SetRange(7, 9, block.Removed)

	var want [block.NumStates]int64
	want[block.Installing] = 1 * page
	want[block.Clean] = 1 * page
	want[block.Zero] = 1 * page
	want[block.Dirty] = 4 * page
	want[block.Removed] = 2 * page
	want[block.NotPresent] = 7 * page
	assert.Equal(t, want, u.PageStateBytes())
}
