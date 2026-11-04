//go:build linux

package resumebuild

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/rootfs"
)

type flushingProfile struct{ guest.Profile }

func (flushingProfile) Template() guest.TemplateBuild {
	return guest.TemplateBuild{SyncCommand: "flush-volumes"}
}

// A pause flushes with the guest profile's own command when it names one.
func TestPauseFlush(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []flushStep{
		{name: "sync", command: rootfs.SandboxBusyBoxPath + " sync"},
		{name: "drop_caches", command: "echo 3 > /proc/sys/vm/drop_caches"},
	}, pauseFlush(guest.Default()), "the default guest syncs and drops its page cache, as before")
	assert.Equal(t, []flushStep{{name: "sync", command: "flush-volumes"}}, pauseFlush(flushingProfile{guest.Default()}))
}
