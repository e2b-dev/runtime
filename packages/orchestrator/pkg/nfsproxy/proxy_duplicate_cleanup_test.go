//go:build linux

package nfsproxy

import (
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	servernfs "github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot"
)

type sequencedConcernAllocation struct {
	fs     *chrooted.Chrooted
	resume chan struct{}
}

type sequencedConcernBuilder struct {
	inner   *chrooted.Builder
	entered chan sequencedConcernAllocation
}

func (b *sequencedConcernBuilder) Chroot(typ string, team, id uuid.UUID) (*chrooted.Chrooted, error) {
	fs, err := b.inner.Chroot(typ, team, id)
	if err != nil {
		return nil, err
	}
	resume := make(chan struct{}, 1)
	b.entered <- sequencedConcernAllocation{fs: fs, resume: resume}
	<-resume

	return fs, nil
}

func TestConcurrentMountKeepsCanonicalRootOnDuplicateCloseError(t *testing.T) { //nolint:paralleltest // actual root-FD barriers and process-wide telemetry
	entered := make(chan sequencedConcernAllocation, 2)
	f := newConcernFixture(t, nfscfg.Config{Metrics: true}, func(inner *chrooted.Builder) chroot.Builder {
		return &sequencedConcernBuilder{inner: inner, entered: entered}
	})
	type result struct {
		status servernfs.MountStatus
		fs     billy.Filesystem
	}
	startMount := func() <-chan result {
		finished := make(chan result, 1)
		go func() {
			status, fs, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("first"))})
			finished <- result{status: status, fs: fs}
		}()

		return finished
	}
	allocation := func() sequencedConcernAllocation {
		t.Helper()
		select {
		case a := <-entered:
			t.Cleanup(func() { close(a.resume) })

			return a
		case <-time.After(time.Second):
			t.Fatal("MOUNT must reach the actual root allocation before registration")

			return sequencedConcernAllocation{}
		}
	}
	finish := func(done <-chan result) result {
		t.Helper()
		select {
		case r := <-done:
			return r
		case <-time.After(time.Second):
			t.Fatal("MOUNT did not finish after its allocation was released")

			return result{}
		}
	}

	first := startMount()
	canonical := allocation()
	second := startMount()
	duplicate := allocation()
	require.Equal(t, 2, concernRootFDCount(t, f.paths[0]))

	canonical.resume <- struct{}{}
	firstResult := finish(first)
	require.Equal(t, servernfs.MountStatusOk, firstResult.status)
	root := f.proxy.server.Handler.ToHandle(t.Context(), firstResult.fs, nil)
	require.NotEmpty(t, root)

	// Inject a real Close error without changing production syscall hooks:
	// closing the unused allocation now makes its cleanup return ErrClosed.
	// Both MOUNTs allocated before the canonical one registered.
	require.NoError(t, duplicate.fs.Close())
	duplicate.resume <- struct{}{}
	secondResult := finish(second)
	require.Equal(t, servernfs.MountStatusOk, secondResult.status,
		"cleanup failure on an unused duplicate must not deny a usable canonical root")
	assert.Equal(t, root, f.proxy.server.Handler.ToHandle(t.Context(), secondResult.fs, nil))
	_, err := canonical.fs.Stat(".")
	require.NoError(t, err, "the canonical root must remain usable")
	require.Equal(t, 1, concernRootFDCount(t, f.paths[0]))
	require.NoError(t, f.sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))
	require.Eventually(t, func() bool { return f.proxy.retirement.count() == 0 }, time.Second, time.Millisecond)
	assert.Zero(t, concernRootFDCount(t, f.paths[0]), "owner release must close the surviving root")
}
