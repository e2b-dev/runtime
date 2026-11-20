//go:build linux

package template

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type Template interface {
	Files() storage.CachePaths
	Memfile(ctx context.Context) (block.ReadonlyDevice, error)
	Rootfs(ctx context.Context) (block.ReadonlyDevice, error)
	Snapfile() (File, error)
	Metadata() (metadata.Template, error)
	UpdateMetadata(meta metadata.Template) error
	Close(ctx context.Context) error
}

// closeTemplate closes the template's memfile, rootfs and snapfile. It waits
// for each under a context that cannot be cancelled: a fetch still in flight
// must be joined before the caller removes the directory it writes into, and
// a device that lands after an early return would never be closed.
func closeTemplate(ctx context.Context, t Template) (e error) {
	ctx = context.WithoutCancel(ctx)
	closable := make([]io.Closer, 0)

	memfile, err := t.Memfile(ctx)
	if err != nil {
		e = errors.Join(e, err)
	} else {
		closable = append(closable, memfile)
	}

	rootfs, err := t.Rootfs(ctx)
	if err != nil {
		e = errors.Join(e, err)
	} else {
		closable = append(closable, rootfs)
	}

	snapfile, err := t.Snapfile()
	if err != nil {
		e = errors.Join(e, err)
	} else {
		closable = append(closable, snapfile)
	}

	for _, c := range closable {
		if err := c.Close(); err != nil {
			e = errors.Join(e, err)
		}
	}

	if e != nil {
		return fmt.Errorf("error closing template: %w", e)
	}

	return nil
}

type NoopFile struct{}

func (n *NoopFile) Close() error {
	return nil
}

func (n *NoopFile) Path() string {
	return "/dev/null"
}
