//go:build linux

package template

import (
	"context"
	"errors"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type LocalTemplate struct {
	paths storage.CachePaths

	memfile block.ReadonlyDevice
	rootfs  block.ReadonlyDevice
}

func NewLocalTemplate(
	paths storage.CachePaths,
	rootfs block.ReadonlyDevice,
	memfile block.ReadonlyDevice,
) *LocalTemplate {
	return &LocalTemplate{
		paths:   paths,
		memfile: memfile,
		rootfs:  rootfs,
	}
}

func (t *LocalTemplate) Close(ctx context.Context) error {
	return closeTemplate(ctx, t)
}

func (t *LocalTemplate) Files() storage.CachePaths {
	return t.paths
}

func (t *LocalTemplate) Memfile(ctx context.Context) (block.ReadonlyDevice, error) {
	_, span := tracer.Start(ctx, "local-template-memfile")
	defer span.End()

	return t.memfile, nil
}

func (t *LocalTemplate) Rootfs(context.Context) (block.ReadonlyDevice, error) {
	return t.rootfs, nil
}

func (t *LocalTemplate) Snapfile() (File, error) {
	return &NoopFile{}, errors.New("snapfile not available in local template")
}

func (t *LocalTemplate) Metadata() (metadata.Template, error) {
	return metadata.Template{}, errors.New("metadata not available in local template")
}

func (t *LocalTemplate) UpdateMetadata(_ metadata.Template) error {
	return errors.New("metadata not available in local template")
}
