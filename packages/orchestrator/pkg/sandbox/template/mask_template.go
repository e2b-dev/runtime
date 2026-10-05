//go:build linux

package template

import (
	"context"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type MaskTemplate struct {
	template Template

	memfile  *block.ReadonlyDevice
	metadata *metadata.Template
}

type MaskTemplateOption func(*MaskTemplate)

func WithMemfile(memfile block.ReadonlyDevice) MaskTemplateOption {
	return func(c *MaskTemplate) {
		c.memfile = &memfile
	}
}

// WithMetadata replaces the template's metadata, e.g. to record what a reboot applied.
func WithMetadata(meta metadata.Template) MaskTemplateOption {
	return func(c *MaskTemplate) {
		c.metadata = &meta
	}
}

func NewMaskTemplate(
	template Template,
	opts ...MaskTemplateOption,
) *MaskTemplate {
	t := &MaskTemplate{
		template: template,
		memfile:  nil,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func (c *MaskTemplate) Close(_ context.Context) error {
	if c.memfile != nil {
		return (*c.memfile).Close()
	}

	return nil
}

func (c *MaskTemplate) Files() storage.CachePaths {
	return c.template.Files()
}

func (c *MaskTemplate) Memfile(ctx context.Context) (block.ReadonlyDevice, error) {
	ctx, span := tracer.Start(ctx, "mask-template-memfile")
	defer span.End()

	if c.memfile != nil {
		return *c.memfile, nil
	}

	return c.template.Memfile(ctx)
}

func (c *MaskTemplate) Rootfs() (block.ReadonlyDevice, error) {
	return c.template.Rootfs()
}

func (c *MaskTemplate) Snapfile() (File, error) {
	return c.template.Snapfile()
}

func (c *MaskTemplate) Metadata() (metadata.Template, error) {
	if c.metadata != nil {
		return *c.metadata, nil
	}

	return c.template.Metadata()
}

func (c *MaskTemplate) UpdateMetadata(meta metadata.Template) error {
	return c.template.UpdateMetadata(meta)
}
