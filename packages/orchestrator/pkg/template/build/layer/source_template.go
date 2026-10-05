//go:build linux

package layer

import (
	"context"
	"fmt"

	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
)

var _ SourceTemplateProvider = (*CacheSourceTemplateProvider)(nil)

type CacheSourceTemplateProvider struct {
	buildID string
}

func NewCacheSourceTemplateProvider(
	buildID string,
) *CacheSourceTemplateProvider {
	return &CacheSourceTemplateProvider{
		buildID: buildID,
	}
}

func (cstp *CacheSourceTemplateProvider) Get(ctx context.Context, templateCache *sbxtemplate.Cache) (sbxtemplate.Template, func(), error) {
	template, release, err := templateCache.GetTemplatePinned(
		ctx,
		cstp.buildID,
		false,
		true,
	)
	if err != nil {
		return nil, release, fmt.Errorf("get template snapshot: %w", err)
	}

	return template, release, nil
}

var _ SourceTemplateProvider = (*DirectSourceTemplateProvider)(nil)

type DirectSourceTemplateProvider struct {
	SourceTemplate sbxtemplate.Template
}

func NewDirectSourceTemplateProvider(template sbxtemplate.Template) *DirectSourceTemplateProvider {
	return &DirectSourceTemplateProvider{SourceTemplate: template}
}

func (dstp *DirectSourceTemplateProvider) Get(_ context.Context, _ *sbxtemplate.Cache) (sbxtemplate.Template, func(), error) {
	return dstp.SourceTemplate, func() {}, nil
}
