package handlers

import (
	"context"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// templateRow is a row of a template list. A query that lists templates
// selects these columns, in this order, so that its generated row type
// converts to this one.
type templateRow = queries.ListDefaultTemplatesRow

// templateToAPI maps a template list row. Without usage, the template's usage
// fields (spawn and build counts, last spawn, creator) stay zero or empty: a
// caller sees them for its own templates only, and admins for default ones.
func templateToAPI(ctx context.Context, row templateRow, withUsage bool) api.Template {
	envdVersion := ""
	if row.BuildEnvdVersion != nil {
		envdVersion = *row.BuildEnvdVersion
	}

	diskMB := int64(0)
	if row.BuildTotalDiskSizeMb != nil {
		diskMB = *row.BuildTotalDiskSizeMb
	}

	template := api.Template{
		TemplateID:  row.TemplateID,
		BuildID:     row.BuildID.String(),
		CpuCount:    api.CPUCount(row.BuildVcpu),
		MaxCpuCount: sharedUtils.CastPtr(row.BuildMaxVcpu, func(v int64) api.CPUCount { return api.CPUCount(v) }),
		MemoryMB:    api.MemoryMB(row.BuildRamMb),
		DiskSizeMB:  api.DiskSizeMB(diskMB),
		Public:      row.Public,
		Aliases:     row.Aliases,
		Names:       row.Names,
		CreatedAt:   row.CreatedAt,
		UpdatedAt:   row.UpdatedAt,
		BuildStatus: getCorrespondingTemplateBuildStatus(ctx, row.BuildStatus),
		EnvdVersion: envdVersion,
		IsDefault:   &row.IsDefault,
		Description: row.DefaultDescription,
	}

	if withUsage {
		template.LastSpawnedAt = row.LastSpawnedAt
		template.SpawnCount = row.SpawnCount
		template.BuildCount = row.BuildCount
		if row.CreatorID != nil {
			template.CreatedBy = &api.TeamUser{Id: *row.CreatorID}
		}
	}

	return template
}

// templateWithBuildsToAPI maps a template the way the template lists do, with
// its builds.
func templateWithBuildsToAPI(ctx context.Context, row templateRow, builds []queries.EnvBuild) api.TemplateWithBuilds {
	apiBuilds := make([]api.TemplateBuild, 0, len(builds))
	for _, build := range builds {
		apiBuilds = append(apiBuilds, templateBuildToAPI(ctx, build))
	}

	return templateWithBuilds(templateToAPI(ctx, row, true), apiBuilds)
}

// templateWithBuilds is a template list item with builds. TemplateWithBuilds
// has every field of Template; TestTemplateWithBuildsCopiesEveryTemplateField
// fails when one is not copied.
func templateWithBuilds(template api.Template, builds []api.TemplateBuild) api.TemplateWithBuilds {
	return api.TemplateWithBuilds{
		TemplateID:    template.TemplateID,
		BuildID:       template.BuildID,
		CpuCount:      template.CpuCount,
		MaxCpuCount:   template.MaxCpuCount,
		MemoryMB:      template.MemoryMB,
		DiskSizeMB:    template.DiskSizeMB,
		Public:        template.Public,
		Aliases:       template.Aliases,
		Names:         template.Names,
		CreatedAt:     template.CreatedAt,
		UpdatedAt:     template.UpdatedAt,
		CreatedBy:     template.CreatedBy,
		LastSpawnedAt: template.LastSpawnedAt,
		SpawnCount:    template.SpawnCount,
		BuildCount:    template.BuildCount,
		EnvdVersion:   template.EnvdVersion,
		BuildStatus:   template.BuildStatus,
		IsDefault:     template.IsDefault,
		Description:   template.Description,
		Builds:        builds,
	}
}
