package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// adminResolveMaxNames is the most names one resolve call takes, the spec's
// maxItems.
const adminResolveMaxNames = 200

// PostUnstableAdminTemplatesAliasesResolve reports, for each name, the
// template that holds it as an alias without a namespace and the one that
// holds it in the requested namespace, with every alias of each template.
// Two indexed queries: one by alias, one by the templates it found.
func (a *APIStore) PostUnstableAdminTemplatesAliasesResolve(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminTemplateAliasResolveRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	if err := validateAliasResolveRequest(body); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return
	}

	rows, err := a.sqlcDB.AdminResolveTemplateAliases(ctx, queries.AdminResolveTemplateAliasesParams{
		Names:     body.Names,
		Namespace: body.Namespace,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when resolving template aliases")
		telemetry.ReportCriticalError(ctx, "error when resolving template aliases", err)

		return
	}

	aliases, err := a.sqlcDB.AdminListAliasesOfTemplates(ctx, resolvedTemplateIDs(rows))
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when resolving template aliases")
		telemetry.ReportCriticalError(ctx, "error when listing aliases of resolved templates", err)

		return
	}

	c.JSON(http.StatusOK, aliasResolutions(body.Names, rows, aliases))
}

// resolvedTemplateIDs lists each template found once, at most two per name.
func resolvedTemplateIDs(rows []queries.AdminResolveTemplateAliasesRow) []string {
	seen := make(map[string]struct{}, len(rows))
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.TemplateID]; ok {
			continue
		}
		seen[row.TemplateID] = struct{}{}
		ids = append(ids, row.TemplateID)
	}

	return ids
}

func validateAliasResolveRequest(body api.AdminTemplateAliasResolveRequest) error {
	if strings.TrimSpace(body.Namespace) == "" {
		return errors.New("namespace must not be blank")
	}
	if len(body.Names) == 0 {
		return errors.New("names must not be empty")
	}
	if len(body.Names) > adminResolveMaxNames {
		return fmt.Errorf("names takes at most %d entries", adminResolveMaxNames)
	}

	seen := make(map[string]struct{}, len(body.Names))
	for _, name := range body.Names {
		if strings.TrimSpace(name) == "" {
			return errors.New("names must not contain a blank name")
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("names lists %q twice", name)
		}
		seen[name] = struct{}{}
	}

	return nil
}

// aliasResolutions answers every requested name in request order. The unique
// (alias, namespace) index allows at most one row per name and place, so a
// row never overwrites another.
func aliasResolutions(
	names []string,
	rows []queries.AdminResolveTemplateAliasesRow,
	aliases []queries.AdminListAliasesOfTemplatesRow,
) api.AdminTemplateAliasResolveResponse {
	aliasesOf := make(map[string][]api.AdminResolvedTemplateAlias, len(rows))
	for _, alias := range aliases {
		aliasesOf[alias.TemplateID] = append(aliasesOf[alias.TemplateID], api.AdminResolvedTemplateAlias{
			Alias:     alias.Alias,
			Namespace: alias.Namespace,
		})
	}

	byName := make(map[string]*api.AdminTemplateAliasResolution, len(names))
	results := make([]api.AdminTemplateAliasResolution, len(names))
	for i, name := range names {
		results[i] = api.AdminTemplateAliasResolution{Name: name}
		byName[name] = &results[i]
	}

	for _, row := range rows {
		result, ok := byName[row.Alias]
		if !ok {
			continue
		}

		templateAliases := aliasesOf[row.TemplateID]
		if templateAliases == nil {
			templateAliases = []api.AdminResolvedTemplateAlias{}
		}
		template := &api.AdminResolvedTemplate{TemplateID: row.TemplateID, Public: row.Public, Aliases: &templateAliases}
		if row.Namespace == nil {
			result.Naked = template
		} else {
			result.Namespaced = template
		}
	}

	return api.AdminTemplateAliasResolveResponse{Results: results}
}
