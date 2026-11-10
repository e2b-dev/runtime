package handlers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// adminTemplateLookupBuilds is how many of a template's newest builds the
// admin lookup returns, the page size of the template detail's build list.
const adminTemplateLookupBuilds = int32(100)

func (a *APIStore) GetUnstableAdminTemplatesAliases(c *gin.Context, params api.GetUnstableAdminTemplatesAliasesParams) {
	ctx := c.Request.Context()

	search := strings.TrimSpace(params.Search)
	if search == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "search must not be blank")

		return
	}

	cursorID, err := parseAdminTemplatesCursor(params.NextToken)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid next token")

		return
	}

	limit := min(max(sharedUtils.DerefOrDefault(params.Limit, templatesDefaultLimit), 1), templatesMaxLimit)
	rows, err := a.sqlcDB.AdminSearchTemplateAliases(ctx, queries.AdminSearchTemplateAliasesParams{
		Search:       search,
		CursorID:     cursorID,
		LimitPlusOne: limit + 1,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when searching template aliases")
		telemetry.ReportCriticalError(ctx, "error when searching template aliases", err)

		return
	}

	if int32(len(rows)) > limit {
		rows = rows[:limit]
		c.Header("X-Next-Token", base64.URLEncoding.EncodeToString([]byte(rows[len(rows)-1].TemplateID)))
	}

	templateIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		templateIDs = append(templateIDs, row.TemplateID)
	}

	aliases, err := a.sqlcDB.AdminListTemplateAliases(ctx, templateIDs)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when listing template aliases")
		telemetry.ReportCriticalError(ctx, "error when listing template aliases", err)

		return
	}

	aliasesByTemplate := make(map[string][]api.AdminTemplateAlias, len(rows))
	for _, alias := range aliases {
		aliasesByTemplate[alias.EnvID] = append(aliasesByTemplate[alias.EnvID], api.AdminTemplateAlias{
			Id:          alias.ID,
			TemplateID:  alias.EnvID,
			Alias:       alias.Alias,
			Namespace:   alias.Namespace,
			IsRenamable: alias.IsRenamable,
		})
	}

	result := make([]api.AdminTemplateAliases, 0, len(rows))
	for _, row := range rows {
		templateAliases := aliasesByTemplate[row.TemplateID]
		if templateAliases == nil {
			templateAliases = []api.AdminTemplateAlias{}
		}

		result = append(result, api.AdminTemplateAliases{
			TemplateID: row.TemplateID,
			Public:     row.Public,
			Team: api.AdminTemplateTeam{
				Id:    row.TeamID,
				Name:  row.TeamName,
				Email: row.TeamEmail,
			},
			Aliases: templateAliases,
		})
	}

	c.JSON(http.StatusOK, result)
}

func (a *APIStore) PostUnstableAdminTemplatesAliases(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminTemplateAliasCreateRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	alias, err := parseNakedAlias(body.Alias)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return
	}

	templateID := strings.TrimSpace(body.TemplateID)
	if !a.requireActiveTemplate(c, templateID) {
		return
	}

	// The API resolves a name without a namespace as an alias before trying it
	// as a template ID, so an alias equal to a template ID would hide that
	// template.
	conflicts, err := a.sqlcDB.CheckAliasConflictsWithTemplateID(ctx, alias)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when checking the alias")
		telemetry.ReportCriticalError(ctx, "error when checking the alias against template IDs", err)

		return
	}
	if conflicts {
		a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Alias %q is the ID of a template", alias))

		return
	}

	created, err := a.sqlcDB.AdminCreateNakedTemplateAlias(ctx, queries.AdminCreateNakedTemplateAliasParams{
		Alias:      alias,
		TemplateID: templateID,
	})
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Alias %q is already taken", alias))

		return
	case dberrors.IsForeignKeyViolation(err):
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Template %s not found", templateID))

		return
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when adding the alias")
		telemetry.ReportCriticalError(ctx, "error when adding a template alias", err)

		return
	}

	a.invalidateNakedAlias(ctx, templateID, alias)

	c.JSON(http.StatusCreated, api.AdminTemplateAlias{
		Id:          created.ID,
		TemplateID:  created.EnvID,
		Alias:       created.Alias,
		Namespace:   created.Namespace,
		IsRenamable: created.IsRenamable,
	})
}

func (a *APIStore) DeleteUnstableAdminTemplatesAliasesAliasID(c *gin.Context, aliasID api.AliasID) {
	ctx := c.Request.Context()

	deleted, err := a.sqlcDB.AdminDeleteNakedTemplateAlias(ctx, aliasID)
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, "Alias without a namespace not found")

		return
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when removing the alias")
		telemetry.ReportCriticalError(ctx, "error when removing a template alias", err)

		return
	}

	a.invalidateNakedAlias(ctx, deleted.EnvID, deleted.Alias)

	c.JSON(http.StatusOK, api.AdminTemplateAliasDeleted{
		AliasID:    deleted.ID,
		TemplateID: deleted.EnvID,
		Alias:      deleted.Alias,
	})
}

func (a *APIStore) PostUnstableAdminTemplatesDefaults(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminDefaultTemplateCreateRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	templateID := strings.TrimSpace(body.TemplateID)
	if !a.requireActiveTemplate(c, templateID) {
		return
	}

	created, err := a.sqlcDB.AdminCreateDefaultTemplate(ctx, queries.AdminCreateDefaultTemplateParams{
		TemplateID:  templateID,
		Description: normalizeDefaultTemplateDescription(body.Description),
	})
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Template %s is already a default template", templateID))

		return
	case dberrors.IsForeignKeyViolation(err):
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Template %s not found", templateID))

		return
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when adding the default template")
		telemetry.ReportCriticalError(ctx, "error when adding a default template", err)

		return
	}

	c.JSON(http.StatusCreated, api.AdminDefaultTemplate{
		TemplateID:  created.EnvID,
		Description: created.Description,
	})
}

func (a *APIStore) PatchUnstableAdminTemplatesDefaultsTemplateID(c *gin.Context, templateID api.TemplateID) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminDefaultTemplateUpdateRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	updated, err := a.sqlcDB.AdminUpdateDefaultTemplateDescription(ctx, queries.AdminUpdateDefaultTemplateDescriptionParams{
		TemplateID:  templateID,
		Description: normalizeDefaultTemplateDescription(body.Description),
	})
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Default template %s not found", templateID))

		return
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when updating the default template")
		telemetry.ReportCriticalError(ctx, "error when updating a default template", err)

		return
	}

	c.JSON(http.StatusOK, api.AdminDefaultTemplate{
		TemplateID:  updated.EnvID,
		Description: updated.Description,
	})
}

func (a *APIStore) DeleteUnstableAdminTemplatesDefaultsTemplateID(c *gin.Context, templateID api.TemplateID) {
	ctx := c.Request.Context()

	_, err := a.sqlcDB.AdminDeleteDefaultTemplate(ctx, templateID)
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Default template %s not found", templateID))

		return
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when removing the default template")
		telemetry.ReportCriticalError(ctx, "error when removing a default template", err)

		return
	}

	c.Status(http.StatusNoContent)
}

func (a *APIStore) GetUnstableAdminTemplatesLookup(c *gin.Context, params api.GetUnstableAdminTemplatesLookupParams) {
	ctx := c.Request.Context()

	query := strings.TrimSpace(params.Query)
	if query == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "query must not be blank")

		return
	}

	templateID, matchedBuildID, err := a.resolveAdminTemplateQuery(ctx, query)
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, "No template found for this query")

		return
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when looking up the template")
		telemetry.ReportCriticalError(ctx, "error when looking up a template", err)

		return
	}

	owner, err := a.sqlcDB.AdminGetTemplateOwner(ctx, templateID)
	if err != nil {
		a.respondAdminLookupError(c, err)

		return
	}

	template, err := a.sqlcDB.GetTemplateByIDWithAliases(ctx, templateID)
	if err != nil {
		a.respondAdminLookupError(c, err)

		return
	}

	builds, err := a.sqlcDB.GetTemplateBuilds(ctx, queries.GetTemplateBuildsParams{
		TemplateID: templateID,
		CursorTime: maxTemplatesCursorTime,
		CursorID:   maxBuildID,
		BuildLimit: adminTemplateLookupBuilds,
	})
	if err != nil {
		a.respondAdminLookupError(c, err)

		return
	}

	result := api.AdminTemplateLookup{
		TemplateID: template.ID,
		Team: api.AdminTemplateTeam{
			Id:    owner.TeamID,
			Name:  owner.TeamName,
			Email: owner.TeamEmail,
		},
		Names:             template.Names,
		Public:            template.Public,
		CreatedAt:         template.CreatedAt,
		LastSpawnedAt:     template.LastSpawnedAt,
		SpawnCount:        template.SpawnCount,
		IsSnapshot:        owner.SnapshotSandboxID != nil,
		SnapshotSandboxID: owner.SnapshotSandboxID,
		MatchedBuildID:    matchedBuildID,
		Builds:            make([]api.TemplateBuild, 0, len(builds)),
	}
	if result.Names == nil {
		result.Names = []string{}
	}

	for _, build := range builds {
		result.Builds = append(result.Builds, templateBuildToAPI(ctx, build))
	}

	c.JSON(http.StatusOK, result)
}

// resolveAdminTemplateQuery finds the template a lookup query names: a build
// ID (a UUID, with no fallback when no template has the build), else a
// template ID, else a template name, namespace/alias or an alias without a
// namespace. A query that matches nothing returns a not-found error.
func (a *APIStore) resolveAdminTemplateQuery(ctx context.Context, query string) (string, *uuid.UUID, error) {
	if buildID, err := uuid.Parse(query); err == nil && len(query) == len(uuid.Nil.String()) {
		templateID, err := a.sqlcDB.AdminGetTemplateIDByBuildID(ctx, buildID)
		if err != nil {
			return "", nil, err
		}

		return templateID, &buildID, nil
	}

	template, err := a.sqlcDB.GetTemplateById(ctx, query)
	if err == nil {
		return template.ID, nil, nil
	}
	if !dberrors.IsNotFoundError(err) {
		return "", nil, err
	}

	params := queries.GetTemplateByAliasParams{Alias: query}
	if namespace, alias, found := strings.Cut(query, id.NamespaceSeparator); found && namespace != "" {
		params = queries.GetTemplateByAliasParams{Alias: alias, Namespace: &namespace}
	} else if found {
		params.Alias = alias
	}

	byAlias, err := a.sqlcDB.GetTemplateByAlias(ctx, params)
	if err != nil {
		return "", nil, err
	}

	return byAlias.ID, nil, nil
}

func (a *APIStore) respondAdminLookupError(c *gin.Context, err error) {
	if dberrors.IsNotFoundError(err) {
		// The template was deleted between resolving the query and reading it.
		a.sendAPIStoreError(c, http.StatusNotFound, "No template found for this query")

		return
	}

	a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when looking up the template")
	telemetry.ReportCriticalError(c.Request.Context(), "error when looking up a template", err)
}

// requireActiveTemplate answers 404 for a template that does not exist or is
// deleted.
func (a *APIStore) requireActiveTemplate(c *gin.Context, templateID string) bool {
	ctx := c.Request.Context()

	_, err := a.sqlcDB.GetTemplateById(ctx, templateID)
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Template %s not found", templateID))

		return false
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting the template")
		telemetry.ReportCriticalError(ctx, "error when getting a template", err)

		return false
	}

	return true
}

// invalidateNakedAlias clears what the API caches about an alias without a
// namespace after it was added or removed: the alias lookup, including a
// cached miss, and the template's cached aliases and names.
func (a *APIStore) invalidateNakedAlias(ctx context.Context, templateID, alias string) {
	ctx = context.WithoutCancel(ctx)

	a.templateCache.InvalidateAlias(ctx, nil, alias)
	a.templateCache.InvalidateAllTags(ctx, templateID)

	logger.L().Info(ctx, "Invalidated template alias caches", logger.WithTemplateID(templateID), zap.String("alias", alias))
}

// parseNakedAlias validates an alias without a namespace by the rules of
// template names and returns it normalized (trimmed, lowercase).
func parseNakedAlias(value string) (string, error) {
	if strings.Contains(value, id.NamespaceSeparator) || strings.Contains(value, id.TagSeparator) {
		return "", fmt.Errorf("alias %q must not have a namespace or a tag", value)
	}

	alias, _, err := id.ParseName(value)
	if err != nil {
		return "", fmt.Errorf("invalid alias %q: lowercase letters, digits, '-' and '_' only", value)
	}

	return alias, nil
}

func normalizeDefaultTemplateDescription(description *string) *string {
	if description == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*description)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}

// parseAdminTemplatesCursor decodes the alias search's next token, the last
// template ID of the previous page. No token means the first page.
func parseAdminTemplatesCursor(token *api.PaginationNextToken) (string, error) {
	if token == nil || *token == "" {
		return "", nil
	}

	decoded, err := base64.URLEncoding.DecodeString(*token)
	if err != nil {
		return "", fmt.Errorf("invalid next token: %w", err)
	}
	if len(decoded) == 0 {
		return "", errors.New("invalid next token: empty")
	}

	return string(decoded), nil
}
