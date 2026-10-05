-- name: GetTeamClusterForTemplateBuild :one
SELECT cluster_id FROM public.teams
WHERE id = @team_id
FOR SHARE;

-- name: GetTemplateForAliasRebuild :one
SELECT id, cluster_id, public FROM public.envs
WHERE id = @template_id AND team_id = @team_id
  AND deleted_at IS NULL AND source = 'template'
FOR SHARE;

-- name: ReplaceTemplateAlias :execrows
UPDATE public.env_aliases
SET env_id = @template_id
WHERE alias = @alias
  AND namespace = @namespace
  AND env_id = @previous_template_id;
