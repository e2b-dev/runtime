-- name: AdminSearchTemplateAliases :many
-- Templates whose ID equals @search, or that own an alias equal to @search in
-- any namespace, with their owning team, paginated by template ID. Deleted
-- templates are left out: deleting a template releases its aliases.
SELECT
    e.id AS template_id,
    e.public,
    t.id AS team_id,
    t.name AS team_name,
    t.email AS team_email
FROM public.active_envs AS e
JOIN public.teams AS t ON t.id = e.team_id
WHERE
    (e.id = @search OR e.id IN (SELECT ea.env_id FROM public.env_aliases AS ea WHERE ea.alias = @search))
    AND e.id > @cursor_id
ORDER BY e.id
LIMIT sqlc.arg(limit_plus_one)::int;

-- name: AdminListTemplateAliases :many
-- The aliases of the given templates: aliases without a namespace first, then
-- by namespace and alias.
SELECT id, env_id, alias, namespace, is_renamable
FROM public.env_aliases
WHERE env_id = ANY(sqlc.arg(template_ids)::text[])
ORDER BY env_id, namespace NULLS FIRST, alias;

-- name: AdminCreateNakedTemplateAlias :one
-- Adds an alias without a namespace, which resolves for every team. Returns
-- no row when the alias is already taken.
INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace)
VALUES (@alias, @template_id, TRUE, NULL)
ON CONFLICT (alias, namespace) DO NOTHING
RETURNING id, env_id, alias, namespace, is_renamable;

-- name: AdminDeleteNakedTemplateAlias :one
-- Removes an alias without a namespace; a namespaced alias is not touched.
DELETE FROM public.env_aliases
WHERE id = @alias_id AND namespace IS NULL
RETURNING id, env_id, alias;

-- name: AdminCreateDefaultTemplate :one
-- Returns no row when the template is already a default template.
INSERT INTO public.env_defaults (env_id, description)
VALUES (@template_id, sqlc.narg(description))
ON CONFLICT (env_id) DO NOTHING
RETURNING env_id, description;

-- name: AdminUpdateDefaultTemplateDescription :one
UPDATE public.env_defaults
SET description = sqlc.narg(description)
WHERE env_id = @template_id
RETURNING env_id, description;

-- name: AdminDeleteDefaultTemplate :one
DELETE FROM public.env_defaults
WHERE env_id = @template_id
RETURNING env_id;

-- name: AdminGetTemplateOwner :one
-- The template's owning team and, for a snapshot template, the sandbox it
-- was taken from.
SELECT
    e.id AS template_id,
    t.id AS team_id,
    t.name AS team_name,
    t.email AS team_email,
    st.sandbox_id AS snapshot_sandbox_id
FROM public.active_envs AS e
JOIN public.teams AS t ON t.id = e.team_id
LEFT JOIN public.snapshot_templates AS st ON st.env_id = e.id
WHERE e.id = @template_id;

-- name: AdminGetTemplateIDByBuildID :one
-- The template a build belongs to, by the build's newest assignment.
SELECT eba.env_id
FROM public.env_build_assignments AS eba
JOIN public.active_envs AS e ON e.id = eba.env_id
WHERE eba.build_id = @build_id
ORDER BY eba.created_at DESC
LIMIT 1;
