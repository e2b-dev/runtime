-- name: AdminResolveTemplateAliases :many
-- The templates holding any of @names as an alias without a namespace or in
-- @namespace. At most two rows per name, both found through the unique
-- (alias, namespace) index. Deleted templates are left out.
SELECT
    ea.alias,
    ea.namespace,
    e.id AS template_id,
    e.public
FROM public.env_aliases AS ea
JOIN public.active_envs AS e ON e.id = ea.env_id
WHERE
    ea.alias = ANY(sqlc.arg(names)::text[])
    AND (ea.namespace IS NULL OR ea.namespace = sqlc.arg(namespace)::text);

-- name: AdminListAliasesOfTemplates :many
-- Every alias of the templates in @template_ids, in any namespace, found
-- through the index on env_id.
SELECT
    ea.env_id AS template_id,
    ea.alias,
    ea.namespace
FROM public.env_aliases AS ea
WHERE ea.env_id = ANY(sqlc.arg(template_ids)::text[])
ORDER BY ea.env_id, ea.namespace NULLS FIRST, ea.alias;
