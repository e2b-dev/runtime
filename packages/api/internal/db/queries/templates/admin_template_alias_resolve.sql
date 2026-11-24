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
