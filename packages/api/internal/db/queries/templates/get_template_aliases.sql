-- name: GetTemplateAliases :many
SELECT
    ea.alias,
    ea.namespace,
    ea.env_id
FROM public.env_aliases ea
WHERE ea.env_id = ANY(sqlc.arg(env_ids)::text[]);
