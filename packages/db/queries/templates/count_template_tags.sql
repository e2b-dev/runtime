-- name: CountTemplateTags :one
SELECT COUNT(DISTINCT eba.tag)::bigint AS total
FROM public.env_build_assignments eba
JOIN public.env_builds eb ON eb.id = eba.build_id
WHERE eba.env_id = sqlc.arg(template_id)::text
  AND eb.status_group = 'ready';
