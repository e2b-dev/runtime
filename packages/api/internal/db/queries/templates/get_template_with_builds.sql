-- name: GetTemplateBuilds :many
SELECT DISTINCT eb.*
FROM public.env_build_assignments eba
JOIN public.env_builds eb ON eb.id = eba.build_id
WHERE eba.env_id = sqlc.arg(template_id)
  AND (eb.created_at, eb.id::text) < (@cursor_time, @cursor_id::text)
ORDER BY eb.created_at DESC, eb.id DESC
LIMIT @build_limit;
