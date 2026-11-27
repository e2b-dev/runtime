-- name: GetTemplateBuildWithTemplate :one
SELECT sqlc.embed(e), sqlc.embed(eb)
FROM "public"."active_envs" e
JOIN "public"."env_build_assignments" eba ON eba.env_id = e.id
JOIN "public"."env_builds" eb ON eb.id = eba.build_id
WHERE e.id = sqlc.arg(template_id) AND eb.id = sqlc.arg(build_id)
  AND e.source = 'template';
