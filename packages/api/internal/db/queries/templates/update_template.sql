-- name: UpdateTemplate :one
UPDATE "public"."envs" e
SET public = @public
WHERE id IN (
    SELECT e.id FROM "public"."active_envs" e
    LEFT JOIN "public"."env_aliases" ea ON ea.env_id = e.id
    WHERE e.team_id = @team_id
    AND e.source = 'template'
    AND (e.id = @template_id_or_alias OR ea.alias = @template_id_or_alias)
)
RETURNING e.id;
