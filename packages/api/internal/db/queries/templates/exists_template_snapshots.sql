-- name: ExistsTemplateSnapshots :one
SELECT EXISTS(
    SELECT 1
    FROM "public"."snapshots" s
    JOIN "public"."active_envs" e ON e.id = s.env_id
    WHERE s.base_env_id = @env_id
);