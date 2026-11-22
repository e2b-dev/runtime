-- name: UpdateTemplateSpawnCount :exec
UPDATE "public"."envs"
SET spawn_count = spawn_count + @spawn_count, last_spawned_at = @last_spawned_at
WHERE id = @template_id;
