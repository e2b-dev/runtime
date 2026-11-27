-- name: UpsertTemplateAliasIfNotExists :one
-- Attempts to create an alias. Returns the env_id that the alias points to.
-- If the alias already exists, returns the existing env_id without modifying it.
-- Uses ON CONFLICT DO NOTHING to avoid race conditions.
WITH inserted AS (
    INSERT INTO "public"."env_aliases" (alias, env_id, is_renamable, namespace)
    VALUES (@alias, @template_id, TRUE, sqlc.narg(namespace))
    ON CONFLICT (alias, namespace) DO NOTHING
    RETURNING env_id
)
SELECT env_id FROM inserted
UNION ALL
SELECT env_id FROM "public"."env_aliases" 
WHERE alias = @alias 
  AND namespace IS NOT DISTINCT FROM sqlc.narg(namespace)
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1;
