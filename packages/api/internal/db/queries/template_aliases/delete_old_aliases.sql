-- name: DeleteOtherTemplateAliases :many
DELETE FROM "public"."env_aliases"
WHERE env_id = $1
  AND is_renamable = TRUE
RETURNING CASE
    WHEN namespace IS NOT NULL THEN namespace || '/' || alias
    ELSE alias
  END::text AS alias_key;