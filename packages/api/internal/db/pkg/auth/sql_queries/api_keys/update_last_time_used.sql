-- name: UpdateLastTimeUsed :exec
UPDATE "public"."team_api_keys" tak
SET last_used = now()
WHERE tak.api_key_hash = $1;
