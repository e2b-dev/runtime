-- name: GetTeamAPIKeyHashes :many
SELECT tak.api_key_hash
FROM "public"."team_api_keys" tak
WHERE tak.team_id = @team_id;