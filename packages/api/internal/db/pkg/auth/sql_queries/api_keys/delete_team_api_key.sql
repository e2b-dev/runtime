-- name: DeleteTeamAPIKey :many
DELETE FROM "public"."team_api_keys"
WHERE id = @id AND team_id = @team_id
RETURNING api_key_hash;
