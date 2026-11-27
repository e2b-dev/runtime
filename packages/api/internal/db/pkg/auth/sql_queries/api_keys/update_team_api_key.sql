-- name: UpdateTeamApiKey :one
UPDATE "public"."team_api_keys" SET name = @name , updated_at = @updated_at WHERE id = @id AND team_id = @team_id
RETURNING id;