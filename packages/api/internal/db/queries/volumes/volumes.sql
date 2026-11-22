-- name: CreateVolume :one
INSERT INTO volumes (id, team_id, volume_type, name)
VALUES (@id, @team_id, @volume_type, @name)
RETURNING *;

-- name: GetVolume :one
SELECT * FROM volumes WHERE id = @volume_id AND team_id = @team_id;

-- name: GetVolumesByName :many
SELECT * FROM volumes WHERE team_id = @team_id AND name IN (
    SELECT UNNEST(@volume_names::text[])
);

-- name: FindVolumesByTeamID :many
SELECT * FROM volumes WHERE team_id = @team_id;

-- name: DeleteVolume :exec
DELETE FROM volumes WHERE team_id = @team_id AND id = @volume_id;
