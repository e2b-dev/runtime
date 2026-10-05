-- name: UpdateTeam :one
UPDATE public.teams
SET
    name = CASE
        WHEN sqlc.arg(name_set)::bool THEN sqlc.narg(name)::text
        ELSE name
    END
WHERE id = sqlc.arg(team_id)::uuid
RETURNING id, name;
