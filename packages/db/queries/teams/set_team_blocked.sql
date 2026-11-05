-- name: SetTeamBlocked :execrows
UPDATE public.teams
SET
    is_blocked = sqlc.arg(is_blocked)::boolean,
    blocked_reason = sqlc.narg(blocked_reason)::text
WHERE id = sqlc.arg(team_id)::uuid;
