-- name: SetTeamBanned :execrows
UPDATE public.teams
SET is_banned = sqlc.arg(is_banned)::boolean
WHERE id = sqlc.arg(team_id)::uuid;
