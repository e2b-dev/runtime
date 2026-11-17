-- name: UpsertTeamMember :exec
WITH cleared_default AS (
    UPDATE public.users_teams
    SET is_default = false
    WHERE user_id = sqlc.arg(user_id)::uuid
      AND team_id <> sqlc.arg(team_id)::uuid
      AND is_default
      AND sqlc.arg(is_default)::boolean
    RETURNING 1
)
INSERT INTO public.users_teams (user_id, team_id, is_default, added_by, role)
SELECT
    sqlc.arg(user_id)::uuid,
    sqlc.arg(team_id)::uuid,
    sqlc.arg(is_default)::boolean,
    sqlc.narg(added_by)::uuid,
    COALESCE(sqlc.narg(role)::text, 'member')
WHERE (SELECT count(*) FROM cleared_default) >= 0
ON CONFLICT (team_id, user_id) DO UPDATE
SET
    is_default = EXCLUDED.is_default,
    role = COALESCE(sqlc.narg(role)::text, public.users_teams.role);

-- name: DeleteTeamMember :exec
DELETE FROM public.users_teams
WHERE team_id = sqlc.arg(team_id)::uuid
  AND user_id = sqlc.arg(user_id)::uuid;
