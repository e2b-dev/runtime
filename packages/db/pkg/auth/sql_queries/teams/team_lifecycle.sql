-- name: CreateTeamMembership :exec
INSERT INTO public.users_teams (user_id, team_id, is_default, added_by)
VALUES (
    sqlc.arg(user_id)::uuid,
    sqlc.arg(team_id)::uuid,
    sqlc.arg(is_default)::boolean,
    sqlc.narg(added_by)::uuid
);
