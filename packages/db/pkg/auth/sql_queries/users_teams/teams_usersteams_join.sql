-- name: GetTeamsWithUsersTeams :many
SELECT t.id, t.created_at, t.name, t.deleted_at, ut.is_default
FROM "public"."teams" t
JOIN "public"."users_teams" ut ON ut.team_id = t.id
WHERE ut.user_id = $1;
