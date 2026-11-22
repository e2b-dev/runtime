-- GetTeamMemberIDs lists the users whose cached auth entry carries this team's
-- data, so invalidating the team can reach all of them.
-- name: GetTeamMemberIDs :many
SELECT ut.user_id
FROM "public"."users_teams" ut
WHERE ut.team_id = @team_id;
