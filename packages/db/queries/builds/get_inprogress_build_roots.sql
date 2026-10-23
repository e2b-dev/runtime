-- name: GetInProgressBuildRootsForTeam :many
-- Returns the build IDs of the team's in-progress template builds (data is
-- being written to storage but the build is not yet finalized in env_builds,
-- so it may not have a stable env_build_assignments row yet). These must be
-- treated as live roots so a concurrent template deletion cannot reclaim a
-- layer an in-flight build still depends on.
SELECT build_id
FROM "public"."active_template_builds"
WHERE team_id = @team_id;
