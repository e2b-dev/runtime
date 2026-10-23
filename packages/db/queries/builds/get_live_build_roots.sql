-- name: GetLiveBuildRootsForTeamExcludingTemplate :many
-- Returns the build IDs of every build assigned to a still-active (not
-- soft-deleted) template of the team, EXCLUDING the template being deleted.
-- These are the "live roots" whose header mappings may still reference — and
-- therefore protect — a build that the deletion query considers exclusive to
-- the doomed template. Cache sharing is team-scoped, so a cross-template
-- ancestor can only come from the same team; this bounds the set.
SELECT DISTINCT eb.id AS build_id
FROM "public"."env_build_assignments" eba
JOIN "public"."env_builds" eb ON eb.id = eba.build_id
JOIN "public"."active_envs" e ON e.id = eba.env_id
WHERE e.team_id = @team_id
  AND eba.env_id != @template_id;
