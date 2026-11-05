-- name: GetLiveBuildRootsForTeamExcludingTemplate :many
-- Returns the "live roots" of a team whose header mappings may still reference —
-- and therefore protect — a build that the deletion query considers exclusive to
-- the template being deleted. Cache sharing is team-scoped, so a cross-template
-- ancestor can only come from the same team; this bounds the set.
--
-- The set is the UNION of two sources, because a build can protect an ancestor
-- without having an env_build_assignments row to an active template yet:
--   (1) builds assigned to a still-active (not soft-deleted) OTHER template, and
--   (2) the team's in-progress/pending builds, which are materialized (or being
--       materialized) on a node and reuse ancestor layers via cache hits, but
--       may not have an assignment row until they finish.
-- Source (2) closes the gap where a concurrent build reusing the doomed
-- template's exclusive ancestor would otherwise be invisible to the guard.
--
-- Residual (handled by the offline sweeper, not here): builds whose existence is
-- only known to the orchestrator/Redis before any DB row lands.
SELECT DISTINCT eb.id AS build_id
FROM "public"."env_build_assignments" eba
JOIN "public"."env_builds" eb ON eb.id = eba.build_id
JOIN "public"."active_envs" e ON e.id = eba.env_id
WHERE e.team_id = @team_id
  AND eba.env_id != @template_id
UNION
SELECT DISTINCT eb.id AS build_id
FROM "public"."env_builds" eb
WHERE eb.team_id = @team_id
  AND eb.status_group IN ('pending', 'in_progress');
