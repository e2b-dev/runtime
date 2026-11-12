-- A sandbox gets a row in the stopped-sandbox log when it stops. A database
-- without that table fails this statement on its own, so the snapshot lookup
-- below still answers.
-- name: GetAdminSandboxOverviewLogTeamID :one
SELECT sl.team_id
FROM billing.sandbox_logs sl
WHERE sl.sandbox_id = sqlc.arg(sandbox_id)::text
LIMIT 1;

-- The team the stopped-sandbox log names wins; otherwise the snapshot's team.
-- Snapshotting keeps a sandbox running, so the snapshot can come first.
-- name: GetAdminSandboxOverviewTeam :one
SELECT t.id, t.name, t.email
FROM public.teams t
WHERE t.id = COALESCE(
    sqlc.narg(log_team_id)::uuid,
    (SELECT s.team_id FROM public.snapshots s WHERE s.sandbox_id = sqlc.arg(sandbox_id)::text LIMIT 1)
);

-- Each pause rewrites the row, so it describes the most recent pause.
-- name: GetAdminSandboxOverviewLastPause :one
SELECT s.env_id, s.auto_pause, s.config
FROM public.snapshots s
WHERE s.sandbox_id = sqlc.arg(sandbox_id)::text
LIMIT 1;

-- Assignments without a creation time are left out: they have no assignment
-- time to report, and DESC order would put them first.
-- name: ListAdminSandboxOverviewBuilds :many
SELECT
    eba.created_at::timestamptz AS assigned_at,
    eba.env_id,
    eba.build_id,
    eba.tag,
    eb.status_group,
    eb.cluster_node_id
FROM public.env_build_assignments eba
JOIN public.env_builds eb ON eb.id = eba.build_id
WHERE eba.env_id = sqlc.arg(template_id)::text
  AND eba.tag = sqlc.arg(tag)::text
  AND eba.created_at IS NOT NULL
ORDER BY eba.created_at DESC;
