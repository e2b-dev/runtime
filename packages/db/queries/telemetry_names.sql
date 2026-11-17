-- Cluster names come from the cluster service.
-- name: ListTelemetryTeamNames :many
SELECT id, name, cluster_id::uuid AS cluster_id
FROM public.teams
WHERE deleted_at IS NULL AND cluster_id IS NOT NULL;
