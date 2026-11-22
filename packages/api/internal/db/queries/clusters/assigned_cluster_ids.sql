-- The remote clusters the cluster pool connects to: those a team references.
-- name: ListAssignedClusterIDs :many
SELECT DISTINCT cluster_id::uuid AS cluster_id
FROM public.teams
WHERE cluster_id IS NOT NULL;
