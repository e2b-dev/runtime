-- name: ListTelemetryNames :many
SELECT 'team'::text AS kind, id::text AS id, name, ''::text AS telemetry_client_id, COALESCE(cluster_id::text, '')::text AS cluster_id
FROM public.teams WHERE deleted_at IS NULL AND cluster_id IS NOT NULL
UNION ALL
SELECT 'cluster'::text AS kind, id::text AS id, name, COALESCE(telemetry_client_id, '') AS telemetry_client_id, ''::text AS cluster_id
FROM public.clusters WHERE deletion_requested_at IS NULL;

-- name: UpdateClusterTelemetryClient :one
WITH updated AS (
    UPDATE public.clusters
    SET telemetry_client_id = sqlc.arg(telemetry_client_id)::text
    WHERE id = sqlc.arg(cluster_id)::uuid
      AND deletion_requested_at IS NULL
      AND (telemetry_client_id IS NULL OR telemetry_client_id = sqlc.arg(telemetry_client_id)::text)
    RETURNING id
)
SELECT EXISTS(SELECT 1 FROM updated) AS updated,
       EXISTS(SELECT 1 FROM public.clusters WHERE id = sqlc.arg(cluster_id)::uuid AND deletion_requested_at IS NULL) AS found;
