-- name: IsProjectDeleted :one
SELECT EXISTS (
    SELECT 1 FROM public.teams
    WHERE id = sqlc.arg(project_id)::uuid AND deleted_at IS NOT NULL
) AS deleted;
