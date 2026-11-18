-- name: GetProjectPlacementRevision :one
-- Read after locking the team in a separate statement so the last holder's revision is visible.
SELECT COALESCE((SELECT revision FROM projection.project_placements WHERE project_id = @project_id), 0)::bigint AS revision;

-- name: RecordProjectPlacementRevision :exec
INSERT INTO projection.project_placements (project_id, revision)
VALUES (@project_id, @revision)
ON CONFLICT (project_id) DO UPDATE SET revision = EXCLUDED.revision
WHERE projection.project_placements.revision < EXCLUDED.revision;
