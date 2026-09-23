-- Advance in the same transaction as teams.is_blocked so retries cannot skip an unwritten state.
-- name: ApplyProjectBlockProjection :one
WITH changed AS (
    INSERT INTO projection.project_blocks (project_id, revision)
    VALUES (
        sqlc.arg(project_id)::uuid,
        sqlc.arg(revision)::bigint
    )
    ON CONFLICT (project_id) DO UPDATE
    SET
        revision = EXCLUDED.revision,
        updated_at = now()
    WHERE projection.project_blocks.revision < EXCLUDED.revision
    RETURNING project_id
)
SELECT EXISTS (SELECT 1 FROM changed) AS applied;
