-- Advance in the same transaction as teams.is_blocked so retries cannot skip an unwritten state.
-- name: ApplyProjectBlockProjection :one
WITH changed AS (
    INSERT INTO projection.project_blocks (project_id, revision, decided_at)
    VALUES (
        sqlc.arg(project_id)::uuid,
        sqlc.arg(revision)::bigint,
        sqlc.narg(decided_at)::timestamptz
    )
    ON CONFLICT (project_id) DO UPDATE
    SET
        revision = EXCLUDED.revision,
        decided_at = EXCLUDED.decided_at,
        updated_at = now()
    WHERE projection.project_blocks.revision < EXCLUDED.revision
    RETURNING project_id
)
SELECT EXISTS (SELECT 1 FROM changed) AS applied;
