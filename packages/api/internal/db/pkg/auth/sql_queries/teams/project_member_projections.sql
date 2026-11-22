-- name: ApplyProjectMemberProjection :one
WITH changed AS (
    INSERT INTO projection.project_members (project_id, user_id, revision, present)
    VALUES (
        sqlc.arg(project_id)::uuid,
        sqlc.arg(user_id)::uuid,
        sqlc.arg(revision)::bigint,
        sqlc.arg(present)::boolean
    )
    ON CONFLICT (project_id, user_id) DO UPDATE
    SET
        revision = EXCLUDED.revision,
        present = EXCLUDED.present,
        updated_at = now()
    WHERE projection.project_members.revision < EXCLUDED.revision
    RETURNING project_id
)
SELECT EXISTS (SELECT 1 FROM changed) AS applied;
