-- name: ApplyProjectSettingsProjection :exec
INSERT INTO projection.project_settings (project_id, revision, sandbox_default_allow_public_traffic)
VALUES (
    sqlc.arg(project_id)::uuid,
    sqlc.arg(revision)::bigint,
    sqlc.narg(sandbox_default_allow_public_traffic)::boolean
)
ON CONFLICT (project_id) DO UPDATE
SET
    revision = EXCLUDED.revision,
    sandbox_default_allow_public_traffic = EXCLUDED.sandbox_default_allow_public_traffic,
    updated_at = now()
WHERE projection.project_settings.revision < EXCLUDED.revision;
