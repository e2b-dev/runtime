-- name: ApplyProjectSettingsProjection :exec
INSERT INTO projection.project_settings (
    project_id, revision, sandbox_default_allow_public_traffic, sandbox_default_expose_localhost_ports
)
VALUES (
    sqlc.arg(project_id)::uuid,
    sqlc.arg(revision)::bigint,
    sqlc.narg(sandbox_default_allow_public_traffic)::boolean,
    sqlc.narg(sandbox_default_expose_localhost_ports)::boolean
)
ON CONFLICT (project_id) DO UPDATE
SET
    revision = EXCLUDED.revision,
    sandbox_default_allow_public_traffic = EXCLUDED.sandbox_default_allow_public_traffic,
    sandbox_default_expose_localhost_ports = EXCLUDED.sandbox_default_expose_localhost_ports,
    updated_at = now()
WHERE projection.project_settings.revision < EXCLUDED.revision;
