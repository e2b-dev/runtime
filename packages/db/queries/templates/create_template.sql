-- name: CreateOrUpdateTemplate :one
INSERT INTO "public"."envs"(id, team_id, created_by, updated_at, public, cluster_id, source)
VALUES (@template_id, @team_id, @created_by, NOW(), @public, @cluster_id, 'template')
ON CONFLICT (id) DO UPDATE
SET updated_at  = NOW(),
    build_count = envs.build_count + 1
-- Soft-delete is permanent: skip the update for a deleted env so no row is
-- returned and the build registration fails instead of resurrecting it.
WHERE envs.deleted_at IS NULL
RETURNING id;

-- name: InvalidateUnstartedTemplateBuilds :exec
WITH invalidated AS (
    UPDATE "public"."env_builds" eb
    SET status = 'failed',
        reason = @reason,
        updated_at = NOW(),
        finished_at = NOW()
    FROM "public"."env_build_assignments" eba
    WHERE eba.build_id = eb.id
        AND eba.env_id = @template_id
        AND eba.tag = ANY(@tags::text[])
        AND eb.status_group = 'pending'
    RETURNING eb.id
)
DELETE FROM public.active_template_builds
WHERE build_id IN (SELECT id FROM invalidated);

-- name: CreateTemplateBuild :exec
-- kernel_version and firecracker_version are populated here for backwards
-- compatibility with consumers that read the env_builds row before the build
-- completes. The template-manager reports the versions it actually used via
-- TemplateBuildMetadata, and FinishTemplateBuild overwrites these fields with
-- the reported values.
INSERT INTO "public"."env_builds" (
    id,
    updated_at,
    status,
    ram_mb,
    vcpu,
    kernel_version,
    firecracker_version,
    free_disk_size_mb,
    start_cmd,
    ready_cmd,
    dockerfile,
    version
) VALUES (
    @build_id,
    NOW(),
    @status,
    @ram_mb,
    @vcpu,
    @kernel_version,
    @firecracker_version,
    @free_disk_size_mb,
    @start_cmd,
    @ready_cmd,
    @dockerfile,
    @version
);
