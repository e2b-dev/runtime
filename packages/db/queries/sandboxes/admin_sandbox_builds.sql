-- resumable applies GetLastSnapshot's rule: the snapshot's template is not
-- deleted and has a ready build under the default tag.
-- name: GetAdminSandboxBuildsTemplate :one
SELECT
    s.env_id,
    EXISTS (
        SELECT 1
        FROM public.active_envs e
        JOIN public.env_build_assignments eba
            ON eba.env_id = e.id
           AND eba.tag = 'default'
        JOIN public.env_builds eb
            ON eb.id = eba.build_id
           AND eb.status_group = 'ready'
        WHERE e.id = s.env_id
    ) AS resumable
FROM public.snapshots s
WHERE s.sandbox_id = sqlc.arg(sandbox_id)::text;

-- Locks the template row so concurrent restores of the same sandbox run one
-- at a time, each seeing the assignment the previous one wrote. NO KEY UPDATE
-- leaves inserts that only reference the row free to proceed.
-- name: LockAdminSandboxSnapshotTemplate :one
SELECT e.id
FROM public.snapshots s
JOIN public.envs e ON e.id = s.env_id
WHERE s.sandbox_id = sqlc.arg(sandbox_id)::text
  AND e.deleted_at IS NULL
FOR NO KEY UPDATE OF e;

-- The build a resume or start resolves the tag to: its newest ready
-- assignment. Ties on created_at break on build_id, the order the tag lookup
-- index keeps.
-- name: GetAdminSandboxCurrentBuildID :one
SELECT eba.build_id
FROM public.env_build_assignments eba
JOIN public.env_builds eb ON eb.id = eba.build_id
WHERE eba.env_id = sqlc.arg(template_id)::text
  AND eba.tag = sqlc.arg(tag)::text
  AND eba.created_at IS NOT NULL
  AND eb.status_group = 'ready'
ORDER BY eba.created_at DESC, eba.build_id DESC
LIMIT 1;

-- name: ListAdminSandboxBuilds :many
SELECT
    eba.build_id,
    eba.env_id,
    eba.tag,
    eba.created_at::timestamptz AS assigned_at,
    eb.status_group,
    eb.cluster_node_id
FROM public.env_build_assignments eba
JOIN public.env_builds eb ON eb.id = eba.build_id
WHERE eba.env_id = sqlc.arg(template_id)::text
  AND eba.tag = sqlc.arg(tag)::text
  AND eba.created_at IS NOT NULL
  AND (NOT sqlc.arg(by_status_groups)::boolean OR eb.status_group = ANY(sqlc.arg(status_groups)::text[]))
ORDER BY
    CASE WHEN sqlc.arg(descending)::boolean THEN eba.created_at END DESC,
    CASE WHEN sqlc.arg(descending)::boolean THEN eba.build_id END DESC,
    CASE WHEN NOT sqlc.arg(descending)::boolean THEN eba.created_at END ASC,
    CASE WHEN NOT sqlc.arg(descending)::boolean THEN eba.build_id END ASC
LIMIT sqlc.arg(page_size)::bigint OFFSET sqlc.arg(offset_count)::bigint;

-- Takes the same filters as ListAdminSandboxBuilds.
-- name: CountAdminSandboxBuilds :one
SELECT COUNT(*)::bigint AS total
FROM public.env_build_assignments eba
JOIN public.env_builds eb ON eb.id = eba.build_id
WHERE eba.env_id = sqlc.arg(template_id)::text
  AND eba.tag = sqlc.arg(tag)::text
  AND eba.created_at IS NOT NULL
  AND (NOT sqlc.arg(by_status_groups)::boolean OR eb.status_group = ANY(sqlc.arg(status_groups)::text[]));

-- name: GetAdminSandboxRestoreBuild :one
SELECT eb.status_group, eb.filesystem_only
FROM public.env_build_assignments eba
JOIN public.env_builds eb ON eb.id = eba.build_id
WHERE eba.env_id = sqlc.arg(template_id)::text
  AND eba.build_id = sqlc.arg(build_id)::uuid
  AND eba.tag = sqlc.arg(tag)::text
LIMIT 1;

-- clock_timestamp() rather than the column's transaction-start default, so
-- the row sorts after every assignment committed before the restore took its
-- lock.
-- name: CreateAdminSandboxBuildAssignment :one
INSERT INTO public.env_build_assignments (env_id, build_id, tag, created_at)
VALUES (sqlc.arg(template_id)::text, sqlc.arg(build_id)::uuid, sqlc.arg(tag)::text, clock_timestamp())
RETURNING created_at::timestamptz AS assigned_at;
