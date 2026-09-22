-- name: GetSnapshotsWithCursor :many
SELECT COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases, COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    sqlc.embed(s),
    eb.id AS build_id,
    eb.vcpu AS build_vcpu,
    eb.ram_mb AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    eb.created_at AS build_created_at
FROM "public"."snapshots" s
JOIN "public"."active_envs" e ON e.id = s.env_id
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM "public"."env_aliases"
    WHERE env_id = s.base_env_id
) ea ON TRUE
JOIN LATERAL (
    SELECT eb.id, eb.vcpu, eb.ram_mb, eb.total_disk_size_mb, eb.envd_version, eb.created_at
    FROM "public"."env_build_assignments" eba
    JOIN "public"."env_builds" eb ON eb.id = eba.build_id
    WHERE
        eba.env_id = s.env_id
        AND eba.tag = 'default'
        AND eb.status_group = 'ready'
    ORDER BY eba.created_at DESC
    LIMIT 1
) eb ON TRUE
WHERE
    s.team_id = @team_id
    -- The order here is important, we want started_at descending, but sandbox_id ascending
    -- Avoid estimating JSON containment when the caller supplied no metadata filter.
    AND (@metadata::jsonb = '{}'::jsonb OR s.metadata @> @metadata)
    AND s.sandbox_started_at >= @started_after::timestamptz
    AND (s.sandbox_started_at, @cursor_id::text) < (@cursor_time, s.sandbox_id)
ORDER BY s.sandbox_started_at DESC, s.sandbox_id ASC
LIMIT $1;

-- name: GetSnapshotsWithCursorAsc :many
-- Ascending counterpart of GetSnapshotsWithCursor. It is the exact reverse order
-- (started_at ASC, sandbox_id DESC) which maps onto a backward scan of the
-- idx_snapshots_team_time_id (team_id, sandbox_started_at DESC, sandbox_id) index.
SELECT COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases, COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    sqlc.embed(s),
    eb.id AS build_id,
    eb.vcpu AS build_vcpu,
    eb.ram_mb AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    eb.created_at AS build_created_at
FROM "public"."snapshots" s
JOIN "public"."active_envs" e ON e.id = s.env_id
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM "public"."env_aliases"
    WHERE env_id = s.base_env_id
) ea ON TRUE
JOIN LATERAL (
    SELECT eb.id, eb.vcpu, eb.ram_mb, eb.total_disk_size_mb, eb.envd_version, eb.created_at
    FROM "public"."env_build_assignments" eba
    JOIN "public"."env_builds" eb ON eb.id = eba.build_id
    WHERE
        eba.env_id = s.env_id
        AND eba.tag = 'default'
        AND eb.status_group = 'ready'
    ORDER BY eba.created_at DESC
    LIMIT 1
) eb ON TRUE
WHERE
    s.team_id = @team_id
    AND (@metadata::jsonb = '{}'::jsonb OR s.metadata @> @metadata)
    AND s.sandbox_started_at >= @started_after::timestamptz
    -- The lower bound supplies the timestamp constraint for the ID tie-breaker and
    -- gives the planner an indexable starting point for the ascending scan.
    AND s.sandbox_started_at >= @cursor_time
    AND (s.sandbox_started_at > @cursor_time OR s.sandbox_id < @cursor_id::text)
ORDER BY s.sandbox_started_at ASC, s.sandbox_id DESC
LIMIT $1;

-- name: GetSnapshotsByTemplateWithCursor :many
SELECT COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases, COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    sqlc.embed(s),
    eb.id AS build_id,
    eb.vcpu AS build_vcpu,
    eb.ram_mb AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    eb.created_at AS build_created_at
FROM "public"."snapshots" s
JOIN "public"."active_envs" e ON e.id = s.env_id
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM "public"."env_aliases"
    WHERE env_id = s.base_env_id
) ea ON TRUE
JOIN LATERAL (
    SELECT eb.id, eb.vcpu, eb.ram_mb, eb.total_disk_size_mb, eb.envd_version, eb.created_at
    FROM "public"."env_build_assignments" eba
    JOIN "public"."env_builds" eb ON eb.id = eba.build_id
    WHERE
        eba.env_id = s.env_id
        AND eba.tag = 'default'
        AND eb.status_group = 'ready'
    ORDER BY eba.created_at DESC
    LIMIT 1
) eb ON TRUE
WHERE
    s.team_id = @team_id
    AND s.base_env_id = @template_id
    AND (@metadata::jsonb = '{}'::jsonb OR s.metadata @> @metadata)
    AND s.sandbox_started_at >= @started_after::timestamptz
    AND (s.sandbox_started_at, @cursor_id::text) < (@cursor_time, s.sandbox_id)
ORDER BY s.sandbox_started_at DESC, s.sandbox_id ASC
LIMIT $1;

-- name: GetSnapshotsByTemplateWithCursorAsc :many
SELECT COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases, COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    sqlc.embed(s),
    eb.id AS build_id,
    eb.vcpu AS build_vcpu,
    eb.ram_mb AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    eb.created_at AS build_created_at
FROM "public"."snapshots" s
JOIN "public"."active_envs" e ON e.id = s.env_id
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM "public"."env_aliases"
    WHERE env_id = s.base_env_id
) ea ON TRUE
JOIN LATERAL (
    SELECT eb.id, eb.vcpu, eb.ram_mb, eb.total_disk_size_mb, eb.envd_version, eb.created_at
    FROM "public"."env_build_assignments" eba
    JOIN "public"."env_builds" eb ON eb.id = eba.build_id
    WHERE
        eba.env_id = s.env_id
        AND eba.tag = 'default'
        AND eb.status_group = 'ready'
    ORDER BY eba.created_at DESC
    LIMIT 1
) eb ON TRUE
WHERE
    s.team_id = @team_id
    AND s.base_env_id = @template_id
    AND (@metadata::jsonb = '{}'::jsonb OR s.metadata @> @metadata)
    AND s.sandbox_started_at >= @started_after::timestamptz
    -- The lower bound supplies the timestamp constraint for the ID tie-breaker and
    -- gives the planner an indexable starting point for the ascending scan.
    AND s.sandbox_started_at >= @cursor_time
    AND (s.sandbox_started_at > @cursor_time OR s.sandbox_id < @cursor_id::text)
ORDER BY s.sandbox_started_at ASC, s.sandbox_id DESC
LIMIT $1;
