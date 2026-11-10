-- Team template search, one query per (sort column,
-- direction) pair so each branch stays an index-driven top-N scan.
--
-- Each query merges two keyset-paginated branches: the team's own templates,
-- and the public default templates (env_defaults) of other teams, included
-- when include_defaults is set. Both branches use the same sort key, cursor
-- predicate and LIMIT, so the merged page is consistent across pages. The
-- branches cannot overlap (the second skips the team's own templates), so
-- they are combined with UNION ALL.
--
-- The columns are those of ListDefaultTemplates, in the same order, so the
-- rows of both convert to one type. The newest build tagged default in any
-- state gives build_status, and the newest ready build tagged default gives
-- the resources, zero when there is none.
--
-- search is a substring matched case-insensitively against the template ID,
-- each alias and each namespace/alias name; the caller escapes LIKE
-- wildcards. A NULL public matches both visibilities.

-- name: SearchTeamTemplatesByCreatedAtDesc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        (d.env_id IS NOT NULL)::boolean AS is_default,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) < (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        TRUE AS is_default,
        d.description AS default_description
    FROM public.env_defaults AS d
    JOIN public.active_envs AS e ON e.id = d.env_id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND e.team_id <> sqlc.arg(team_id)::uuid AND e.source = 'template' AND e.public
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) < (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
page AS (
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM team_templates
    UNION ALL
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM default_templates
    ORDER BY created_at DESC, id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
    p.id AS template_id,
    p.team_id,
    p.created_at,
    p.updated_at,
    p.public,
    p.build_count,
    p.spawn_count,
    p.last_spawned_at,
    p.created_by AS creator_id,
    p.is_default,
    COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
    COALESCE(eb.vcpu, 0)::bigint AS build_vcpu,
    eb.max_vcpu AS build_max_vcpu,
    COALESCE(eb.ram_mb, 0)::bigint AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    COALESCE(latest_build.status_group, 'pending') AS build_status,
    COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
    COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    p.default_description
FROM page AS p
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM public.env_aliases
    WHERE env_id = p.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.status_group
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default'
    ORDER BY ba.created_at DESC
    LIMIT 1
) latest_build ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.max_vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY p.created_at DESC, p.id DESC;

-- name: SearchTeamTemplatesByCreatedAtAsc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        (d.env_id IS NOT NULL)::boolean AS is_default,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) > (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        TRUE AS is_default,
        d.description AS default_description
    FROM public.env_defaults AS d
    JOIN public.active_envs AS e ON e.id = d.env_id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND e.team_id <> sqlc.arg(team_id)::uuid AND e.source = 'template' AND e.public
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) > (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
page AS (
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM team_templates
    UNION ALL
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM default_templates
    ORDER BY created_at ASC, id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
    p.id AS template_id,
    p.team_id,
    p.created_at,
    p.updated_at,
    p.public,
    p.build_count,
    p.spawn_count,
    p.last_spawned_at,
    p.created_by AS creator_id,
    p.is_default,
    COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
    COALESCE(eb.vcpu, 0)::bigint AS build_vcpu,
    eb.max_vcpu AS build_max_vcpu,
    COALESCE(eb.ram_mb, 0)::bigint AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    COALESCE(latest_build.status_group, 'pending') AS build_status,
    COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
    COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    p.default_description
FROM page AS p
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM public.env_aliases
    WHERE env_id = p.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.status_group
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default'
    ORDER BY ba.created_at DESC
    LIMIT 1
) latest_build ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.max_vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY p.created_at ASC, p.id ASC;

-- name: SearchTeamTemplatesByUpdatedAtDesc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        (d.env_id IS NOT NULL)::boolean AS is_default,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) < (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        TRUE AS is_default,
        d.description AS default_description
    FROM public.env_defaults AS d
    JOIN public.active_envs AS e ON e.id = d.env_id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND e.team_id <> sqlc.arg(team_id)::uuid AND e.source = 'template' AND e.public
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) < (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
page AS (
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM team_templates
    UNION ALL
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM default_templates
    ORDER BY updated_at DESC, id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
    p.id AS template_id,
    p.team_id,
    p.created_at,
    p.updated_at,
    p.public,
    p.build_count,
    p.spawn_count,
    p.last_spawned_at,
    p.created_by AS creator_id,
    p.is_default,
    COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
    COALESCE(eb.vcpu, 0)::bigint AS build_vcpu,
    eb.max_vcpu AS build_max_vcpu,
    COALESCE(eb.ram_mb, 0)::bigint AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    COALESCE(latest_build.status_group, 'pending') AS build_status,
    COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
    COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    p.default_description
FROM page AS p
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM public.env_aliases
    WHERE env_id = p.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.status_group
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default'
    ORDER BY ba.created_at DESC
    LIMIT 1
) latest_build ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.max_vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY p.updated_at DESC, p.id DESC;

-- name: SearchTeamTemplatesByUpdatedAtAsc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        (d.env_id IS NOT NULL)::boolean AS is_default,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) > (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.team_id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        TRUE AS is_default,
        d.description AS default_description
    FROM public.env_defaults AS d
    JOIN public.active_envs AS e ON e.id = d.env_id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND e.team_id <> sqlc.arg(team_id)::uuid AND e.source = 'template' AND e.public
        AND (sqlc.narg(public)::boolean IS NULL OR e.public = sqlc.narg(public)::boolean)
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases AS sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) > (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
page AS (
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM team_templates
    UNION ALL
    SELECT id, team_id, created_at, updated_at, public, build_count, spawn_count, last_spawned_at, created_by, is_default, default_description
    FROM default_templates
    ORDER BY updated_at ASC, id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
    p.id AS template_id,
    p.team_id,
    p.created_at,
    p.updated_at,
    p.public,
    p.build_count,
    p.spawn_count,
    p.last_spawned_at,
    p.created_by AS creator_id,
    p.is_default,
    COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
    COALESCE(eb.vcpu, 0)::bigint AS build_vcpu,
    eb.max_vcpu AS build_max_vcpu,
    COALESCE(eb.ram_mb, 0)::bigint AS build_ram_mb,
    eb.total_disk_size_mb AS build_total_disk_size_mb,
    eb.envd_version AS build_envd_version,
    COALESCE(latest_build.status_group, 'pending') AS build_status,
    COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
    COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
    p.default_description
FROM page AS p
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM public.env_aliases
    WHERE env_id = p.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.status_group
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default'
    ORDER BY ba.created_at DESC
    LIMIT 1
) latest_build ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.max_vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = p.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY p.updated_at ASC, p.id ASC;
