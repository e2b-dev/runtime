-- Team template listing, one query per (sort field, direction) pair.
--
-- Each query merges two keyset-paginated branches: the team's own templates
-- and the globally shared default templates (env_defaults, included when
-- include_defaults is set). Both branches compute identical sort keys and
-- cursor predicates and carry their own ORDER BY + LIMIT, so each is an
-- index-driven top-N scan and UNION dedups a team's own template that is
-- also a global default.
--
-- The list intentionally supports no sorting or filtering by build fields
-- (cpu, memory): a template can have many tagged builds, so a single build
-- value per template is ambiguous. Build columns in the result are display
-- only, resolved for the returned page via the eb lateral below. Name
-- sorting is also unsupported: the sort key lives in env_aliases, costing a
-- per-template lateral (seconds per page for 100k+-template teams even
-- warm); finding templates by name is served by the search filter instead.

-- name: ListTeamTemplatesByCreatedAtAsc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) > (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) > (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
ranked AS (
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM team_templates
    UNION
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM default_templates
    ORDER BY created_at ASC, id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
        r.id AS template_id,
        r.created_at,
        r.updated_at,
        r.public,
        r.build_count,
        r.spawn_count,
        r.last_spawned_at,
        r.created_by AS creator_id,
        COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
        COALESCE(eb.vcpu, 0)::bigint AS cpu_count,
        COALESCE(eb.ram_mb, 0)::bigint AS memory_mb,
        eb.total_disk_size_mb AS disk_size_mb,
        eb.envd_version AS envd_version,
        COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
        COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
        (r.default_env_id IS NOT NULL)::boolean AS is_default,
        r.default_description,
        COALESCE(ea.name_sort_key, '')::text AS name_sort_key
FROM ranked r
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names,
        MIN(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END) AS name_sort_key
    FROM public.env_aliases
    WHERE env_id = r.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = r.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY r.created_at ASC, r.id ASC;

-- name: ListTeamTemplatesByCreatedAtDesc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.created_at, e.id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
ranked AS (
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM team_templates
    UNION
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM default_templates
    ORDER BY created_at DESC, id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
        r.id AS template_id,
        r.created_at,
        r.updated_at,
        r.public,
        r.build_count,
        r.spawn_count,
        r.last_spawned_at,
        r.created_by AS creator_id,
        COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
        COALESCE(eb.vcpu, 0)::bigint AS cpu_count,
        COALESCE(eb.ram_mb, 0)::bigint AS memory_mb,
        eb.total_disk_size_mb AS disk_size_mb,
        eb.envd_version AS envd_version,
        COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
        COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
        (r.default_env_id IS NOT NULL)::boolean AS is_default,
        r.default_description,
        COALESCE(ea.name_sort_key, '')::text AS name_sort_key
FROM ranked r
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names,
        MIN(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END) AS name_sort_key
    FROM public.env_aliases
    WHERE env_id = r.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = r.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY r.created_at DESC, r.id DESC;

-- name: ListTeamTemplatesByUpdatedAtAsc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) > (sqlc.arg(cursor_updated_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) > (sqlc.arg(cursor_updated_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at ASC, e.id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
),
ranked AS (
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM team_templates
    UNION
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM default_templates
    ORDER BY updated_at ASC, id ASC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
        r.id AS template_id,
        r.created_at,
        r.updated_at,
        r.public,
        r.build_count,
        r.spawn_count,
        r.last_spawned_at,
        r.created_by AS creator_id,
        COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
        COALESCE(eb.vcpu, 0)::bigint AS cpu_count,
        COALESCE(eb.ram_mb, 0)::bigint AS memory_mb,
        eb.total_disk_size_mb AS disk_size_mb,
        eb.envd_version AS envd_version,
        COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
        COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
        (r.default_env_id IS NOT NULL)::boolean AS is_default,
        r.default_description,
        COALESCE(ea.name_sort_key, '')::text AS name_sort_key
FROM ranked r
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names,
        MIN(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END) AS name_sort_key
    FROM public.env_aliases
    WHERE env_id = r.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = r.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY r.updated_at ASC, r.id ASC;

-- name: ListTeamTemplatesByUpdatedAtDesc :many
WITH team_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    LEFT JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        e.team_id = sqlc.arg(team_id)::uuid AND e.source = 'template'
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) < (sqlc.arg(cursor_updated_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
default_templates AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        d.env_id AS default_env_id,
        d.description AS default_description
    FROM public.active_envs AS e
    JOIN public.env_defaults AS d ON d.env_id = e.id
    WHERE
        sqlc.arg(include_defaults)::boolean
        AND (sqlc.arg(filter_public)::smallint = -1 OR e.public = (sqlc.arg(filter_public)::smallint = 1))
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (e.updated_at, e.id) < (sqlc.arg(cursor_updated_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.updated_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
),
ranked AS (
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM team_templates
    UNION
    SELECT
        id, created_at, updated_at, public, build_count, spawn_count,
        last_spawned_at, created_by, default_env_id, default_description
    FROM default_templates
    ORDER BY updated_at DESC, id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
        r.id AS template_id,
        r.created_at,
        r.updated_at,
        r.public,
        r.build_count,
        r.spawn_count,
        r.last_spawned_at,
        r.created_by AS creator_id,
        COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
        COALESCE(eb.vcpu, 0)::bigint AS cpu_count,
        COALESCE(eb.ram_mb, 0)::bigint AS memory_mb,
        eb.total_disk_size_mb AS disk_size_mb,
        eb.envd_version AS envd_version,
        COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
        COALESCE(ea.names, ARRAY[]::text[])::text[] AS names,
        (r.default_env_id IS NOT NULL)::boolean AS is_default,
        r.default_description,
        COALESCE(ea.name_sort_key, '')::text AS name_sort_key
FROM ranked r
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names,
        MIN(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END) AS name_sort_key
    FROM public.env_aliases
    WHERE env_id = r.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = r.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY r.updated_at DESC, r.id DESC;
