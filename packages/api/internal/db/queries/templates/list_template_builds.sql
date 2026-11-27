-- name: ListTemplateBuilds :many
-- Lists a template's builds, newest first, each with the tags that point at it.
-- A tag points at the build of its latest assignment to a ready build, so a
-- build that is pending or has failed carries no tag. Ties are broken by build
-- ID, the order of the assignments index.
-- A build belongs to the templates it is assigned to, so every page reads all
-- of the template's assignments and orders their builds. It reads only the
-- three columns ordering and filtering need, and the other columns for the
-- page's builds alone: a build row also holds its Dockerfile and commands.
-- @statuses: when not empty, only the builds in these status groups
-- @tags: when not empty, only the builds these tags point at
WITH assignments AS (
  SELECT a.build_id, a.tag, a.created_at
  FROM public.env_build_assignments a
  WHERE a.env_id = sqlc.arg(template_id)
),
builds AS (
  SELECT eb.id, eb.created_at, eb.status_group
  FROM (SELECT DISTINCT a.build_id FROM assignments a) tb
  JOIN public.env_builds eb ON eb.id = tb.build_id
),
build_tags AS (
  SELECT ct.build_id, array_agg(ct.tag ORDER BY ct.tag) AS tags
  FROM (
    SELECT DISTINCT ON (a.tag) a.tag, a.build_id
    FROM assignments a
    JOIN builds rb ON rb.id = a.build_id
    WHERE rb.status_group = 'ready'
    ORDER BY a.tag, a.created_at DESC, a.build_id DESC
  ) ct
  GROUP BY ct.build_id
),
page AS (
  SELECT b.id, b.created_at, bt.tags
  FROM builds b
  LEFT JOIN build_tags bt ON bt.build_id = b.id
  WHERE (b.created_at, b.id) < (sqlc.arg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::uuid)
    AND (
      COALESCE(cardinality(sqlc.arg(statuses)::text[]), 0) = 0
      OR b.status_group = ANY(sqlc.arg(statuses)::text[])
    )
    AND (
      COALESCE(cardinality(sqlc.arg(tags)::text[]), 0) = 0
      OR bt.tags && sqlc.arg(tags)::text[]
    )
  ORDER BY b.created_at DESC, b.id DESC
  LIMIT sqlc.arg(build_limit)
)
SELECT eb.id, eb.created_at, eb.updated_at, eb.finished_at, eb.status_group,
       eb.vcpu, eb.max_vcpu, eb.max_ram_mb, eb.start_memory_mb, eb.ram_mb, eb.total_disk_size_mb, eb.envd_version,
       COALESCE(p.tags, '{}')::text[] AS tags
FROM page p
JOIN public.env_builds eb ON eb.id = p.id
ORDER BY p.created_at DESC, p.id DESC;
