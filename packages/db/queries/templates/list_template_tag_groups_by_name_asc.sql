-- name: ListTemplateTagGroupsByNameAsc :many
WITH tag_window AS (
    SELECT
        eba.tag,
        MAX(eba.created_at)::timestamptz AS latest_assigned_at
    FROM public.env_build_assignments eba
    JOIN public.env_builds eb ON eb.id = eba.build_id
    WHERE eba.env_id = sqlc.arg(template_id)::text
      AND eb.status_group = 'ready'
      -- tags and search input are lowercased on write, so matching is case-insensitive
      AND (
          sqlc.arg(search)::text = ''
          OR strpos(eba.tag, sqlc.arg(search)::text) > 0
      )
      AND (
          sqlc.narg(cursor_tag)::text IS NULL
          OR eba.tag > sqlc.narg(cursor_tag)::text
      )
    GROUP BY eba.tag
    ORDER BY tag ASC
    LIMIT sqlc.arg(tags_limit_plus_one)::int
),
ranked AS (
    SELECT
        eba.id                                   AS assignment_id,
        eba.tag,
        eba.build_id,
        eba.created_at                           AS assigned_at,
        eb.created_at                            AS build_created_at,
        eb.finished_at                           AS build_finished_at,
        tw.latest_assigned_at,
        ROW_NUMBER() OVER (
            PARTITION BY eba.tag
            ORDER BY eba.created_at DESC, eba.id DESC
        ) AS assignment_rank
    FROM public.env_build_assignments eba
    JOIN public.env_builds eb ON eb.id = eba.build_id
    JOIN tag_window tw ON tw.tag = eba.tag
    WHERE eba.env_id = sqlc.arg(template_id)::text
      AND eb.status_group = 'ready'
)
SELECT
    assignment_id,
    tag,
    build_id,
    assigned_at,
    build_created_at,
    build_finished_at,
    latest_assigned_at
FROM ranked
WHERE assignment_rank <= sqlc.arg(assignment_limit_plus_one)::int
ORDER BY tag ASC, assignment_rank ASC;
