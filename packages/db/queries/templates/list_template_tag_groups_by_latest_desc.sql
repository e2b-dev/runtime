-- name: ListTemplateTagGroupsByLatestDesc :many
WITH scored AS (
    SELECT
        eba.id                                   AS assignment_id,
        eba.tag,
        eba.build_id,
        eba.created_at                           AS assigned_at,
        eb.created_at                            AS build_created_at,
        eb.finished_at                           AS build_finished_at,
        MAX(eba.created_at) OVER (
            PARTITION BY eba.tag
        )::timestamptz                           AS latest_assigned_at,
        ROW_NUMBER() OVER (
            PARTITION BY eba.tag
            ORDER BY eba.created_at DESC, eba.id DESC
        ) AS assignment_rank
    FROM public.env_build_assignments eba
    JOIN public.env_builds eb ON eb.id = eba.build_id
    WHERE eba.env_id = sqlc.arg(template_id)::text
      AND eb.status_group = 'ready'
      -- tags and search input are lowercased on write, so matching is case-insensitive
      AND (
          sqlc.arg(search)::text = ''
          OR strpos(eba.tag, sqlc.arg(search)::text) > 0
      )
),
tag_window AS (
    SELECT tag, latest_assigned_at
    FROM scored
    WHERE assignment_rank = 1
      AND (
          sqlc.narg(cursor_time)::timestamptz IS NULL
          OR latest_assigned_at < sqlc.narg(cursor_time)::timestamptz
          OR (
              latest_assigned_at = sqlc.narg(cursor_time)::timestamptz
              AND tag > sqlc.narg(cursor_tag)::text
          )
      )
    ORDER BY latest_assigned_at DESC, tag ASC
    LIMIT sqlc.arg(tags_limit_plus_one)::int
)
SELECT
    s.assignment_id,
    s.tag,
    s.build_id,
    s.assigned_at,
    s.build_created_at,
    s.build_finished_at,
    s.latest_assigned_at
FROM scored s
JOIN tag_window tw ON tw.tag = s.tag
WHERE s.assignment_rank <= sqlc.arg(assignment_limit_plus_one)::int
ORDER BY s.latest_assigned_at DESC, s.tag ASC, s.assignment_rank ASC;
