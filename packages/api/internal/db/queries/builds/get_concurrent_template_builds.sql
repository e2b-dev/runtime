-- name: GetConcurrentTemplateBuilds :many
SELECT DISTINCT eb.* FROM env_build_assignments eba
JOIN env_builds eb ON eb.id = eba.build_id
WHERE
    eba.env_id = @template_id
    AND eb.status_group IN ('pending', 'in_progress')
    AND eb.id != @current_build_id
    AND eba.tag IN (
        SELECT tag FROM env_build_assignments
        WHERE build_id = @current_build_id AND env_id = @template_id
    );