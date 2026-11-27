-- name: GetExclusiveBuildsForTemplateDeletion :many
-- Returns builds that are ONLY assigned to this template (safe to delete).
-- Builds shared with other templates are excluded.
-- DISTINCT needed because builds may have multiple tag assignments to the same template.
SELECT DISTINCT eb.id as build_id, eb.cluster_node_id
FROM "public"."env_build_assignments" eba
JOIN "public"."env_builds" eb ON eb.id = eba.build_id
WHERE eba.env_id = @template_id
  AND NOT EXISTS (
    -- Exclude builds that have assignments to OTHER templates
    SELECT 1 FROM "public"."env_build_assignments" other_eba
    WHERE other_eba.build_id = eb.id AND other_eba.env_id != @template_id
);