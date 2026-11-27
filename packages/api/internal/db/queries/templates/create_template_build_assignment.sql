-- name: CreateTemplateBuildAssignment :execrows
-- FOR SHARE serializes against a concurrent DeleteTemplate so a build can't be
-- attached to a soft-deleted env. 0 rows affected means the template is gone.
WITH active AS (
    SELECT id
    FROM "public"."envs"
    WHERE id = @template_id AND deleted_at IS NULL
    FOR SHARE
)
INSERT INTO "public"."env_build_assignments" (env_id, build_id, tag)
SELECT @template_id, @build_id, @tag::text
WHERE EXISTS (SELECT 1 FROM active);
