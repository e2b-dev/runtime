-- name: DeleteTemplateTags :exec
-- Deletes tag assignments from a template (env)
DELETE FROM "public"."env_build_assignments"
WHERE env_id = @template_id AND tag = ANY(@tags::text[]);