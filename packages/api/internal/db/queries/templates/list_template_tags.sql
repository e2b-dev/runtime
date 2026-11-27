-- name: ListTemplateTags :many
-- Lists the latest tag assignment per tag for a given template.
-- Multiple assignments can exist per tag; only the most recent is returned.
-- @template_id: the template ID to look up
SELECT DISTINCT ON (eba.tag) eba.tag, eba.build_id, eba.created_at
FROM public.env_build_assignments AS eba
WHERE eba.env_id = @template_id
ORDER BY eba.tag, eba.created_at DESC;
