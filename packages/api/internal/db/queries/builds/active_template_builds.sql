-- name: CreateActiveTemplateBuild :exec
INSERT INTO public.active_template_builds (
    build_id,
    team_id,
    template_id,
    tags
) VALUES (
    @build_id,
    @team_id,
    @template_id,
    @tags::text[]
);

-- name: DeleteActiveTemplateBuild :exec
DELETE FROM public.active_template_builds
WHERE build_id = @build_id;
