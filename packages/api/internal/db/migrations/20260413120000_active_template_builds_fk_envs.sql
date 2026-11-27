-- +goose Up

-- Clean up orphaned rows (templates already deleted).
DELETE FROM public.active_template_builds
WHERE template_id NOT IN (SELECT id FROM public.envs);

-- Index to support the FK cascade lookup on envs row deletion.
CREATE INDEX IF NOT EXISTS idx_active_template_builds_template_id
    ON public.active_template_builds (template_id);

-- Ensure cascade cleanup when a template is deleted.
ALTER TABLE public.active_template_builds
    ADD CONSTRAINT fk_active_template_builds_envs
    FOREIGN KEY (template_id) REFERENCES public.envs(id) ON DELETE CASCADE;

-- +goose Down

ALTER TABLE public.active_template_builds
    DROP CONSTRAINT IF EXISTS fk_active_template_builds_envs;

DROP INDEX IF EXISTS public.idx_active_template_builds_template_id;
