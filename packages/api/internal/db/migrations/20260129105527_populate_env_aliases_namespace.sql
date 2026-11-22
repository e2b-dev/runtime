-- +goose Up
-- +goose StatementBegin

/*
Phase 2: Populate namespace for existing aliases.

This migration populates the namespace column for existing aliases:
1. For PRIVATE templates: Update namespace from NULL to the team's slug
2. For PUBLIC templates: Keep the NULL entry (for backward compatibility with bare alias access)
   AND insert a new entry with the team's namespace (for namespace/alias access)
*/

-- For PRIVATE templates: update namespace to team slug
UPDATE public.env_aliases ea
SET namespace = t.slug
FROM public.envs e
JOIN public.teams t ON t.id = e.team_id
WHERE ea.env_id = e.id
  AND ea.namespace IS NULL
  AND e.public = FALSE;

-- For PUBLIC templates: insert new row with namespace (keep NULL entry for backward compat)
INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace)
SELECT ea.alias, ea.env_id, ea.is_renamable, t.slug
FROM public.env_aliases ea
JOIN public.envs e ON e.id = ea.env_id
JOIN public.teams t ON t.id = e.team_id
WHERE ea.namespace IS NULL
  AND e.public = TRUE
ON CONFLICT (alias, namespace) DO NOTHING;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- For rows that have a duplicate NULL-namespace entry (public templates where we inserted
-- a new namespaced row), delete the namespaced row since we can't set it to NULL
DELETE FROM public.env_aliases ea
WHERE ea.namespace IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM public.env_aliases ea2
    WHERE ea2.alias = ea.alias
      AND ea2.env_id = ea.env_id
      AND ea2.namespace IS NULL
  );

-- For remaining rows (private templates that were updated), reset namespace to NULL
UPDATE public.env_aliases
SET namespace = NULL
WHERE namespace IS NOT NULL;

-- +goose StatementEnd
