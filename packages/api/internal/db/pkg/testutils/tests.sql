-- name: GetRowLevelSecurity :many
select
    c.relkind::text as kind,
    n.nspname as namespace_name,
    c.relname as table_name,
    c.relrowsecurity as row_level_security,
    c.reloptions::text[] as options
from pg_catalog.pg_class c
join pg_catalog.pg_namespace n on c.relnamespace = n.oid
where c.relkind in (
    'r', -- ordinary table
    'v', -- view
    'm'  -- materialized view
);

-- name: InsertTestTeam :exec
-- Inserts a team for tests with explicit id/slug and no tier, as the
-- management API creates them.
INSERT INTO public.teams (id, name, email, slug)
VALUES (
    sqlc.arg(id)::uuid,
    sqlc.arg(name)::text,
    sqlc.arg(email)::text,
    sqlc.arg(slug)::text
);

-- name: InsertTestEnv :exec
-- Inserts an env (template) row used as a base for tests. Mirrors what
-- production code does via CreateOrUpdateTemplate but without the build_count
-- bookkeeping so tests can seed deterministic rows.
INSERT INTO public.envs (id, team_id, public, updated_at, source)
VALUES (
    sqlc.arg(id)::text,
    sqlc.arg(team_id)::uuid,
    sqlc.arg(public)::boolean,
    NOW(),
    sqlc.arg(source)::text
);
