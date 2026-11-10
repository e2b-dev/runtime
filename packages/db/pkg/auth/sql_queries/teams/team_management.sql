-- Project reconciliation for the control-plane management interface. The caller
-- supplies the id, so create and reconcile are one request and these are its
-- branches.

-- Taken first, so the branch is decided by whether the project exists rather
-- than by an insert failing. Returns the slug so a reconcile can tell whether
-- it moves.
-- name: LockManagedTeam :one
SELECT slug FROM public.teams
WHERE id = sqlc.arg(id)::uuid
FOR UPDATE;

-- DO NOTHING covers the case where the lock above found nothing and another
-- request inserted the same id before this ran: no row means that happened. A
-- slug collision is a different constraint and still raises.
-- name: InsertManagedTeam :one
INSERT INTO public.teams (id, name, slug, email, is_blocked, cluster_id)
VALUES (
    sqlc.arg(id)::uuid,
    sqlc.arg(name)::text,
    sqlc.arg(slug)::text,
    sqlc.arg(email)::text,
    false,
    sqlc.narg(cluster_id)::uuid
)
ON CONFLICT (id) DO NOTHING
RETURNING id, name, slug, email, cluster_id;

-- Touches only the properties the caller synchronizes. is_blocked stays because
-- an operator's decision must outlive a routine push.
-- name: UpdateManagedTeam :one
UPDATE public.teams
SET
    name = sqlc.arg(name)::text,
    slug = sqlc.arg(slug)::text,
    email = sqlc.arg(email)::text,
    cluster_id = COALESCE(sqlc.narg(cluster_id)::uuid, cluster_id)
WHERE id = sqlc.arg(id)::uuid
RETURNING id, name, slug, email, cluster_id;

-- Follows a slug change: every alias the team holds under an earlier namespace
-- is also given under the new one, so a template stays reachable by the new
-- slug while the old names keep working. Aliases without a namespace are
-- global and stay out of it. Where two earlier namespaces hold the same alias,
-- the one under the slug being replaced wins.
--
-- Only live templates are copied, and FOR SHARE serializes against a
-- concurrent template delete: the delete either waits and then releases the
-- copies too, or has committed and its template is skipped.
--
-- An alias the new namespace already holds is left as it is rather than
-- failing the reconcile, which the caller would only retry. Returns the
-- aliases skipped because the new namespace gives them to another template;
-- the outer select reads the table as it was before the insert.
-- name: CopyTeamAliasesToNamespace :many
WITH live_envs AS MATERIALIZED (
    SELECT id
    FROM public.envs
    WHERE team_id = sqlc.arg(team_id)::uuid AND deleted_at IS NULL
    FOR SHARE
),
candidates AS (
    SELECT DISTINCT ON (ea.alias) ea.alias, ea.env_id, ea.is_renamable
    FROM public.env_aliases ea
    JOIN live_envs e ON e.id = ea.env_id
    WHERE ea.namespace IS NOT NULL
      AND ea.namespace <> sqlc.arg(namespace)::text
    ORDER BY ea.alias, ea.namespace = sqlc.arg(previous_namespace)::text DESC, ea.namespace
),
copied AS (
    INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace)
    SELECT alias, env_id, is_renamable, sqlc.arg(namespace)::text
    FROM candidates
    ON CONFLICT (alias, namespace) DO NOTHING
)
SELECT c.alias
FROM candidates c
JOIN public.env_aliases existing
    ON existing.alias = c.alias
   AND existing.namespace = sqlc.arg(namespace)::text
   AND existing.env_id <> c.env_id;
