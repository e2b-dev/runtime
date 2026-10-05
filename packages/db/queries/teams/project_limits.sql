-- Taken first, so the branch is decided by whether the project exists rather
-- than by an insert failing.
-- name: LockManagedProject :one
SELECT id FROM public.teams
WHERE id = sqlc.arg(id)::uuid
FOR UPDATE;

-- Advances the ledger that decides whether a delivery gets to write, and
-- answers whether it did.
--
-- The guard is what makes a delayed retry safe: a delivery carrying a revision
-- at or below the one recorded arrived after a newer one, so it is dropped and
-- the values it carried are never written. The caller cannot enforce this on
-- its own -- it fences what it sends, and two deliveries in flight arrive in
-- whichever order the network gives them.
--
-- The conflict action takes a row lock held to commit, so a second delivery for
-- the same project waits here and is compared against the winner's revision
-- rather than against what it read.
-- name: ApplyProjectLimitsProjection :one
WITH changed AS (
    INSERT INTO projection.project_limits (project_id, revision, decided_at)
    VALUES (
        sqlc.arg(project_id)::uuid,
        sqlc.arg(revision)::bigint,
        sqlc.narg(decided_at)::timestamptz
    )
    ON CONFLICT (project_id) DO UPDATE
    SET
        revision = EXCLUDED.revision,
        decided_at = EXCLUDED.decided_at,
        updated_at = now()
    WHERE projection.project_limits.revision < EXCLUDED.revision
    RETURNING project_id
)
SELECT EXISTS (SELECT 1 FROM changed) AS applied;

-- UpsertProjectLimits records a project's effective limits, which the
-- team_limits view reads in preference to the tier-plus-addons arithmetic.
--
-- Every column is supplied on every call: the caller sends a complete set, so
-- there is no partial update to merge and no prior row to read first.
--
-- max_disk_size_mb and max_free_disk_size_mb are the same ceiling under the old
-- and the new name, and the table holds them equal. Both are written here so a
-- receiver of this version keeps the compatibility bridge in application code.
--
-- Which deliveries reach this statement is the ledger's decision, and the two
-- writes share a transaction. Nothing here compares revisions.
-- name: UpsertProjectLimits :exec
INSERT INTO public.project_limits (
    team_id,
    max_length_hours,
    concurrent_sandboxes,
    concurrent_template_builds,
    max_vcpu,
    max_ram_mb,
    disk_mb,
    events_ttl_days,
    default_free_disk_size_mb,
    max_disk_size_mb,
    max_free_disk_size_mb,
    api_team_rps_list,
    updated_at
) VALUES (
    sqlc.arg(team_id)::uuid,
    sqlc.arg(max_length_hours)::bigint,
    sqlc.arg(concurrent_sandboxes)::bigint,
    sqlc.arg(concurrent_template_builds)::bigint,
    sqlc.arg(max_vcpu)::bigint,
    sqlc.arg(max_ram_mb)::bigint,
    sqlc.arg(disk_mb)::bigint,
    sqlc.arg(events_ttl_days)::bigint,
    sqlc.arg(default_free_disk_size_mb)::bigint,
    sqlc.arg(max_free_disk_size_mb)::bigint,
    sqlc.arg(max_free_disk_size_mb)::bigint,
    sqlc.arg(api_team_rps_list)::bigint,
    now()
)
ON CONFLICT (team_id) DO UPDATE SET
    max_length_hours           = EXCLUDED.max_length_hours,
    concurrent_sandboxes       = EXCLUDED.concurrent_sandboxes,
    concurrent_template_builds = EXCLUDED.concurrent_template_builds,
    max_vcpu                   = EXCLUDED.max_vcpu,
    max_ram_mb                 = EXCLUDED.max_ram_mb,
    disk_mb                    = EXCLUDED.disk_mb,
    events_ttl_days            = EXCLUDED.events_ttl_days,
    default_free_disk_size_mb  = EXCLUDED.default_free_disk_size_mb,
    max_disk_size_mb           = EXCLUDED.max_disk_size_mb,
    max_free_disk_size_mb      = EXCLUDED.max_free_disk_size_mb,
    api_team_rps_list          = EXCLUDED.api_team_rps_list,
    updated_at                 = now();
