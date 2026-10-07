-- Advance in the same transaction as team_billing_profiles so retries cannot
-- skip an unwritten state. Same fence as ApplyProjectLimitsProjection.
-- name: ApplyBillingProfileProjection :one
WITH changed AS (
    INSERT INTO projection.billing_profiles (project_id, revision, decided_at)
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
    WHERE projection.billing_profiles.revision < EXCLUDED.revision
    RETURNING project_id
)
SELECT EXISTS (SELECT 1 FROM changed) AS applied;

-- Every column is supplied on every call: the caller sends a complete profile,
-- so there is no partial update to merge. Which deliveries reach this statement
-- is the ledger's decision; nothing here compares revisions.
-- name: UpsertTeamBillingProfile :exec
INSERT INTO public.team_billing_profiles (
    team_id,
    has_payment_method,
    enterprise,
    plan,
    updated_at
) VALUES (
    sqlc.arg(team_id)::uuid,
    sqlc.arg(has_payment_method)::boolean,
    sqlc.arg(enterprise)::boolean,
    sqlc.narg(plan)::text,
    now()
)
ON CONFLICT (team_id) DO UPDATE SET
    has_payment_method = EXCLUDED.has_payment_method,
    enterprise         = EXCLUDED.enterprise,
    plan               = EXCLUDED.plan,
    updated_at         = now();
