-- +goose Up
-- +goose StatementBegin

-- Effective limits pushed in by the service that owns projects, replacing the
-- arithmetic team_limits does today.
--
-- The view derives a team's limits by joining tiers to addons. addons is
-- written only by billing and read by nothing in this repo except that view,
-- which makes the sandbox-creation read path depend on a table this side does
-- not own. Pushing absolutes inverts that: the owner computes, this side reads
-- one local table.
--
-- Nothing writes this table yet. While it is empty every COALESCE below falls
-- through to the existing expression, so the view returns exactly what it
-- returned before -- which is the point of landing it separately from anything
-- that populates it.
CREATE TABLE IF NOT EXISTS "public"."project_limits" (
    team_id                    uuid PRIMARY KEY REFERENCES "public"."teams" ("id") ON DELETE CASCADE,

    -- bigint throughout to match what the view already yields; a narrower type
    -- here would silently clamp a pushed value.
    max_length_hours           bigint NOT NULL CHECK (max_length_hours >= 0),
    concurrent_sandboxes       bigint NOT NULL CHECK (concurrent_sandboxes >= 0),
    concurrent_template_builds bigint NOT NULL CHECK (concurrent_template_builds >= 0),
    max_vcpu                   bigint NOT NULL CHECK (max_vcpu >= 0),
    max_ram_mb                 bigint NOT NULL CHECK (max_ram_mb >= 0),
    disk_mb                    bigint NOT NULL CHECK (disk_mb >= 0),
    events_ttl_days            bigint NOT NULL CHECK (events_ttl_days >= 0),
    default_free_disk_size_mb  bigint NOT NULL CHECK (default_free_disk_size_mb >= 0),
    max_disk_size_mb           bigint NOT NULL CHECK (max_disk_size_mb >= 0),

    updated_at                 timestamptz NOT NULL DEFAULT now(),

    -- Mirrors tiers_default_free_disk_size_lte_max_check. The view reads both
    -- columns straight from here, so without this an override could surface a
    -- free allowance above the ceiling -- a pair tiers cannot produce and that
    -- readers have never had to handle.
    CONSTRAINT project_limits_default_free_disk_size_lte_max_check
        CHECK (default_free_disk_size_mb <= max_disk_size_mb)
);

-- tiers CHECKs the same columns as > 0. These allow 0 deliberately: this table
-- is a push target, and rejecting a value the caller considers valid turns a
-- product decision into a retry loop that never drains. Negatives are always a
-- bug, so they stay rejected. Every column is NOT NULL, so a row overrides all
-- nine or does not exist -- there is no half-overridden team to reason about.
--
-- The cross-column check above is different in kind: an incoherent pair is not
-- a policy this side should defer to, and it is the one invariant every reader
-- of these two columns has been able to assume since tiers gained them.

CREATE OR REPLACE VIEW "public"."team_limits"
WITH (security_invoker=on) AS
SELECT
    t.id,
    COALESCE(pl.max_length_hours, tier.max_length_hours) AS max_length_hours,
    COALESCE(pl.concurrent_sandboxes, tier.concurrent_instances + a.extra_concurrent_sandboxes) AS concurrent_sandboxes,
    COALESCE(pl.concurrent_template_builds, tier.concurrent_template_builds + a.extra_concurrent_template_builds) AS concurrent_template_builds,
    COALESCE(pl.max_vcpu, tier.max_vcpu + a.extra_max_vcpu) AS max_vcpu,
    COALESCE(pl.max_ram_mb, tier.max_ram_mb + a.extra_max_ram_mb) AS max_ram_mb,
    COALESCE(pl.disk_mb, tier.disk_mb + a.extra_disk_mb) AS disk_mb,
    COALESCE(pl.events_ttl_days, tier.events_ttl_days + a.extra_events_ttl_days) AS events_ttl_days,
    COALESCE(pl.default_free_disk_size_mb, (tier.default_free_disk_size_mb + a.extra_disk_mb))::bigint AS default_free_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, (tier.max_disk_size_mb + a.extra_max_disk_size_mb))::bigint AS max_disk_size_mb
FROM "public"."teams" t
JOIN "public"."tiers" tier ON t.tier = tier.id
LEFT JOIN "public"."project_limits" pl ON pl.team_id = t.id
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(extra_concurrent_sandboxes), 0)::bigint AS extra_concurrent_sandboxes,
           COALESCE(SUM(extra_concurrent_template_builds), 0)::bigint AS extra_concurrent_template_builds,
           COALESCE(SUM(extra_max_vcpu), 0)::bigint AS extra_max_vcpu,
           COALESCE(SUM(extra_max_ram_mb), 0)::bigint AS extra_max_ram_mb,
           COALESCE(SUM(extra_disk_mb), 0)::bigint AS extra_disk_mb,
           COALESCE(SUM(extra_events_ttl_days), 0)::bigint AS extra_events_ttl_days,
           COALESCE(SUM(COALESCE(extra_max_disk_size_mb, extra_disk_mb)), 0)::bigint AS extra_max_disk_size_mb
    FROM "public"."addons" addon
    WHERE addon.team_id = t.id
      AND addon.valid_from <= now()
      AND (addon.valid_to IS NULL OR addon.valid_to > now())
) a ON true;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Restore the pre-override definition, then drop the table it referenced.
-- CREATE OR REPLACE suffices because the column list and types are unchanged.
CREATE OR REPLACE VIEW "public"."team_limits"
WITH (security_invoker=on) AS
SELECT
    t.id,
    tier.max_length_hours,
    (tier.concurrent_instances + a.extra_concurrent_sandboxes) AS concurrent_sandboxes,
    (tier.concurrent_template_builds + a.extra_concurrent_template_builds) AS concurrent_template_builds,
    (tier.max_vcpu + a.extra_max_vcpu) AS max_vcpu,
    (tier.max_ram_mb + a.extra_max_ram_mb) AS max_ram_mb,
    (tier.disk_mb + a.extra_disk_mb) AS disk_mb,
    (tier.events_ttl_days + a.extra_events_ttl_days) AS events_ttl_days,
    (tier.default_free_disk_size_mb + a.extra_disk_mb)::bigint AS default_free_disk_size_mb,
    (tier.max_disk_size_mb + a.extra_max_disk_size_mb)::bigint AS max_disk_size_mb
FROM "public"."teams" t
JOIN "public"."tiers" tier ON t.tier = tier.id
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(extra_concurrent_sandboxes), 0)::bigint AS extra_concurrent_sandboxes,
           COALESCE(SUM(extra_concurrent_template_builds), 0)::bigint AS extra_concurrent_template_builds,
           COALESCE(SUM(extra_max_vcpu), 0)::bigint AS extra_max_vcpu,
           COALESCE(SUM(extra_max_ram_mb), 0)::bigint AS extra_max_ram_mb,
           COALESCE(SUM(extra_disk_mb), 0)::bigint AS extra_disk_mb,
           COALESCE(SUM(extra_events_ttl_days), 0)::bigint AS extra_events_ttl_days,
           COALESCE(SUM(COALESCE(extra_max_disk_size_mb, extra_disk_mb)), 0)::bigint AS extra_max_disk_size_mb
    FROM "public"."addons" addon
    WHERE addon.team_id = t.id
      AND addon.valid_from <= now()
      AND (addon.valid_to IS NULL OR addon.valid_to > now())
) a ON true;

DROP TABLE IF EXISTS "public"."project_limits";

-- +goose StatementEnd
