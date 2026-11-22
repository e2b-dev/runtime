-- +goose Up
-- +goose StatementBegin

-- The existing disk ceiling limits requested free space. Add its clearer name
-- beside every legacy source column while old readers and writers remain live.
-- These columns stay nullable until application-level dual writes are deployed.
ALTER TABLE "public"."tiers"
    ADD COLUMN max_free_disk_size_mb bigint;

ALTER TABLE "public"."addons"
    ADD COLUMN extra_max_free_disk_size_mb bigint;

ALTER TABLE "public"."project_limits"
    ADD COLUMN max_free_disk_size_mb bigint;

-- Append the canonical output without consulting the empty canonical storage.
-- The legacy columns remain authoritative throughout the compatibility rollout.
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
    COALESCE(pl.max_disk_size_mb, (tier.max_disk_size_mb + a.extra_max_disk_size_mb))::bigint AS max_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, (tier.max_disk_size_mb + a.extra_max_disk_size_mb))::bigint AS max_free_disk_size_mb
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

-- PostgreSQL cannot remove an appended view column with CREATE OR REPLACE.
DROP VIEW IF EXISTS "public"."team_limits";

CREATE VIEW "public"."team_limits"
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

ALTER TABLE "public"."project_limits"
    DROP COLUMN max_free_disk_size_mb;

ALTER TABLE "public"."addons"
    DROP COLUMN extra_max_free_disk_size_mb;

ALTER TABLE "public"."tiers"
    DROP COLUMN max_free_disk_size_mb;

-- +goose StatementEnd
