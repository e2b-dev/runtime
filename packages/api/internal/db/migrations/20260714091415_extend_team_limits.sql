-- +goose Up
-- +goose StatementBegin

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

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- PostgreSQL cannot remove appended view columns with CREATE OR REPLACE, so restore the previous
-- definition transactionally. Concurrent readers cannot observe the view between these statements.
DROP VIEW IF EXISTS "public"."team_limits";

CREATE VIEW "public"."team_limits"
WITH (security_invoker=on) AS
SELECT
    t.id,
    tier.max_length_hours,
    (tier.concurrent_instances + a.extra_concurrent_sandboxes) AS concurrent_sandboxes,
    (tier.concurrent_template_builds + a.extra_concurrent_template_builds) AS concurrent_template_builds,
    (tier.max_vcpu + a.extra_max_vcpu) AS max_vcpu,
    (tier.max_ram_mb + a.extra_max_ram_mb) AS max_ram_mb,
    (tier.disk_mb + a.extra_disk_mb) AS disk_mb,
    (tier.events_ttl_days + a.extra_events_ttl_days) AS events_ttl_days
FROM "public"."teams" t
JOIN "public"."tiers" tier ON t.tier = tier.id
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(extra_concurrent_sandboxes), 0)::bigint AS extra_concurrent_sandboxes,
           COALESCE(SUM(extra_concurrent_template_builds), 0)::bigint AS extra_concurrent_template_builds,
           COALESCE(SUM(extra_max_vcpu), 0)::bigint AS extra_max_vcpu,
           COALESCE(SUM(extra_max_ram_mb), 0)::bigint AS extra_max_ram_mb,
           COALESCE(SUM(extra_disk_mb), 0)::bigint AS extra_disk_mb,
           COALESCE(SUM(extra_events_ttl_days), 0)::bigint AS extra_events_ttl_days
    FROM "public"."addons" addon
    WHERE addon.team_id = t.id
      AND addon.valid_from <= now()
      AND (addon.valid_to IS NULL OR addon.valid_to > now())
) a ON true;

-- +goose StatementEnd
