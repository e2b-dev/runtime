-- +goose Up
-- +goose StatementBegin

ALTER TABLE "public"."tiers"
    ADD COLUMN "events_ttl_days" bigint NOT NULL DEFAULT 7,
    ADD CONSTRAINT "tiers_events_ttl_days_check" CHECK (events_ttl_days > 0);

ALTER TABLE "public"."addons"
    ADD COLUMN "extra_events_ttl_days" bigint NOT NULL DEFAULT 0;

CREATE OR REPLACE VIEW "team_limits"
WITH (security_invoker=on) AS
SELECT
    t.id,
    tier.max_length_hours,
    (tier.concurrent_instances + a.extra_concurrent_sandboxes) as concurrent_sandboxes,
    (tier.concurrent_template_builds + a.extra_concurrent_template_builds) as concurrent_template_builds,
    (tier.max_vcpu + a.extra_max_vcpu) as max_vcpu,
    (tier.max_ram_mb + a.extra_max_ram_mb) as max_ram_mb,
    (tier.disk_mb + a.extra_disk_mb) as disk_mb,
    (tier.events_ttl_days + a.extra_events_ttl_days) as events_ttl_days
FROM "public".teams t
JOIN "public"."tiers" tier on t.tier = tier.id
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(extra_concurrent_sandboxes),0)::bigint           as extra_concurrent_sandboxes,
           COALESCE(SUM(extra_concurrent_template_builds),0)::bigint     as extra_concurrent_template_builds,
           COALESCE(SUM(extra_max_vcpu),0)::bigint                       as extra_max_vcpu,
           COALESCE(SUM(extra_max_ram_mb),0)::bigint                     as extra_max_ram_mb,
           COALESCE(SUM(extra_disk_mb),0)::bigint                        as extra_disk_mb,
           COALESCE(SUM(extra_events_ttl_days),0)::bigint                as extra_events_ttl_days
    FROM "public"."addons" addon
    WHERE addon.team_id = t.id
      AND addon.valid_from <= now()
      AND (addon.valid_to IS NULL OR addon.valid_to > now())
    ) a ON true;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS "team_limits";

CREATE VIEW "team_limits"
WITH (security_invoker=on) AS
SELECT
    t.id,
    tier.max_length_hours,
    (tier.concurrent_instances + a.extra_concurrent_sandboxes) as concurrent_sandboxes,
    (tier.concurrent_template_builds + a.extra_concurrent_template_builds) as concurrent_template_builds,
    (tier.max_vcpu + a.extra_max_vcpu) as max_vcpu,
    (tier.max_ram_mb + a.extra_max_ram_mb) as max_ram_mb,
    (tier.disk_mb + a.extra_disk_mb) as disk_mb
FROM "public".teams t
JOIN "public"."tiers" tier on t.tier = tier.id
LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(extra_concurrent_sandboxes),0)::bigint           as extra_concurrent_sandboxes,
           COALESCE(SUM(extra_concurrent_template_builds),0)::bigint     as extra_concurrent_template_builds,
           COALESCE(SUM(extra_max_vcpu),0)::bigint                       as extra_max_vcpu,
           COALESCE(SUM(extra_max_ram_mb),0)::bigint                     as extra_max_ram_mb,
           COALESCE(SUM(extra_disk_mb),0)::bigint                        as extra_disk_mb
    FROM "public"."addons" addon
    WHERE addon.team_id = t.id
      AND addon.valid_from <= now()
      AND (addon.valid_to IS NULL OR addon.valid_to > now())
    ) a ON true;

ALTER TABLE "public"."addons" DROP COLUMN IF EXISTS "extra_events_ttl_days";

ALTER TABLE "public"."tiers"
    DROP CONSTRAINT IF EXISTS "tiers_events_ttl_days_check",
    DROP COLUMN IF EXISTS "events_ttl_days";

-- +goose StatementEnd
