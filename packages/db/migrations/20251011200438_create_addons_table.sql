-- +goose Up
-- +goose StatementBegin

-- Create "addons" table
CREATE TABLE IF NOT EXISTS "public"."addons"
(
    "id"                                uuid        NOT NULL DEFAULT gen_random_uuid(),
    "team_id"                           uuid        NOT NULL,
    "name"                              text        NOT NULL,
    "description"                       text        NULL,
    "extra_concurrent_sandboxes"        bigint      NOT NULL DEFAULT 0,
    "extra_concurrent_template_builds"  bigint      NOT NULL DEFAULT 0,
    "extra_max_vcpu"                    bigint      NOT NULL DEFAULT 0,
    "extra_max_ram_mb"                  bigint      NOT NULL DEFAULT 0,
    "extra_disk_mb"                     bigint      NOT NULL DEFAULT 0,
    "valid_from"                        timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "valid_to"                          timestamptz NULL ,
    "added_by"                          uuid        NOT NULL,
    PRIMARY KEY ("id"),
    CONSTRAINT "addons_teams_addons" FOREIGN KEY ("team_id") REFERENCES "public"."teams" ("id") ON UPDATE NO ACTION ON DELETE CASCADE,
    CONSTRAINT "addons_users_addons" FOREIGN KEY ("added_by") REFERENCES "auth"."users" ("id") ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT "addons_valid_dates_check" CHECK (valid_to IS NULL OR valid_to > valid_from)
);

-- Create index on team_id for faster lookups
CREATE INDEX IF NOT EXISTS "addons_team_id_idx" ON "public"."addons" ("team_id");


CREATE OR REPLACE VIEW "team_limits"
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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP VIEW IF EXISTS "team_limits";
DROP TABLE IF EXISTS "public"."addons" CASCADE;

-- +goose StatementEnd
