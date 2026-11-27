-- +goose Up
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS public.addons, public.tiers CASCADE;

-- +goose Down
SET LOCAL lock_timeout = '5s';

CREATE TABLE public.tiers (
    id text PRIMARY KEY,
    name text NOT NULL,
    disk_mb bigint NOT NULL DEFAULT '512'::bigint,
    concurrent_instances bigint NOT NULL,
    max_length_hours bigint NOT NULL,
    max_vcpu bigint NOT NULL DEFAULT '8'::bigint,
    max_ram_mb bigint NOT NULL DEFAULT '8192'::bigint,
    concurrent_template_builds bigint NOT NULL DEFAULT 20,
    events_ttl_days bigint NOT NULL DEFAULT 7,
    default_free_disk_size_mb bigint NOT NULL,
    max_disk_size_mb bigint NOT NULL,
    max_free_disk_size_mb bigint,
    api_team_rps_list bigint NOT NULL DEFAULT 0,
    api_team_rps_delete bigint NOT NULL DEFAULT 0,
    max_secrets integer NOT NULL DEFAULT 100,
    CONSTRAINT tiers_api_team_rps_delete_check CHECK (api_team_rps_delete >= 0),
    CONSTRAINT tiers_api_team_rps_list_check CHECK (api_team_rps_list >= 0),
    CONSTRAINT tiers_concurrent_sessions_check CHECK (concurrent_instances > 0),
    CONSTRAINT tiers_concurrent_template_builds_check CHECK (concurrent_template_builds > 0),
    CONSTRAINT tiers_default_free_disk_size_lte_max_check CHECK (default_free_disk_size_mb <= max_disk_size_mb),
    CONSTRAINT tiers_default_free_disk_size_mb_check CHECK (default_free_disk_size_mb >= 0),
    CONSTRAINT tiers_disk_mb_check CHECK (disk_mb > 0),
    CONSTRAINT tiers_events_ttl_days_check CHECK (events_ttl_days > 0),
    CONSTRAINT tiers_max_disk_size_mb_check CHECK (max_disk_size_mb > 0),
    CONSTRAINT tiers_max_secrets_check CHECK (max_secrets > 0)
);

COMMENT ON COLUMN public.tiers.concurrent_instances IS 'The number of instances the team can run concurrently';
COMMENT ON COLUMN public.tiers.concurrent_template_builds IS 'The number of concurrent template builds the team can run';

INSERT INTO public.tiers (
    id, name, disk_mb, concurrent_instances, max_length_hours, max_vcpu, max_ram_mb,
    concurrent_template_builds, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
    api_team_rps_list, api_team_rps_delete, max_secrets
)
SELECT 'base_v1', 'Base tier', disk_mb, concurrent_sandboxes, max_length_hours, max_vcpu, max_ram_mb,
    concurrent_template_builds, events_ttl_days, default_free_disk_size_mb, max_disk_size_mb,
    api_team_rps_list, api_team_rps_delete, max_secrets
FROM public.default_limits;

CREATE TABLE public.addons (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id uuid NOT NULL CONSTRAINT addons_teams_addons REFERENCES public.teams (id) ON DELETE CASCADE,
    name text NOT NULL,
    description text,
    extra_concurrent_sandboxes bigint NOT NULL DEFAULT 0,
    extra_concurrent_template_builds bigint NOT NULL DEFAULT 0,
    extra_max_vcpu bigint NOT NULL DEFAULT 0,
    extra_max_ram_mb bigint NOT NULL DEFAULT 0,
    extra_disk_mb bigint NOT NULL DEFAULT 0,
    valid_from timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to timestamptz,
    added_by uuid NOT NULL CONSTRAINT addons_users_addons REFERENCES public.users (id),
    idempotency_key text,
    extra_events_ttl_days bigint NOT NULL DEFAULT 0,
    extra_max_disk_size_mb bigint,
    extra_max_free_disk_size_mb bigint,
    extra_api_team_rps_list bigint NOT NULL DEFAULT 0,
    extra_api_team_rps_delete bigint NOT NULL DEFAULT 0,
    extra_max_secrets integer NOT NULL DEFAULT 0,
    CONSTRAINT addons_extra_api_team_rps_delete_check CHECK (extra_api_team_rps_delete >= 0),
    CONSTRAINT addons_extra_api_team_rps_list_check CHECK (extra_api_team_rps_list >= 0),
    CONSTRAINT addons_extra_max_disk_size_mb_check CHECK (extra_max_disk_size_mb >= 0),
    CONSTRAINT addons_extra_max_secrets_check CHECK (extra_max_secrets >= 0),
    CONSTRAINT addons_valid_dates_check CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE UNIQUE INDEX addons_idempotency_key_uidx ON public.addons (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX addons_team_id_idx ON public.addons (team_id);
