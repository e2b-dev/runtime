-- +goose Up
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

-- The limits of a team without a project_limits row. The team limits query
-- cross-joins this table, so it holds exactly one row: the key admits only
-- true, and the trigger below refuses to remove it, since a team that resolves
-- no limits cannot authenticate.
--
-- Each CHECK is the stricter of the tiers and project_limits checks on that
-- column, so a default is a value both tables accept.
CREATE TABLE public.default_limits (
    id                         boolean PRIMARY KEY DEFAULT true CHECK (id),
    max_length_hours           bigint NOT NULL CHECK (max_length_hours >= 0),
    concurrent_sandboxes       bigint NOT NULL CHECK (concurrent_sandboxes > 0),
    concurrent_template_builds bigint NOT NULL CHECK (concurrent_template_builds > 0),
    max_vcpu                   bigint NOT NULL CHECK (max_vcpu >= 0),
    max_ram_mb                 bigint NOT NULL CHECK (max_ram_mb >= 0),
    disk_mb                    bigint NOT NULL CHECK (disk_mb > 0),
    events_ttl_days            bigint NOT NULL CHECK (events_ttl_days > 0),
    default_free_disk_size_mb  bigint NOT NULL CHECK (default_free_disk_size_mb >= 0),
    -- Served under both max_disk_size_mb and max_free_disk_size_mb, which name
    -- one ceiling.
    max_disk_size_mb           bigint NOT NULL CHECK (max_disk_size_mb > 0),
    api_team_rps_list          bigint NOT NULL CHECK (api_team_rps_list >= 0),
    api_team_rps_delete        bigint NOT NULL CHECK (api_team_rps_delete >= 0),
    max_secrets                bigint NOT NULL CHECK (max_secrets > 0),

    CONSTRAINT default_limits_default_free_disk_size_lte_max_check
        CHECK (default_free_disk_size_mb <= max_disk_size_mb)
);

CREATE FUNCTION public.default_limits_keep_row()
    RETURNS trigger
    LANGUAGE plpgsql
AS $keep_row$
BEGIN
    RAISE EXCEPTION 'public.default_limits must keep its row; update it instead';
END
$keep_row$;

CREATE TRIGGER default_limits_keep_row
    BEFORE DELETE OR TRUNCATE ON public.default_limits
    FOR EACH STATEMENT EXECUTE FUNCTION public.default_limits_keep_row();

-- base_v1 is the tier every team is created on, so a deployment that tuned it
-- keeps those values for new teams, and team_limits agrees with the query for
-- them while both are live.
DO $seed$
BEGIN
    INSERT INTO public.default_limits (
        max_length_hours, concurrent_sandboxes, concurrent_template_builds,
        max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
        default_free_disk_size_mb, max_disk_size_mb,
        api_team_rps_list, api_team_rps_delete, max_secrets
    )
    SELECT max_length_hours, concurrent_instances, concurrent_template_builds,
           max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
           default_free_disk_size_mb, max_disk_size_mb,
           api_team_rps_list, api_team_rps_delete, max_secrets
    FROM public.tiers
    WHERE id = 'base_v1';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'tier base_v1 is missing; public.default_limits is seeded from it';
    END IF;
END
$seed$;

-- The query reads neither tiers nor add-ons, so every team without a row is
-- pinned to what team_limits resolves for it now. A pushed row, including one
-- pushed while this runs, stays. A team whose sum breaks a project_limits CHECK
-- fails the migration rather than being clamped.
INSERT INTO public.project_limits (
    team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
    max_vcpu, max_ram_mb, disk_mb, events_ttl_days,
    default_free_disk_size_mb, max_disk_size_mb, max_free_disk_size_mb,
    api_team_rps_list, api_team_rps_delete, max_secrets
)
SELECT tl.id, tl.max_length_hours, tl.concurrent_sandboxes, tl.concurrent_template_builds,
       tl.max_vcpu, tl.max_ram_mb, tl.disk_mb, tl.events_ttl_days,
       tl.default_free_disk_size_mb, tl.max_disk_size_mb, tl.max_free_disk_size_mb,
       tl.api_team_rps_list, tl.api_team_rps_delete, tl.max_secrets
FROM public.team_limits tl
ON CONFLICT (team_id) DO NOTHING;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

-- The rows the backfill inserted stay: nothing tells them from pushed ones, and
-- they hold what team_limits resolved for their teams before the Up.
DROP TABLE public.default_limits;
DROP FUNCTION public.default_limits_keep_row();
-- +goose StatementEnd
