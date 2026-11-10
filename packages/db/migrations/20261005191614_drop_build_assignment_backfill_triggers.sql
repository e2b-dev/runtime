-- +goose Up
-- Every write path now sets env_builds.env_id and team_id when it inserts the
-- build, so these AFTER INSERT triggers on env_build_assignments have nothing
-- left to fill. Dropping them removes an env_builds lookup from every
-- assignment insert.
SET LOCAL lock_timeout = '2s';

DROP TRIGGER IF EXISTS trigger_backfill_env_id ON public.env_build_assignments;
DROP TRIGGER IF EXISTS trigger_backfill_team_id ON public.env_build_assignments;
DROP FUNCTION IF EXISTS public.backfill_env_id_from_assignment();
DROP FUNCTION IF EXISTS public.backfill_team_id_from_assignment();

-- +goose Down
-- The functions as 20261002234434_merge_build_assignment_backfill_triggers.sql
-- left them, and the triggers from 20260204172712 and 20260218120000.
SET LOCAL lock_timeout = '2s';

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.backfill_env_id_from_assignment()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.env_builds
       SET env_id  = COALESCE(env_id, NEW.env_id),
           team_id = COALESCE(team_id, (SELECT e.team_id FROM public.envs e WHERE e.id = NEW.env_id))
     WHERE id = NEW.build_id
       AND num_nulls(env_id, team_id) > 0;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.backfill_team_id_from_assignment()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- backfill_env_id_from_assignment() fills team_id as well.
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trigger_backfill_env_id
    AFTER INSERT ON public.env_build_assignments
    FOR EACH ROW EXECUTE FUNCTION public.backfill_env_id_from_assignment();

CREATE TRIGGER trigger_backfill_team_id
    AFTER INSERT ON public.env_build_assignments
    FOR EACH ROW EXECUTE FUNCTION public.backfill_team_id_from_assignment();
