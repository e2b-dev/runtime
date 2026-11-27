-- +goose NO TRANSACTION
-- +goose Up
-- The cluster service holds the cluster records, and the API checks a team's
-- new cluster there, so nothing here refers to public.clusters any more. Each
-- statement locks its tables exclusively; without a transaction, each lock is
-- released before the next is requested.
SET lock_timeout = '5s';

DROP TRIGGER IF EXISTS teams_cluster_not_deleting ON public.teams;
ALTER TABLE public.teams DROP CONSTRAINT IF EXISTS teams_cluster_id_fkey;
ALTER TABLE public.envs DROP CONSTRAINT IF EXISTS envs_cluster_id_fkey;

RESET lock_timeout;

DROP FUNCTION IF EXISTS public.refuse_team_on_deleting_cluster();

-- +goose Down
-- Every step can run again: a rollback the lock timeout stops keeps what it
-- already added, and its retry starts from the top.
SET lock_timeout = '5s';

-- NOT VALID: rows written since the drop may name clusters this table lacks.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'teams_cluster_id_fkey' AND conrelid = 'public.teams'::regclass) THEN
        ALTER TABLE public.teams
            ADD CONSTRAINT teams_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES public.clusters (id) NOT VALID;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'envs_cluster_id_fkey' AND conrelid = 'public.envs'::regclass) THEN
        ALTER TABLE public.envs
            ADD CONSTRAINT envs_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES public.clusters (id) NOT VALID;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.refuse_team_on_deleting_cluster()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $refuse_team_on_deleting_cluster$
DECLARE
    deleting boolean;
BEGIN
    IF NEW.cluster_id IS NULL OR (TG_OP = 'UPDATE' AND NEW.cluster_id IS NOT DISTINCT FROM OLD.cluster_id) THEN
        RETURN NEW;
    END IF;
    SELECT deletion_requested_at IS NOT NULL INTO deleting
    FROM public.clusters
    WHERE id = NEW.cluster_id
    FOR KEY SHARE;
    IF deleting THEN
        RAISE EXCEPTION 'cluster % is being deleted', NEW.cluster_id
            USING ERRCODE = 'check_violation', CONSTRAINT = 'teams_cluster_not_deleting';
    END IF;
    RETURN NEW;
END
$refuse_team_on_deleting_cluster$;
-- +goose StatementEnd

CREATE OR REPLACE TRIGGER teams_cluster_not_deleting
    BEFORE INSERT OR UPDATE OF cluster_id ON public.teams
    FOR EACH ROW EXECUTE FUNCTION public.refuse_team_on_deleting_cluster();

RESET lock_timeout;
