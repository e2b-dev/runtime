-- +goose Up
-- +goose StatementBegin
-- Once a cluster's deletion is recorded, no team is newly assigned to it, so
-- the teams the deletion found are the last ones it has to delete. The key
-- share lock waits for a deletion request holding the cluster row and then
-- reads what it committed: an assignment either commits before the deletion
-- is recorded, where the deleting caller's later listing finds it, or is
-- refused. Rewriting a team's existing cluster is not a new assignment.
CREATE FUNCTION public.refuse_team_on_deleting_cluster()
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

CREATE TRIGGER teams_cluster_not_deleting
    BEFORE INSERT OR UPDATE OF cluster_id ON public.teams
    FOR EACH ROW EXECUTE FUNCTION public.refuse_team_on_deleting_cluster();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER teams_cluster_not_deleting ON public.teams;
DROP FUNCTION public.refuse_team_on_deleting_cluster();
-- +goose StatementEnd
