-- +goose Up
-- The two AFTER INSERT triggers on env_build_assignments each filled one
-- env_builds column with `UPDATE ... WHERE id = NEW.build_id AND <col> IS NULL`.
-- When statistics under-count the rows where team_id is NULL, the planner can
-- drive that UPDATE from a team_id-leading index instead of the primary key, and
-- every insert into env_build_assignments then walks the index's NULL entries,
-- including dead ones that vacuum cannot remove yet. The cost per insert grows
-- with that backlog.
--
-- One function now fills both columns in a single UPDATE whose only indexable
-- predicate is the primary key: num_nulls() cannot be used as an index
-- condition. Each column is still filled only while it is NULL, from the first
-- assignment, and an insert writes one env_builds row version instead of two.
-- The second function becomes a no-op; both triggers stay in place.
--
-- CREATE OR REPLACE keeps each function's oid, owner and ACL, so the triggers
-- pick up the new bodies at commit. No lock is taken on either table.
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

-- +goose Down
-- The bodies from 20260204172712_remove_build_assignment_triggers.sql and
-- 20260218120000_add_team_id_to_env_builds.sql.
SET LOCAL lock_timeout = '2s';

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.backfill_env_id_from_assignment()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE env_builds SET env_id = NEW.env_id WHERE id = NEW.build_id AND env_id IS NULL;
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
    UPDATE env_builds
    SET team_id = (SELECT team_id FROM envs WHERE id = NEW.env_id)
    WHERE id = NEW.build_id AND team_id IS NULL;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
