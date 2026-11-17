-- +goose Up
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

-- A write that sets status, or an insert without a group, gets the group
-- derived from it. A write that sets only status_group gets status filled in,
-- so callers no longer need to name the column before it is dropped.
CREATE OR REPLACE FUNCTION public.compute_status_group() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF (TG_OP = 'INSERT' AND NEW.status_group IS NULL)
     OR (TG_OP = 'UPDATE' AND NEW.status IS DISTINCT FROM OLD.status) THEN
    NEW.status_group := CASE
      WHEN NEW.status IN ('pending', 'waiting') THEN 'pending'
      WHEN NEW.status IN ('in_progress', 'building', 'snapshotting') THEN 'in_progress'
      WHEN NEW.status IN ('ready', 'uploaded', 'success') THEN 'ready'
      ELSE 'failed'
    END;
  ELSE
    NEW.status := CASE NEW.status_group
      WHEN 'pending' THEN 'waiting'
      WHEN 'in_progress' THEN 'building'
      WHEN 'ready' THEN 'uploaded'
      WHEN 'failed' THEN 'failed'
    END;
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_compute_status_group
    BEFORE INSERT OR UPDATE OF status, status_group ON public.env_builds
    FOR EACH ROW EXECUTE FUNCTION public.compute_status_group();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

CREATE OR REPLACE FUNCTION public.compute_status_group() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.status_group := CASE
    WHEN NEW.status IN ('pending', 'waiting') THEN 'pending'
    WHEN NEW.status IN ('in_progress', 'building', 'snapshotting') THEN 'in_progress'
    WHEN NEW.status IN ('ready', 'uploaded', 'success') THEN 'ready'
    ELSE 'failed'
  END;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_compute_status_group
    BEFORE INSERT OR UPDATE OF status ON public.env_builds
    FOR EACH ROW EXECUTE FUNCTION public.compute_status_group();
-- +goose StatementEnd
