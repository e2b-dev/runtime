-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE public.env_builds ADD COLUMN IF NOT EXISTS status_group text;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION compute_status_group() RETURNS TRIGGER AS $$
BEGIN
  NEW.status_group := CASE
    WHEN NEW.status IN ('pending', 'waiting') THEN 'pending'
    WHEN NEW.status IN ('in_progress', 'building', 'snapshotting') THEN 'in_progress'
    WHEN NEW.status IN ('ready', 'uploaded', 'success') THEN 'ready'
    ELSE 'failed'
  END;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE OR REPLACE TRIGGER trg_compute_status_group
  BEFORE INSERT OR UPDATE OF status ON public.env_builds
  FOR EACH ROW EXECUTE FUNCTION compute_status_group();

-- +goose StatementBegin
CREATE OR REPLACE PROCEDURE backfill_status_group() AS $$
DECLARE
  batch_size INT := 50000;
  rows_updated INT;
BEGIN
  LOOP
    UPDATE public.env_builds
    SET status_group = CASE
        WHEN status IN ('pending', 'waiting') THEN 'pending'
        WHEN status IN ('in_progress', 'building', 'snapshotting') THEN 'in_progress'
        WHEN status IN ('ready', 'uploaded', 'success') THEN 'ready'
        ELSE 'failed'
      END
    WHERE id IN (
      SELECT id FROM public.env_builds
      WHERE status_group IS NULL
      LIMIT batch_size
    );
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    COMMIT;
    EXIT WHEN rows_updated = 0;
    RAISE NOTICE 'backfill_status_group: updated % rows, sleeping 10s before next batch...', rows_updated;
    PERFORM pg_sleep(10);
  END LOOP;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CALL backfill_status_group();
DROP PROCEDURE backfill_status_group();

ALTER TABLE public.env_builds
  ADD CONSTRAINT chk_status_group_not_null
  CHECK (status_group IS NOT NULL) NOT VALID;

ALTER TABLE public.env_builds
  VALIDATE CONSTRAINT chk_status_group_not_null;

ALTER TABLE public.env_builds
  ALTER COLUMN status_group SET NOT NULL;

ALTER TABLE public.env_builds
  DROP CONSTRAINT chk_status_group_not_null;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_status_group
  ON public.env_builds(status_group);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_status_group;
DROP TRIGGER IF EXISTS trg_compute_status_group ON public.env_builds;
DROP FUNCTION IF EXISTS compute_status_group();
ALTER TABLE public.env_builds DROP COLUMN IF EXISTS status_group;
