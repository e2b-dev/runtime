-- +goose Up
-- +goose NO TRANSACTION

ALTER TABLE public.env_builds ADD COLUMN IF NOT EXISTS team_id uuid;

-- +goose StatementBegin
-- Trigger to auto-populate team_id when a build is assigned to an env.
-- Mirrors the existing backfill_env_id_from_assignment() trigger pattern.
CREATE OR REPLACE FUNCTION backfill_team_id_from_assignment()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE env_builds
    SET team_id = (SELECT team_id FROM envs WHERE id = NEW.env_id)
    WHERE id = NEW.build_id AND team_id IS NULL;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE OR REPLACE TRIGGER trigger_backfill_team_id
    AFTER INSERT ON env_build_assignments
    FOR EACH ROW EXECUTE FUNCTION backfill_team_id_from_assignment();

-- +goose StatementBegin
-- Cursor-based backfill: walks env_builds by (created_at, id) so each batch only scans
-- forward, giving O(N) total work without needing a temporary index.
-- Resumable: on restart, skips to the first unprocessed row instead of re-scanning from epoch.
-- Uses env_build_assignments -> envs to resolve team_id (NOT the legacy env_builds.env_id column).
CREATE OR REPLACE PROCEDURE backfill_env_builds_team_id() AS $$
DECLARE
  batch_size INT := 50000;
  rows_updated INT;
  total_updated INT := 0;
  last_created_at TIMESTAMP WITH TIME ZONE := '1970-01-01 00:00:00+00';
  last_id UUID := '00000000-0000-0000-0000-000000000000';
  current_max_created_at TIMESTAMP WITH TIME ZONE;
  current_max_id UUID;
  resume_at TIMESTAMP WITH TIME ZONE;
  resume_id UUID;
BEGIN
  RAISE NOTICE 'backfill_env_builds_team_id: starting backfill with batch_size %', batch_size;

  -- Resume: skip rows already backfilled by a previous (interrupted) run.
  -- Uses idx_env_builds_created_at (backward scan) + idx_env_build_assignments_build.
  SELECT eb.created_at, eb.id INTO resume_at, resume_id
  FROM public.env_builds eb
  WHERE eb.team_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.env_build_assignments eba
      JOIN public.envs e ON e.id = eba.env_id
      WHERE eba.build_id = eb.id
    )
  ORDER BY eb.created_at, eb.id
  LIMIT 1;

  IF resume_at IS NULL THEN
    RAISE NOTICE 'backfill_env_builds_team_id: no rows to backfill';
    RETURN;
  END IF;

  -- Position cursor to the row immediately before the first unprocessed row
  -- so the loop's > comparison includes it.
  SELECT eb.created_at, eb.id INTO last_created_at, last_id
  FROM public.env_builds eb
  WHERE (eb.created_at, eb.id) < (resume_at, resume_id)
  ORDER BY eb.created_at DESC, eb.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    last_created_at := '1970-01-01 00:00:00+00';
    last_id := '00000000-0000-0000-0000-000000000000';
  END IF;

  RAISE NOTICE 'backfill_env_builds_team_id: resuming from cursor (%, %)', last_created_at, last_id;

  LOOP
    SELECT created_at, id INTO current_max_created_at, current_max_id
    FROM (
      SELECT created_at, id FROM public.env_builds
      WHERE (created_at, id) > (last_created_at, last_id)
      ORDER BY created_at, id
      LIMIT batch_size
    ) sub
    ORDER BY created_at DESC, id DESC
    LIMIT 1;

    EXIT WHEN current_max_created_at IS NULL;

    RAISE NOTICE 'backfill_env_builds_team_id: selected batch window up to created_at: %, id: %',
      current_max_created_at, current_max_id;

    UPDATE public.env_builds eb
    SET team_id = sub.team_id
    FROM (
      SELECT eb2.id, e2.team_id
      FROM public.env_builds eb2
      JOIN public.env_build_assignments eba ON eba.build_id = eb2.id
      JOIN public.envs e2 ON e2.id = eba.env_id
      WHERE eb2.team_id IS NULL
        AND (eb2.created_at, eb2.id) > (last_created_at, last_id)
        AND (eb2.created_at, eb2.id) <= (current_max_created_at, current_max_id)
    ) sub
    WHERE eb.id = sub.id;

    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    total_updated := total_updated + rows_updated;
    last_created_at := current_max_created_at;
    last_id := current_max_id;
    COMMIT;
    RAISE NOTICE 'backfill_env_builds_team_id: updated % rows in batch, total: %, up to created_at: %, id: %',
      rows_updated, total_updated, last_created_at, last_id;
    IF rows_updated > 0 THEN
      RAISE NOTICE 'backfill_env_builds_team_id: sleeping 10s before next batch...';
      PERFORM pg_sleep(10);
    END IF;
  END LOOP;
  RAISE NOTICE 'backfill_env_builds_team_id: complete, % rows updated', total_updated;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CALL backfill_env_builds_team_id();
DROP PROCEDURE backfill_env_builds_team_id();

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_team_status_pagination
  ON public.env_builds (team_id, created_at DESC, id DESC) INCLUDE (status, status_group);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_team_env_created_id
  ON public.env_builds (team_id, env_id, created_at DESC, id DESC);

DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_created_at;

-- +goose Down
-- +goose NO TRANSACTION

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_created_at ON env_builds (created_at DESC);
DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_team_env_created_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_team_status_pagination;
DROP TRIGGER IF EXISTS trigger_backfill_team_id ON env_build_assignments;
DROP FUNCTION IF EXISTS backfill_team_id_from_assignment();
ALTER TABLE public.env_builds DROP COLUMN IF EXISTS team_id;
