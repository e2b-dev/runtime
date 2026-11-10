-- Stamps snapshots.env_deleted_at on snapshots whose env was soft-deleted
-- before soft deletes began stamping it. Migration 20261028205102 does this
-- itself on a small table, but before the rollout, so every install runs this
-- script once after it; on a larger table the script does all of the work.
--
-- It walks the snapshots table in physical order, a range of heap pages per
-- transaction, and writes only rows whose env is deleted and whose
-- env_deleted_at is still NULL, so it is idempotent and a rerun or a resumed
-- run is always safe. It installs nothing in the schema.
--
-- When to run it:
--
-- - After every service that soft-deletes envs runs the release that stamps
--   snapshots: a delete by an older release after the walk has passed a row
--   leaves that row unstamped until the next run.
-- - To completion before creating any index whose key or predicate reads
--   env_deleted_at. While no index reads the column, a stamp is a heap-only
--   update that adds no index entries when its page has room; once one does,
--   every stamp adds an entry to every index on snapshots.
-- - In particular, to completion before migration 20261103090000
--   (add_snapshot_list_live_indexes) builds the live-only snapshot indexes: a
--   row stamped after they are built leaves its space in them, so they stay
--   near full size. If they were built first, rebuild each with REINDEX INDEX
--   CONCURRENTLY once this finishes.
--
-- Run it with psql outside a transaction (no BEGIN, no --single-transaction),
-- since it commits after each batch. statement_timeout spans the whole run,
-- not one batch, so turn it off; the script refuses to start otherwise.
-- Optional settings go first, in the same session:
--
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
--     -c "SET statement_timeout = 0" \
--     -c "SET backfill.batch_pages = '200'" \
--     -c "SET backfill.pause = '200ms'" \
--     -f backfill_snapshots_env_deleted_at.sql
--
--   backfill.batch_pages  heap pages walked per batch (default 200)
--   backfill.pause        sleep between batches (default 200ms)
--   backfill.start_page   resume from this page; every batch reports it
--
-- Walking pages in order reads the table sequentially and dirties each page
-- once; a walk in key order would revisit pages at random, since the keys are
-- random UUIDs. In exchange, a row that another update moves onto a page the
-- walk has already passed is not seen. A pause that rewrites a row fills in
-- its stamp and a soft delete writes it, so only a row whose env was deleted
-- earlier and that some other update moves during the run can be missed. A
-- deleted env's snapshot is rarely updated, and a rerun stamps it. Reading a
-- page range directly needs PostgreSQL 14 or newer; an older server would scan
-- the whole table for every batch.
--
-- What to watch while it runs; raise the pause or lower the batch if any of
-- these climbs:
--
-- - WAL volume and replica lag: every stamp is a new row version.
-- - Queries on read replicas cancelled by recovery conflicts
--   (pg_stat_database_conflicts), once vacuum removes the old versions.
-- - Logical replication slots, such as a change data capture feed: every
--   stamp is decoded, and the primary keeps WAL until the slowest consumer
--   confirms it. pg_replication_slots shows the backlog as
--   pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn).
-- - Autovacuum on snapshots: each stamp leaves a dead row version, so
--   autovacuum passes over the table and its indexes repeatedly.
--
-- Stopping the run (Ctrl-C) loses at most the batch in flight.
DO $backfill$
DECLARE
    batch_pages bigint := COALESCE(NULLIF(current_setting('backfill.batch_pages', true), '')::bigint, 200);
    pause interval := COALESCE(NULLIF(current_setting('backfill.pause', true), '')::interval, '200ms');
    next_page bigint := COALESCE(NULLIF(current_setting('backfill.start_page', true), '')::bigint, 0);
    table_pages bigint;
    batch_stamped bigint;
    total_stamped bigint := 0;
BEGIN
    IF batch_pages < 1 THEN
        RAISE EXCEPTION 'backfill.batch_pages must be positive, got %', batch_pages;
    END IF;
    -- The timer starts with this statement, and the COMMIT after each batch
    -- does not restart it, so any limit would cancel the run partway.
    IF current_setting('statement_timeout') <> '0' THEN
        RAISE EXCEPTION 'statement_timeout is %, and it would cancel the run partway', current_setting('statement_timeout')
            USING HINT = 'Run SET statement_timeout = 0 in the same session first.';
    END IF;

    LOOP
        -- Read again every batch: a row the walk has not reached can move to a
        -- page added since the run began.
        table_pages := pg_relation_size('public.snapshots') / current_setting('block_size')::bigint;
        EXIT WHEN next_page >= table_pages;

        -- EXECUTE plans every batch with its own page range, which a TID range
        -- scan reads directly.
        EXECUTE $batch$
            UPDATE public.snapshots s
            SET env_deleted_at = e.deleted_at
            FROM public.envs e
            WHERE s.ctid >= $1
              AND s.ctid < $2
              AND s.env_deleted_at IS NULL
              AND e.id = s.env_id
              AND e.deleted_at IS NOT NULL
        $batch$
        USING format('(%s,0)', next_page)::tid, format('(%s,0)', next_page + batch_pages)::tid;
        GET DIAGNOSTICS batch_stamped = ROW_COUNT;

        COMMIT;

        total_stamped := total_stamped + batch_stamped;
        next_page := next_page + batch_pages;
        RAISE NOTICE 'stamped % snapshots, % of % pages walked; resume with SET backfill.start_page = ''%''',
            total_stamped, LEAST(next_page, table_pages), table_pages, next_page;

        IF next_page < table_pages THEN
            PERFORM pg_sleep_for(pause);
        END IF;
    END LOOP;

    RAISE NOTICE 'done: stamped % snapshots', total_stamped;
END
$backfill$;
