-- The batch is a CTE so that it runs once: a LIMIT subquery in the WHERE
-- clause can run again and delete more than max_rows. SKIP LOCKED lets
-- purgers on several replicas take different rows, and leaves a row that a
-- Reserve is reusing. The scan has no index, for the same reason as
-- ListExpiredStarts.
-- name: PurgeTombstones :execrows
WITH batch AS MATERIALIZED (
  SELECT actor_id FROM public.scheduling_actor_run
   WHERE state = 'gone' AND gone_at <= now() - @retention::interval
   LIMIT @max_rows
     FOR UPDATE SKIP LOCKED
)
DELETE FROM public.scheduling_actor_run r
 USING batch
 WHERE r.actor_id = batch.actor_id AND r.state = 'gone' AND r.gone_at <= now() - @retention::interval;

-- name: PurgeExpiredKillClaims :exec
WITH batch AS MATERIALIZED (
  SELECT project_id, actor_id, claim_id FROM public.scheduling_kill_claim
   WHERE until <= now()
   ORDER BY until
   LIMIT @max_rows
     FOR UPDATE SKIP LOCKED
)
DELETE FROM public.scheduling_kill_claim c
 USING batch
 WHERE c.project_id = batch.project_id AND c.actor_id = batch.actor_id AND c.claim_id = batch.claim_id;
