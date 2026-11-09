-- One statement, so that the counter row lock is held for one round trip.
-- The CTEs share one snapshot: run cannot see a row that a concurrent
-- reservation inserts, so it conflicts on it instead. When bump counted the
-- actor and run inserted nothing, the caller must roll back.
--
-- others is read in the snapshot and does not see concurrent changes to the
-- other slots. Only the row of the own slot is locked and read again.
--
-- A gone row is reused only by the same project with a new reservation ID,
-- so a repeated Reserve never restarts a start that has ended.
-- name: Reserve :one
WITH free AS (
  SELECT NOT EXISTS (
    SELECT 1 FROM public.scheduling_actor_run
     WHERE actor_id = @actor_id::uuid
       AND (state <> 'gone' OR project_id <> @project_id::uuid OR reservation_id = @reservation_id::uuid)
  ) AS ok
), others AS (
  SELECT coalesce(sum(live), 0)::bigint AS live
    FROM public.scheduling_project_live_count
   WHERE project_id = @project_id::uuid AND slot <> @counter_slot::smallint
), bump AS (
  INSERT INTO public.scheduling_project_live_count AS c (project_id, slot, live)
  SELECT @project_id::uuid, @counter_slot::smallint, 1 FROM free, others
   WHERE free.ok AND others.live < @live_limit::bigint
  ON CONFLICT (project_id, slot) DO UPDATE SET live = c.live + 1
   WHERE c.live + (SELECT live FROM others) < @live_limit::bigint
  RETURNING 1
), run AS (
  INSERT INTO public.scheduling_actor_run AS r (actor_id, project_id, state, end_time, reservation_id, reserved_at, counter_slot)
  SELECT @actor_id::uuid, @project_id::uuid, 'starting'::public.scheduling_actor_state, @end_time::timestamptz, @reservation_id::uuid, now(), @counter_slot::smallint FROM bump
  ON CONFLICT (actor_id) DO UPDATE
     SET state = 'starting', end_time = EXCLUDED.end_time, revision = 1,
         reservation_id = EXCLUDED.reservation_id, reserved_at = EXCLUDED.reserved_at,
         transition_id = NULL, transition_action = NULL, transition_at = NULL,
         failure_message = NULL, failure_code = NULL, gone_at = NULL,
         refused_until = NULL, refused_since = NULL, counter_slot = EXCLUDED.counter_slot
   WHERE r.state = 'gone' AND r.project_id = EXCLUDED.project_id AND r.reservation_id <> EXCLUDED.reservation_id
  RETURNING 1
)
SELECT (SELECT ok FROM free)::boolean AS free,
       EXISTS (SELECT 1 FROM bump) AS bumped,
       EXISTS (SELECT 1 FROM run) AS inserted;

-- name: InspectReservation :one
SELECT project_id, state, reservation_id, (transition_action IS NOT NULL)::boolean AS transitions,
       failure_message, failure_code
  FROM public.scheduling_actor_run
 WHERE actor_id = @actor_id;
