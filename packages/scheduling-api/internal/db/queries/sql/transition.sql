-- A finished transition and a restored pause keep their transition_id, so a
-- repeat with the same ID never begins a second transition.
-- name: BeginTransition :execrows
UPDATE public.scheduling_actor_run r
   SET state = @state::public.scheduling_actor_state, transition_id = @transition_id::uuid,
       transition_action = @transition_action::smallint, transition_at = now(),
       failure_message = NULL, failure_code = NULL
 WHERE r.actor_id = @actor_id AND r.project_id = @project_id AND r.state = 'running'
   AND r.transition_id IS DISTINCT FROM @transition_id::uuid
   AND (@expected_execution_id::text = '' OR EXISTS (
         SELECT 1 FROM public.scheduling_actor a
          WHERE a.actor_id = r.actor_id AND a.execution_id = @expected_execution_id::text))
   AND (NOT @require_expired::boolean OR (r.end_time <= now() AND (r.refused_until IS NULL OR r.refused_until <= now())));

-- name: InspectTransition :one
SELECT r.state, r.transition_id, r.transition_action, r.failure_message, r.failure_code,
       r.refused_until, r.refused_since,
       (r.end_time <= now() AND (r.refused_until IS NULL OR r.refused_until <= now()))::boolean AS expired,
       a.execution_id
  FROM public.scheduling_actor_run r
  LEFT JOIN public.scheduling_actor a USING (actor_id)
 WHERE r.actor_id = @actor_id AND r.project_id = @project_id;

-- name: FinishTransition :one
UPDATE public.scheduling_actor_run
   SET state = 'gone', gone_at = now(), failure_message = sqlc.narg(failure_message)::text,
       failure_code = sqlc.narg(failure_code)::integer
 WHERE actor_id = @actor_id AND project_id = @project_id AND transition_id = @transition_id::uuid AND state IN ('pausing', 'killing')
RETURNING counter_slot;

-- name: DeleteActor :execrows
DELETE FROM public.scheduling_actor WHERE actor_id = @actor_id AND project_id = @project_id;

-- A refusal within 30 s after the previous retry window ended continues the
-- series that refused_since started. SET reads the values before the update.
-- name: RestoreRunning :one
UPDATE public.scheduling_actor_run
   SET state = 'running',
       refused_since = CASE
         WHEN refused_since IS NOT NULL AND refused_until IS NOT NULL AND now() <= refused_until + interval '30 seconds'
         THEN refused_since ELSE now() END,
       refused_until = now() + @retry_after::interval
 WHERE actor_id = @actor_id AND project_id = @project_id AND transition_id = @transition_id::uuid
   AND transition_action = 1 AND state = 'pausing'
RETURNING refused_until, refused_since;

-- transition_age_us comes from the database clock, so that the poll schedule
-- does not depend on the clocks of the replicas. It is 0 without a transition.
-- name: WatchTransition :one
SELECT r.state, r.transition_id, r.transition_action, r.failure_message, r.failure_code,
       COALESCE((extract(epoch FROM statement_timestamp() - r.transition_at) * 1000000)::bigint, 0)::bigint AS transition_age_us
  FROM public.scheduling_actor_run r
 WHERE r.actor_id = @actor_id AND r.project_id = @project_id;
