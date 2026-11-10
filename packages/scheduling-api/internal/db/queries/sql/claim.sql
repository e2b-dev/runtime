-- name: ClaimTransition :one
UPDATE public.scheduling_actor_run r
   SET transition_id = @new_transition_id::uuid, transition_at = now()
 WHERE r.actor_id = @actor_id AND r.project_id = @project_id AND r.state IN ('pausing', 'killing')
   AND r.transition_id = @old_transition_id::uuid
   AND r.transition_at <= now() - @stale_after::interval
   AND EXISTS (SELECT 1 FROM public.scheduling_actor a
                WHERE a.actor_id = r.actor_id AND a.execution_id = @expected_execution_id::text)
RETURNING r.transition_action::smallint AS transition_action, r.transition_at::timestamptz AS transition_at;

-- name: InspectClaim :one
SELECT state, transition_id, transition_action, transition_at,
       (transition_at IS NOT NULL AND transition_at <= now() - @stale_after::interval)::boolean AS stale
  FROM public.scheduling_actor_run
 WHERE actor_id = @actor_id AND project_id = @project_id;
