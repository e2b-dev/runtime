-- name: MarkRunning :execrows
UPDATE public.scheduling_actor_run SET state = 'running', end_time = least(coalesce(sqlc.narg(end_time)::timestamptz, end_time), @max_end_time::timestamptz)
 WHERE actor_id = @actor_id AND project_id = @project_id AND reservation_id = @reservation_id AND state = 'starting';

-- name: InsertActor :exec
INSERT INTO public.scheduling_actor (actor_id, project_id, execution_id, start_time, node_id, cluster_id,
                                     base_template_id, template_id, max_instance_length, auto_pause,
                                     auto_pause_filesystem_only, metadata, record)
VALUES (@actor_id, @project_id, @execution_id, @start_time, @node_id, @cluster_id,
        @base_template_id, @template_id, @max_instance_length, @auto_pause,
        @auto_pause_filesystem_only, @metadata, @record);

-- name: InspectStart :one
SELECT state, reservation_id, (transition_action IS NOT NULL)::boolean AS transitions
  FROM public.scheduling_actor_run
 WHERE actor_id = @actor_id AND project_id = @project_id;

-- name: FinishStart :one
UPDATE public.scheduling_actor_run
   SET state = 'gone', gone_at = now(), failure_message = @failure_message::text, failure_code = @failure_code::integer
 WHERE actor_id = @actor_id AND project_id = @project_id AND reservation_id = @reservation_id AND state = 'starting'
RETURNING counter_slot;

-- Fails on the live >= 0 check rather than hiding a counter that is already
-- wrong.
-- name: ReleaseCounterSlot :execrows
UPDATE public.scheduling_project_live_count SET live = live - 1
 WHERE project_id = @project_id AND slot = @slot;
