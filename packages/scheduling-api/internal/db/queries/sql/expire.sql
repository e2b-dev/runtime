-- Scans the table: an index on state or reserved_at would make state changes
-- non-HOT updates. Pages by actor_id, so that starts that fail to end do not
-- fill every batch.
-- name: ListExpiredStarts :many
SELECT actor_id, project_id, reservation_id
  FROM public.scheduling_actor_run
 WHERE state = 'starting' AND reserved_at <= now() - @start_timeout::interval AND actor_id > @after
  ORDER BY actor_id
 LIMIT @max_starts;

-- name: ExpireStart :one
UPDATE public.scheduling_actor_run
   SET state = 'gone', gone_at = now(), failure_message = @failure_message::text, failure_code = @failure_code::integer
 WHERE actor_id = @actor_id AND project_id = @project_id AND reservation_id = @reservation_id AND state = 'starting'
   AND reserved_at <= now() - @start_timeout::interval
RETURNING counter_slot;
