-- Reads all projects. Scans and sorts the table: an index on state or
-- transition_at would make state changes non-HOT updates. A transition whose
-- actor has no record is not listed.
-- name: ListStaleTransitions :many
SELECT r.actor_id, r.project_id, r.transition_id::uuid AS transition_id,
       r.transition_action::smallint AS transition_action, r.transition_at::timestamptz AS transition_at,
       a.execution_id, a.node_id, a.cluster_id
  FROM public.scheduling_actor_run r
  JOIN public.scheduling_actor a ON a.actor_id = r.actor_id AND a.project_id = r.project_id
 WHERE r.state IN ('pausing', 'killing') AND r.transition_at <= now() - @stale_after::interval
   AND (r.transition_at, r.actor_id) > (@after_transition_at::timestamptz, @after_actor_id::uuid)
 ORDER BY r.transition_at, r.actor_id
 LIMIT @max_rows;
