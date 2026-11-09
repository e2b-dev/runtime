-- name: GetLiveActor :one
SELECT a.actor_id, a.project_id, a.execution_id, a.start_time, a.max_instance_length, a.node_id,
       a.cluster_id, a.base_template_id, a.template_id, a.metadata, a.auto_pause,
       a.auto_pause_filesystem_only, a.record, r.state, r.end_time
  FROM public.scheduling_actor a
  JOIN public.scheduling_actor_run r USING (actor_id)
 WHERE a.actor_id = @actor_id AND a.project_id = @project_id AND r.state IN ('running', 'pausing', 'killing', 'snapshotting');
