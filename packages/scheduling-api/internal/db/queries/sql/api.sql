-- name: LockAdmission :exec
SELECT pg_advisory_xact_lock(hashtextextended(@actor_key::text, 0));

-- name: HasKillClaim :one
SELECT EXISTS (SELECT 1 FROM public.scheduling_kill_claim WHERE project_id = @project_id AND actor_id = @actor_id AND until > now());

-- name: ClaimSnapshotDeletion :execrows
INSERT INTO public.scheduling_kill_claim (actor_id, project_id, claim_id, until)
SELECT @actor_id::uuid, @project_id::uuid, @claim_id::uuid, now() + interval '60 seconds'
 WHERE NOT EXISTS (SELECT 1 FROM public.scheduling_actor_run WHERE actor_id = @actor_id AND (state <> 'gone' OR project_id <> @project_id))
ON CONFLICT (project_id, actor_id, claim_id) DO UPDATE
   SET until = excluded.until;

-- name: ReleaseSnapshotDeletionClaim :exec
DELETE FROM public.scheduling_kill_claim WHERE actor_id = @actor_id AND project_id = @project_id AND claim_id = @claim_id;

-- name: InspectAPIReservation :one
SELECT reservation_id, (state = 'starting')::boolean AS pending, failure_message, failure_code, transition_id
  FROM public.scheduling_actor_run WHERE actor_id = @actor_id AND project_id = @project_id;

-- name: ListLive :many
SELECT a.actor_id, a.project_id, a.execution_id, a.start_time, a.max_instance_length, a.node_id,
       a.cluster_id, a.base_template_id, a.template_id, a.metadata, a.auto_pause,
       a.auto_pause_filesystem_only, a.record, r.state, r.end_time, r.refused_until, r.refused_since,
       r.envd_refused_until, r.envd_refused_since
  FROM public.scheduling_actor a JOIN public.scheduling_actor_run r USING (actor_id)
 WHERE r.state IN ('running', 'pausing', 'killing', 'snapshotting')
   AND (sqlc.narg(project_id)::uuid IS NULL OR r.project_id = sqlc.narg(project_id))
   AND (cardinality(@states::text[]) = 0 OR r.state::text = ANY(@states::text[]))
   AND (NOT @expired_only::boolean OR (r.end_time <= now() AND (r.refused_until IS NULL OR r.refused_until <= now())))
   AND r.actor_id > @after_id::uuid
 ORDER BY r.actor_id LIMIT @page_size::integer;

-- name: UpdateDeadline :execrows
UPDATE public.scheduling_actor_run r SET end_time = least(@end_time::timestamptz, a.start_time + a.max_instance_length)
  FROM public.scheduling_actor a
 WHERE r.actor_id = @actor_id AND r.project_id = @project_id AND r.state = 'running'
   AND a.actor_id = r.actor_id AND a.execution_id = @execution_id AND r.end_time = @expected_end_time::timestamptz;
