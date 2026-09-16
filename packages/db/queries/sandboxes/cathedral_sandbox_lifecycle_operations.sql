-- name: ReserveCathedralSandboxLifecycleOperation :one
INSERT INTO public.cathedral_sandbox_lifecycle_operations (
    team_id, operation_key, request_sha256, operation_kind, sandbox_id,
    execution_id, remaining_lifetime_ms, cleanup_state
) VALUES (
    sqlc.arg(team_id)::uuid, sqlc.arg(operation_key)::text,
    sqlc.arg(request_sha256)::text, sqlc.arg(operation_kind)::text,
    sqlc.arg(sandbox_id)::text, sqlc.arg(execution_id)::text,
    sqlc.narg(remaining_lifetime_ms)::bigint,
    CASE WHEN sqlc.arg(operation_kind)::text = 'delete' THEN 'pending' ELSE 'not_required' END
)
ON CONFLICT (team_id, operation_key) DO NOTHING
RETURNING *;

-- name: GetCathedralSandboxLifecycleOperation :one
SELECT *
FROM public.cathedral_sandbox_lifecycle_operations
WHERE team_id = sqlc.arg(team_id)::uuid
  AND operation_key = sqlc.arg(operation_key)::text;

-- name: MarkCathedralSandboxLifecycleDispatching :execrows
UPDATE public.cathedral_sandbox_lifecycle_operations
SET state = 'dispatching', dispatch_started_at = COALESCE(dispatch_started_at, now()), updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid
  AND operation_key = sqlc.arg(operation_key)::text
  AND request_sha256 = sqlc.arg(request_sha256)::text
  AND operation_kind = sqlc.arg(operation_kind)::text
  AND sandbox_id = sqlc.arg(sandbox_id)::text
  AND execution_id = sqlc.arg(execution_id)::text
  AND state = 'reserved';

-- name: CompleteCathedralSandboxLifecycleOperation :execrows
UPDATE public.cathedral_sandbox_lifecycle_operations
SET state = 'completed',
    execution_removed_at = sqlc.arg(execution_removed_at)::timestamptz,
    snapshot_build_id = sqlc.narg(snapshot_build_id)::text,
    snapshot_completed_at = sqlc.narg(snapshot_completed_at)::timestamptz,
    cleanup_state = sqlc.arg(cleanup_state)::text,
    result_json = sqlc.arg(result_json)::text,
    error_code = NULL,
    error_message = NULL,
    updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid
  AND operation_key = sqlc.arg(operation_key)::text
  AND request_sha256 = sqlc.arg(request_sha256)::text
  AND operation_kind = sqlc.arg(operation_kind)::text
  AND sandbox_id = sqlc.arg(sandbox_id)::text
  AND execution_id = sqlc.arg(execution_id)::text
  AND state = 'dispatching';

-- name: MarkCathedralSandboxLifecycleUnknown :execrows
UPDATE public.cathedral_sandbox_lifecycle_operations
SET state = 'unknown', error_code = sqlc.narg(error_code)::integer,
    error_message = sqlc.arg(error_message)::text, updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid
  AND operation_key = sqlc.arg(operation_key)::text
  AND request_sha256 = sqlc.arg(request_sha256)::text
  AND operation_kind = sqlc.arg(operation_kind)::text
  AND sandbox_id = sqlc.arg(sandbox_id)::text
  AND execution_id = sqlc.arg(execution_id)::text
  AND state IN ('reserved', 'dispatching', 'unknown');

-- name: FailCathedralSandboxLifecycleOperation :execrows
UPDATE public.cathedral_sandbox_lifecycle_operations
SET state = 'failed', error_code = sqlc.arg(error_code)::integer,
    error_message = sqlc.arg(error_message)::text, updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid
  AND operation_key = sqlc.arg(operation_key)::text
  AND request_sha256 = sqlc.arg(request_sha256)::text
  AND operation_kind = sqlc.arg(operation_kind)::text
  AND sandbox_id = sqlc.arg(sandbox_id)::text
  AND execution_id = sqlc.arg(execution_id)::text
  AND state IN ('reserved', 'dispatching', 'failed');
