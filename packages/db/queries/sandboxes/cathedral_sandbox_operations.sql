-- name: ReserveCathedralSandboxOperation :one
INSERT INTO public.cathedral_sandbox_operations (
    team_id,
    idempotency_key,
    request_sha256,
    sandbox_id,
    state
) VALUES (
    sqlc.arg(team_id)::uuid,
    sqlc.arg(idempotency_key)::text,
    sqlc.arg(request_sha256)::text,
    sqlc.arg(sandbox_id)::text,
    'reserved'
)
ON CONFLICT (team_id, idempotency_key) DO NOTHING
RETURNING *;

-- name: GetCathedralSandboxOperation :one
SELECT *
FROM public.cathedral_sandbox_operations
WHERE team_id = sqlc.arg(team_id)::uuid
  AND idempotency_key = sqlc.arg(idempotency_key)::text;

-- name: MarkCathedralSandboxOperationCreating :execrows
UPDATE public.cathedral_sandbox_operations
SET state = 'creating',
    error_code = NULL,
    error_message = NULL,
    updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid
  AND idempotency_key = sqlc.arg(idempotency_key)::text
  AND request_sha256 = sqlc.arg(request_sha256)::text
  AND sandbox_id = sqlc.arg(sandbox_id)::text
  AND state IN ('reserved', 'creating');

-- name: CompleteCathedralSandboxOperation :execrows
UPDATE public.cathedral_sandbox_operations
SET state = 'ready',
    response_json = CASE
        WHEN state = 'ready' THEN response_json
        ELSE sqlc.arg(response_json)::text
    END,
    error_code = NULL,
    error_message = NULL,
    updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid
  AND idempotency_key = sqlc.arg(idempotency_key)::text
  AND request_sha256 = sqlc.arg(request_sha256)::text
  AND sandbox_id = sqlc.arg(sandbox_id)::text
  AND (
      state IN ('reserved', 'creating')
      OR (state = 'ready' AND response_json = sqlc.arg(response_json)::text)
  );

-- name: FailCathedralSandboxOperation :execrows
UPDATE public.cathedral_sandbox_operations
SET state = 'failed',
    error_code = sqlc.arg(error_code)::integer,
    error_message = sqlc.arg(error_message)::text,
    updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid
  AND idempotency_key = sqlc.arg(idempotency_key)::text
  AND request_sha256 = sqlc.arg(request_sha256)::text
  AND sandbox_id = sqlc.arg(sandbox_id)::text
  AND state IN ('reserved', 'creating', 'failed');
