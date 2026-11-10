-- Applies to the rest of the transaction only, so request queries keep their
-- own limits.
-- name: SetCleanupTimeouts :exec
SELECT set_config('lock_timeout', '1s', true), set_config('statement_timeout', '5s', true);

-- name: TryCleanupLock :one
SELECT pg_try_advisory_lock(@key::bigint)::boolean AS locked, pg_backend_pid()::integer AS pid;

-- A session lock lasts as long as its backend, so the same backend PID means
-- the lock is still held.
-- name: CleanupBackendPID :one
SELECT pg_backend_pid()::integer AS pid;

-- name: ReleaseCleanupLock :one
SELECT pg_advisory_unlock(@key::bigint)::boolean AS released;
