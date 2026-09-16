-- +goose Up
CREATE TABLE public.cathedral_sandbox_lifecycle_operations (
    team_id UUID NOT NULL REFERENCES public.teams(id) ON DELETE CASCADE,
    operation_key VARCHAR(128) NOT NULL,
    request_sha256 CHAR(64) NOT NULL,
    operation_kind VARCHAR(16) NOT NULL,
    sandbox_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    state VARCHAR(16) NOT NULL DEFAULT 'reserved',
    execution_removed_at TIMESTAMPTZ,
    snapshot_build_id TEXT,
    snapshot_completed_at TIMESTAMPTZ,
    remaining_lifetime_ms BIGINT,
    cleanup_state VARCHAR(16) NOT NULL DEFAULT 'not_required',
    result_json TEXT,
    error_code INTEGER,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    dispatch_started_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (team_id, operation_key),
    CONSTRAINT cathedral_lifecycle_key_nonempty
        CHECK (length(operation_key) BETWEEN 8 AND 128),
    CONSTRAINT cathedral_lifecycle_request_sha256
        CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT cathedral_lifecycle_kind
        CHECK (operation_kind IN ('delete', 'pause')),
    CONSTRAINT cathedral_lifecycle_execution_nonempty
        CHECK (length(execution_id) > 0),
    CONSTRAINT cathedral_lifecycle_state
        CHECK (state IN ('reserved', 'dispatching', 'completed', 'failed', 'unknown')),
    CONSTRAINT cathedral_lifecycle_cleanup_state
        CHECK (cleanup_state IN ('not_required', 'pending', 'completed', 'failed')),
    CONSTRAINT cathedral_lifecycle_pause_evidence
        CHECK (state <> 'completed' OR operation_kind <> 'pause' OR
            (execution_removed_at IS NOT NULL AND snapshot_build_id IS NOT NULL AND snapshot_completed_at IS NOT NULL)),
    CONSTRAINT cathedral_lifecycle_delete_evidence
        CHECK (state <> 'completed' OR operation_kind <> 'delete' OR execution_removed_at IS NOT NULL),
    CONSTRAINT cathedral_lifecycle_result
        CHECK (state <> 'completed' OR result_json IS NOT NULL)
);

CREATE INDEX cathedral_lifecycle_sandbox_idx
    ON public.cathedral_sandbox_lifecycle_operations (team_id, sandbox_id, created_at DESC);
CREATE INDEX cathedral_lifecycle_recovery_idx
    ON public.cathedral_sandbox_lifecycle_operations (state, updated_at)
    WHERE state IN ('reserved', 'dispatching', 'unknown');

-- +goose Down
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.cathedral_sandbox_lifecycle_operations LIMIT 1) THEN
        RAISE EXCEPTION 'cannot drop cathedral_sandbox_lifecycle_operations while rows exist';
    END IF;
END $$;

DROP TABLE public.cathedral_sandbox_lifecycle_operations;
