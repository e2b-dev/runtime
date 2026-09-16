-- +goose Up
CREATE TABLE public.cathedral_sandbox_operations (
    team_id UUID NOT NULL REFERENCES public.teams(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(128) NOT NULL,
    request_sha256 CHAR(64) NOT NULL,
    operation_kind VARCHAR(16) NOT NULL DEFAULT 'create',
    sandbox_id TEXT NOT NULL,
    state VARCHAR(16) NOT NULL DEFAULT 'reserved',
    response_json TEXT,
    error_code INTEGER,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (team_id, idempotency_key),
    UNIQUE (team_id, sandbox_id),
    CONSTRAINT cathedral_sandbox_operations_key_nonempty
        CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    CONSTRAINT cathedral_sandbox_operations_request_sha256
        CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT cathedral_sandbox_operations_kind
        CHECK (operation_kind IN ('create')),
    CONSTRAINT cathedral_sandbox_operations_state
        CHECK (state IN ('reserved', 'creating', 'ready', 'failed')),
    CONSTRAINT cathedral_sandbox_operations_ready_response
        CHECK (state <> 'ready' OR response_json IS NOT NULL)
);

CREATE INDEX cathedral_sandbox_operations_state_updated_idx
    ON public.cathedral_sandbox_operations (state, updated_at);

-- +goose Down
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.cathedral_sandbox_operations LIMIT 1) THEN
        RAISE EXCEPTION 'cannot drop cathedral_sandbox_operations while rows exist';
    END IF;
END $$;

DROP TABLE public.cathedral_sandbox_operations;
