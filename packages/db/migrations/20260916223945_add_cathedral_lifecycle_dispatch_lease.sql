-- +goose Up
ALTER TABLE public.cathedral_sandbox_lifecycle_operations
    ADD COLUMN filesystem_only BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN dispatch_attempt INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN dispatch_lease_expires_at TIMESTAMPTZ;

UPDATE public.cathedral_sandbox_lifecycle_operations
SET dispatch_lease_expires_at = COALESCE(dispatch_started_at, updated_at) + interval '2 minutes'
WHERE state = 'dispatching';

ALTER TABLE public.cathedral_sandbox_lifecycle_operations
    ADD CONSTRAINT cathedral_lifecycle_dispatch_attempt_nonnegative
        CHECK (dispatch_attempt >= 0),
    ADD CONSTRAINT cathedral_lifecycle_dispatch_lease
        CHECK ((state = 'dispatching') = (dispatch_lease_expires_at IS NOT NULL)),
    ADD CONSTRAINT cathedral_lifecycle_remaining_nonnegative
        CHECK (remaining_lifetime_ms IS NULL OR remaining_lifetime_ms >= 0);

-- +goose Down
ALTER TABLE public.cathedral_sandbox_lifecycle_operations
    DROP CONSTRAINT cathedral_lifecycle_remaining_nonnegative,
    DROP CONSTRAINT cathedral_lifecycle_dispatch_lease,
    DROP CONSTRAINT cathedral_lifecycle_dispatch_attempt_nonnegative,
    DROP COLUMN dispatch_lease_expires_at,
    DROP COLUMN dispatch_attempt,
    DROP COLUMN filesystem_only;
