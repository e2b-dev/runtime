-- +goose Up
-- A deleted team keeps its row, so what references it stays valid, and is
-- always blocked, so a reader that checks only is_blocked still refuses it.
ALTER TABLE public.teams ADD COLUMN deleted_at timestamptz;
ALTER TABLE public.teams ADD CONSTRAINT teams_deleted_blocked_check
    CHECK (deleted_at IS NULL OR is_blocked);

-- +goose Down
ALTER TABLE public.teams DROP CONSTRAINT teams_deleted_blocked_check;
ALTER TABLE public.teams DROP COLUMN deleted_at;
