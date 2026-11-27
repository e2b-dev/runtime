-- +goose Up
-- +goose StatementBegin
ALTER TABLE sandbox_host_stats_local
    ADD COLUMN IF NOT EXISTS guest_memory_measured Bool DEFAULT false CODEC (ZSTD(1)),
    ADD COLUMN IF NOT EXISTS guest_memory_held_bytes UInt64 DEFAULT 0 CODEC (ZSTD(1)),
    ADD COLUMN IF NOT EXISTS delta_guest_memory_held_mib_millis UInt64 DEFAULT 0 CODEC (ZSTD(1));
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_host_stats
    ADD COLUMN IF NOT EXISTS guest_memory_measured Bool DEFAULT false CODEC (ZSTD(1)),
    ADD COLUMN IF NOT EXISTS guest_memory_held_bytes UInt64 DEFAULT 0 CODEC (ZSTD(1)),
    ADD COLUMN IF NOT EXISTS delta_guest_memory_held_mib_millis UInt64 DEFAULT 0 CODEC (ZSTD(1));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE sandbox_host_stats
    DROP COLUMN IF EXISTS guest_memory_measured,
    DROP COLUMN IF EXISTS guest_memory_held_bytes,
    DROP COLUMN IF EXISTS delta_guest_memory_held_mib_millis;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE sandbox_host_stats_local
    DROP COLUMN IF EXISTS guest_memory_measured,
    DROP COLUMN IF EXISTS guest_memory_held_bytes,
    DROP COLUMN IF EXISTS delta_guest_memory_held_mib_millis;
-- +goose StatementEnd
