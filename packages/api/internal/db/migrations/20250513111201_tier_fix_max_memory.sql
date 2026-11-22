-- +goose Up
-- +goose StatementBegin
ALTER TABLE tiers
    ALTER COLUMN "max_ram_mb" SET DEFAULT '8192'::bigint;
UPDATE tiers SET "max_ram_mb" = 8192 WHERE "max_ram_mb" = 8096;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE tiers
    ALTER COLUMN "max_ram_mb" SET DEFAULT '8096'::bigint;
-- +goose StatementEnd
