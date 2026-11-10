-- +goose Up
ALTER TABLE sandbox_egress_local
    MODIFY TTL toDateTime(ingested_at) + INTERVAL 30 DAY;

-- No Down: shortening retention can irreversibly delete existing data.
