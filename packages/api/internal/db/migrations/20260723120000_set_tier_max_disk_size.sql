-- +goose Up
UPDATE public.tiers
SET max_disk_size_mb = CASE
    WHEN id = 'base_v1' THEN 25600
    ELSE 51200
END;

-- +goose Down
UPDATE public.tiers
SET max_disk_size_mb = default_free_disk_size_mb + 25000;
