-- +goose Up
UPDATE public.default_limits SET concurrent_sandboxes = 10 WHERE concurrent_sandboxes = 20;
UPDATE public.default_limits SET disk_mb = 10240 WHERE disk_mb = 512;
UPDATE public.default_limits SET default_free_disk_size_mb = 10240
WHERE default_free_disk_size_mb = 512 AND max_disk_size_mb >= 10240;
UPDATE public.default_limits SET api_team_rps_list = 10 WHERE api_team_rps_list = 0;
UPDATE public.default_limits SET api_team_rps_delete = 20 WHERE api_team_rps_delete = 0;

-- +goose Down
-- Keep the configured defaults; which columns this changed was not recorded.
