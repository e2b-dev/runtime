-- +goose Up
ALTER TABLE public.clusters ADD COLUMN telemetry_client_id text;
CREATE UNIQUE INDEX clusters_telemetry_client_id_key ON public.clusters(telemetry_client_id);

-- +goose Down
ALTER TABLE public.clusters DROP COLUMN telemetry_client_id;
