-- +goose Up
-- The cluster service holds the cluster records; nothing here reads or writes
-- this table any more.
SET LOCAL lock_timeout = '5s';
DROP TABLE IF EXISTS public.clusters;

-- +goose Down
-- The table comes back empty: its rows are gone.
CREATE TABLE public.clusters (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    endpoint text NOT NULL,
    endpoint_tls boolean DEFAULT true NOT NULL,
    token text NOT NULL,
    sandbox_proxy_domain text,
    auth_org_id text,
    name text NOT NULL,
    deletion_protection boolean DEFAULT true NOT NULL,
    deletion_requested_at timestamp with time zone,
    telemetry_client_id text,
    CONSTRAINT clusters_deletion_requested_unprotected CHECK (deletion_requested_at IS NULL OR NOT deletion_protection)
);
CREATE UNIQUE INDEX clusters_auth_org_id_idx ON public.clusters (auth_org_id) WHERE auth_org_id IS NOT NULL;
CREATE UNIQUE INDEX clusters_telemetry_client_id_key ON public.clusters (telemetry_client_id);
