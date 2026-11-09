-- This file provides schema definitions for sqlc that can't be parsed from migrations.
-- Migrations wrapped in DO $$ blocks are not visible to sqlc's static parser.
-- These statements are not executed - they're only read by sqlc for code generation.

ALTER TABLE teams ADD COLUMN slug TEXT NOT NULL;

CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE IF NOT EXISTS billing.sandbox_logs (
    sandbox_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    vcpu BIGINT NOT NULL,
    ram_mb BIGINT NOT NULL,
    total_disk_size_mb BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    stopped_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    team_id UUID NOT NULL
);

-- trg_compute_status_group fills env_builds.status from status_group until the
-- column is dropped, so no query names it.
ALTER TABLE public.env_builds DROP COLUMN status;
