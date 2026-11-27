-- +goose Up
-- +goose NO TRANSACTION

-- 1. Add new UUID column with default (existing rows get auto-populated)
ALTER TABLE public.users_teams ADD COLUMN IF NOT EXISTS uuid_id UUID DEFAULT gen_random_uuid() NOT NULL;

-- 2. Build unique index concurrently to avoid holding ACCESS EXCLUSIVE lock during index scan
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS users_teams_uuid_id_idx
  ON public.users_teams (uuid_id);

-- 3. Swap primary key using the pre-built index (brief metadata lock only)
ALTER TABLE public.users_teams
  DROP CONSTRAINT users_teams_pkey,
  ADD CONSTRAINT users_teams_pkey PRIMARY KEY USING INDEX users_teams_uuid_id_idx;

-- +goose Down
-- +goose NO TRANSACTION

-- 1. Build unique index on old bigint id concurrently
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS users_teams_id_idx
  ON public.users_teams (id);

-- 2. Swap primary key back to bigint id
ALTER TABLE public.users_teams
  DROP CONSTRAINT users_teams_pkey,
  ADD CONSTRAINT users_teams_pkey PRIMARY KEY USING INDEX users_teams_id_idx;

-- 3. Drop the uuid column
ALTER TABLE public.users_teams DROP COLUMN IF EXISTS uuid_id;
