-- +goose Up
-- +goose NO TRANSACTION

-- 1. Create the unique index FIRST (before any destructive operations)
-- This ensures we have constraint protection throughout the migration.
-- Using 'NULLS NOT DISTINCT' so (alias, NULL) is treated as a unique pair.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_env_aliases_alias_namespace_unique 
  ON public.env_aliases (alias, namespace) 
  NULLS NOT DISTINCT;

-- 2. Drop the old non-unique index (now redundant)
DROP INDEX CONCURRENTLY IF EXISTS idx_env_aliases_alias_namespace;

-- 3. Add the id column with default (existing rows get auto-populated)
ALTER TABLE public.env_aliases ADD COLUMN IF NOT EXISTS id UUID DEFAULT gen_random_uuid() NOT NULL;

-- 4. Swap primary key (atomic operation)
ALTER TABLE public.env_aliases 
  DROP CONSTRAINT env_aliases_pkey,
  ADD CONSTRAINT env_aliases_uuid_pkey PRIMARY KEY (id);

-- +goose Down
-- +goose NO TRANSACTION

-- Recreate the original non-unique index if it doesn't exist
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_aliases_alias_namespace 
  ON public.env_aliases (alias, namespace);

-- Drop the unique index
DROP INDEX CONCURRENTLY IF EXISTS idx_env_aliases_alias_namespace_unique;

-- Restore original primary key (atomic operation)
ALTER TABLE public.env_aliases 
  DROP CONSTRAINT env_aliases_uuid_pkey,
  ADD CONSTRAINT env_aliases_pkey PRIMARY KEY (alias);

-- Drop id column if it exists
ALTER TABLE public.env_aliases DROP COLUMN IF EXISTS id;
