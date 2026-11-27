-- +goose Up
-- +goose NO TRANSACTION

/*
This migration adds namespace support to env_aliases for team-scoped template aliases.

It performs the following steps:

1. Adds a nullable namespace column to env_aliases table
   - Existing aliases keep namespace=NULL so they can be found via fallback logic
   - New aliases created by new code will have namespace set to the team's slug

2. Creates an index on (alias, namespace) for efficient lookups
*/

ALTER TABLE "public"."env_aliases" ADD COLUMN IF NOT EXISTS "namespace" text NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_env_aliases_alias_namespace" 
ON "public"."env_aliases" ("alias", "namespace");

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS "public"."idx_env_aliases_alias_namespace";
ALTER TABLE "public"."env_aliases" DROP COLUMN IF EXISTS "namespace";
