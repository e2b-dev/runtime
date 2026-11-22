-- +goose Up
-- +goose NO TRANSACTION

-- DROP waits on in-flight lock holders rather than building anything; bound
-- it so it neither hangs unbounded nor dies to a short session default.
SET statement_timeout = '1h';

-- Two unique indexes cover the same key column: the UNIQUE constraint's own
-- index (access_tokens_access_token_hash_key) and this standalone unique
-- copy. Identical indexes are interchangeable to the planner, so lookups
-- continue on the constraint's index, uniqueness enforcement is untouched,
-- and every token write stops paying for the second copy. The standalone
-- one is the droppable twin (the constraint-backed index cannot be dropped
-- by DROP INDEX, which makes this fail-loud safe).
DROP INDEX CONCURRENTLY IF EXISTS public.idx_access_tokens_access_token_hash;

-- The migrator session is reused for subsequent migrations: restore its
-- baseline (scripts/migrator.go sets 3h per connection).
SET statement_timeout = '3h';

-- +goose Down
-- +goose NO TRANSACTION

SET statement_timeout = '1h';

-- An interrupted CONCURRENTLY build leaves an INVALID index that IF NOT
-- EXISTS would then skip forever — clear any leftover before rebuilding.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_access_tokens_access_token_hash;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_access_tokens_access_token_hash
    ON public.access_tokens (access_token_hash);

SET statement_timeout = '3h';
