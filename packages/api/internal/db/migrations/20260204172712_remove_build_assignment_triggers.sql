-- +goose Up
-- +goose NO TRANSACTION

-- +goose StatementBegin
-- Remove triggers used during the transition period for build assignments.
-- The application now directly manages env_build_assignments, so these are no longer needed.

-- Drop the sync trigger that auto-created assignments when env_builds were updated
DROP TRIGGER IF EXISTS trigger_sync_env_build_assignment ON env_builds;

-- Drop the validation trigger that handled 'app' vs 'trigger' source conflicts
DROP TRIGGER IF EXISTS trigger_validate_assignment_source ON env_build_assignments;

-- Drop the functions used by these triggers
DROP FUNCTION IF EXISTS sync_env_build_assignment();
DROP FUNCTION IF EXISTS validate_assignment_source_takeover();
-- +goose StatementEnd

-- Drop the index on env_id since we're no longer using it
DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_env_status_created;

-- +goose StatementBegin
-- Drop the foreign key constraint and make env_id nullable (will be removed in a future migration)
ALTER TABLE env_builds DROP CONSTRAINT IF EXISTS env_builds_envs_builds;
ALTER TABLE env_builds ALTER COLUMN env_id DROP NOT NULL;

-- Add a backfill trigger that populates env_id from assignments for backward compatibility
-- This allows old code to read env_id while new code no longer writes it
CREATE OR REPLACE FUNCTION backfill_env_id_from_assignment()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE env_builds SET env_id = NEW.env_id WHERE id = NEW.build_id AND env_id IS NULL;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_backfill_env_id
    AFTER INSERT ON env_build_assignments
    FOR EACH ROW EXECUTE FUNCTION backfill_env_id_from_assignment();
-- +goose StatementEnd

-- +goose Down
-- +goose NO TRANSACTION

-- +goose StatementBegin
-- Drop the backfill trigger (added for backward compat during transition)
DROP TRIGGER IF EXISTS trigger_backfill_env_id ON env_build_assignments;
DROP FUNCTION IF EXISTS backfill_env_id_from_assignment();

-- Populate env_id from env_build_assignments for any builds that have NULL env_id
-- This is needed before we can restore the NOT NULL constraint
UPDATE env_builds eb
SET env_id = (
    SELECT eba.env_id 
    FROM env_build_assignments eba 
    WHERE eba.build_id = eb.id 
    LIMIT 1
)
WHERE eb.env_id IS NULL;

-- Delete any orphaned builds that don't have an assignment (shouldn't happen, but safety first)
DELETE FROM env_builds WHERE env_id IS NULL;

-- Restore the NOT NULL constraint and foreign key constraint
ALTER TABLE env_builds ALTER COLUMN env_id SET NOT NULL;
ALTER TABLE env_builds ADD CONSTRAINT env_builds_envs_builds 
    FOREIGN KEY (env_id) REFERENCES envs(id) ON DELETE CASCADE;
-- +goose StatementEnd

-- Recreate the index
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_env_status_created
    ON env_builds (env_id, status, created_at DESC);

-- +goose StatementBegin
-- Recreate the sync trigger function
CREATE OR REPLACE FUNCTION sync_env_build_assignment()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.env_id IS NOT NULL THEN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.env_id != NEW.env_id) THEN
            INSERT INTO env_build_assignments (env_id, build_id, tag, source, created_at)
            VALUES (NEW.env_id, NEW.id, 'default', 'trigger', CURRENT_TIMESTAMP)
            ON CONFLICT (env_id, build_id, tag) WHERE source IN ('trigger', 'migration') DO NOTHING;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Recreate the sync trigger
CREATE TRIGGER trigger_sync_env_build_assignment
    AFTER INSERT OR UPDATE ON env_builds
    FOR EACH ROW
    EXECUTE FUNCTION sync_env_build_assignment();

-- Recreate the validation trigger function
CREATE OR REPLACE FUNCTION validate_assignment_source_takeover()
RETURNS TRIGGER AS $$
BEGIN
    -- If trigger tries to insert but an 'app' record already exists, skip it
    IF NEW.source = 'trigger' THEN
        IF EXISTS (
            SELECT 1 FROM env_build_assignments 
            WHERE env_id = NEW.env_id AND build_id = NEW.build_id AND tag = NEW.tag AND source = 'app'
        ) THEN
            RETURN NULL;
        END IF;
    
    -- If 'app' inserts, we clean up any legacy 'trigger' or 'migration' records for this identity
    ELSIF NEW.source = 'app' THEN
        DELETE FROM env_build_assignments 
        WHERE env_id = NEW.env_id AND build_id = NEW.build_id AND tag = NEW.tag 
        AND source IN ('trigger', 'migration');
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Recreate the validation trigger
CREATE TRIGGER trigger_validate_assignment_source
    BEFORE INSERT ON env_build_assignments
    FOR EACH ROW EXECUTE FUNCTION validate_assignment_source_takeover();
-- +goose StatementEnd
