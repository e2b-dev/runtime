-- +goose Up
-- +goose NO TRANSACTION

-- +goose StatementBegin
-- 0. Create helper function to safely cast text to UUID
CREATE OR REPLACE FUNCTION try_cast_uuid(p_value text) RETURNS uuid AS $$
BEGIN
    RETURN p_value::uuid;
EXCEPTION WHEN invalid_text_representation THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose StatementEnd

-- +goose StatementBegin
-- 1. Create the new env_build_assignments table
CREATE TABLE IF NOT EXISTS env_build_assignments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id TEXT NOT NULL,
    build_id UUID NOT NULL,
    tag TEXT NOT NULL,
    source TEXT DEFAULT 'app' NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_env_build_assignments_env 
        FOREIGN KEY (env_id) 
        REFERENCES envs(id) 
        ON DELETE CASCADE,
    
    CONSTRAINT fk_env_build_assignments_build 
        FOREIGN KEY (build_id) 
        REFERENCES env_builds(id) 
        ON DELETE CASCADE
);

-- PARTIAL UNIQUE INDEX: 
-- Enforces that for 'trigger' or 'migration', only ONE entry exists per env/build/tag.
-- Does NOT restrict 'app' entries, allowing multiple assignments from code.
CREATE UNIQUE INDEX IF NOT EXISTS uq_legacy_assignments ON env_build_assignments (env_id, build_id, tag)
WHERE source IN ('trigger', 'migration');

CREATE INDEX IF NOT EXISTS idx_env_build_assignments_env_tag_created 
    ON env_build_assignments (env_id, tag, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_env_build_assignments_env_build 
    ON env_build_assignments (env_id, build_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- 2. Validation Trigger: Prevent "Mixed" sources (Trigger vs App)
-- If code ('app') creates an entry, we want to ignore or remove the 'trigger' version.
CREATE OR REPLACE FUNCTION validate_assignment_source_takeover()
RETURNS TRIGGER AS $$
BEGIN
    -- If trigger tries to insert but an 'app' record already exists, skip it
    IF NEW.source = 'trigger' THEN
        IF EXISTS (
            SELECT 1 FROM env_build_assignments 
            WHERE env_id = NEW.env_id AND build_id = NEW.build_id AND tag = NEW.tag AND source = 'app'
        ) THEN
            RETURN NULL; -- Silently ignore
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

DROP TRIGGER IF EXISTS trigger_validate_assignment_source ON env_build_assignments;
CREATE TRIGGER trigger_validate_assignment_source
    BEFORE INSERT ON env_build_assignments
    FOR EACH ROW EXECUTE FUNCTION validate_assignment_source_takeover();
-- +goose StatementEnd

-- +goose StatementBegin
-- 3. Legacy Sync Trigger function
CREATE OR REPLACE FUNCTION sync_env_build_assignment()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.env_id IS NOT NULL THEN
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD.env_id != NEW.env_id) THEN
            -- Note: ON CONFLICT refers to the partial index uq_legacy_assignments
            INSERT INTO env_build_assignments (env_id, build_id, tag, source, created_at)
            VALUES (NEW.env_id, NEW.id, 'default', 'trigger', CURRENT_TIMESTAMP)
            ON CONFLICT (env_id, build_id, tag) WHERE source IN ('trigger', 'migration') DO NOTHING;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose StatementBegin
-- 4. Create trigger to automatically sync changes
DROP TRIGGER IF EXISTS trigger_sync_env_build_assignment ON env_builds;
CREATE TRIGGER trigger_sync_env_build_assignment
    AFTER INSERT OR UPDATE ON env_builds
    FOR EACH ROW
    EXECUTE FUNCTION sync_env_build_assignment();
-- +goose StatementEnd

-- 5. Create index on env_builds.created_at to speed up migration
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_env_builds_created_at ON env_builds (created_at);

-- +goose StatementBegin
-- 6. Create procedure to migrate existing data in batches
CREATE OR REPLACE PROCEDURE migrate_env_builds_to_assignments()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_size INT := 10000;
    rows_affected INT;
    total_migrated INT := 0;
    last_created_at TIMESTAMP WITH TIME ZONE := '1970-01-01 00:00:00+00';
    last_id UUID := '00000000-0000-0000-0000-000000000000';
    current_max_created_at TIMESTAMP WITH TIME ZONE;
    current_max_id UUID;
BEGIN
    LOOP
        -- Get the max (created_at, id) for this batch
        -- Using composite ordering: created_at for sequence, id as tie-breaker
        SELECT created_at, id INTO current_max_created_at, current_max_id
        FROM (
            SELECT created_at, id FROM env_builds 
            WHERE env_id IS NOT NULL 
            AND (created_at, id) > (last_created_at, last_id)
            ORDER BY created_at, id 
            LIMIT batch_size
        ) sub
        ORDER BY created_at DESC, id DESC
        LIMIT 1;
        
        -- Exit if no more records
        IF current_max_created_at IS NULL THEN
            EXIT;
        END IF;
        
        -- Insert the batch using composite range (handles duplicate timestamps correctly)
        INSERT INTO env_build_assignments (env_id, build_id, tag, source, created_at)
        SELECT 
            eb.env_id,
            eb.id as build_id,
            'default' as tag,
            'migration' as source,
            eb.created_at
        FROM env_builds eb
        WHERE eb.env_id IS NOT NULL
        AND (eb.created_at, eb.id) > (last_created_at, last_id)
        AND (eb.created_at, eb.id) <= (current_max_created_at, current_max_id)
        ON CONFLICT (env_id, build_id, tag) WHERE source IN ('trigger', 'migration') DO NOTHING;
        
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        total_migrated := total_migrated + rows_affected;
        last_created_at := current_max_created_at;
        last_id := current_max_id;
        
        COMMIT;
        RAISE NOTICE 'Migrated batch, processed up to created_at: %, id: % (total rows: %)', last_created_at, last_id, total_migrated;
    END LOOP;
    
    RAISE NOTICE 'Migration complete. Total rows migrated: %', total_migrated;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
-- 7. Run the migration procedure
CALL migrate_env_builds_to_assignments();
-- +goose StatementEnd

-- 8. Drop temporary index after migration
DROP INDEX CONCURRENTLY IF EXISTS idx_env_builds_created_at;

-- +goose StatementBegin
-- 9. Clean up the migration procedure
DROP PROCEDURE IF EXISTS migrate_env_builds_to_assignments();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP PROCEDURE IF EXISTS migrate_env_builds_to_assignments();
DROP TRIGGER IF EXISTS trigger_sync_env_build_assignment ON env_builds;
DROP TRIGGER IF EXISTS trigger_validate_assignment_source ON env_build_assignments;
DROP FUNCTION IF EXISTS sync_env_build_assignment();
DROP FUNCTION IF EXISTS validate_assignment_source_takeover();
DROP TABLE IF EXISTS env_build_assignments;
DROP FUNCTION IF EXISTS try_cast_uuid(text);
-- +goose StatementEnd
