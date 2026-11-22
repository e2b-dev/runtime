-- +goose Up
-- +goose NO TRANSACTION

ALTER TABLE teams
    ADD COLUMN IF NOT EXISTS sso_organization_id UUID,
    -- Members of the org are added to this team automatically on first SSO
    -- sign-in. When false, the team belongs to the org but access is granted by
    -- inviting org-domain accounts.
    ADD COLUMN IF NOT EXISTS sso_auto_join BOOLEAN NOT NULL DEFAULT false;

-- Non-unique: one SSO organization can map to multiple teams.
CREATE INDEX CONCURRENTLY IF NOT EXISTS teams_sso_organization_id_idx
    ON teams (sso_organization_id)
    WHERE sso_organization_id IS NOT NULL;

-- +goose Down
-- +goose NO TRANSACTION

DROP INDEX CONCURRENTLY IF EXISTS teams_sso_organization_id_idx;

ALTER TABLE teams
    DROP COLUMN IF EXISTS sso_organization_id,
    DROP COLUMN IF EXISTS sso_auto_join;
