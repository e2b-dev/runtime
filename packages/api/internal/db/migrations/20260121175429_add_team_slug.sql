-- +goose Up
-- +goose StatementBegin

/*
This migration adds team slugs and profile pictures to support user-friendly URLs and team branding.

It performs the following steps:

1. Adds a new column to the teams table:
   - slug: A URL-friendly version of the team name (e.g. "acme-inc")

2. Creates a slug generation function that:
   - Takes a team name and converts it to a URL-friendly format
   - Removes special characters, accents, and spaces
   - Handles email addresses by only using the part before @
   - Converts to lowercase and replaces spaces with hyphens

3. Installs the unaccent PostgreSQL extension for proper accent handling

4. Generates initial slugs for all existing teams:
   - Uses the team name as base for the slug
   - If multiple teams would have the same slug, appends part of the team ID
     to ensure uniqueness

5. Sets up automatic slug generation for new teams:
   - Creates a trigger that runs before team insertion
   - Generates a unique slug using random suffixes if needed
   - Only generates a slug if one isn't explicitly provided

6. Enforces slug uniqueness with a database constraint
*/

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_schema = 'public' 
    AND table_name = 'teams' 
    AND column_name = 'slug'
  ) THEN

    ALTER TABLE teams
    ADD COLUMN slug TEXT;

    CREATE EXTENSION IF NOT EXISTS unaccent;

    CREATE OR REPLACE FUNCTION generate_team_slug(name TEXT)
    RETURNS TEXT AS $func$
    DECLARE
      base_name TEXT;
    BEGIN
      base_name := SPLIT_PART(name, '@', 1);

      RETURN LOWER(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            UNACCENT(TRIM(base_name)),
            '[^a-zA-Z0-9\s-]',
            '',
            'g'
          ),
          '\s+',
          '-',
          'g'
        )
      );
    END;
    $func$ LANGUAGE plpgsql;

    WITH numbered_teams AS (
      SELECT
        id,
        name,
        generate_team_slug(name) as base_slug,
        ROW_NUMBER() OVER (PARTITION BY generate_team_slug(name) ORDER BY created_at) as slug_count
      FROM teams
      WHERE slug IS NULL
    )
    UPDATE teams
    SET slug =
      CASE
        WHEN t.slug_count = 1 THEN t.base_slug
        ELSE t.base_slug || '-' || SUBSTRING(teams.id::text, 1, 4)
      END
    FROM numbered_teams t
    WHERE teams.id = t.id;

    CREATE OR REPLACE FUNCTION generate_team_slug_trigger()
    RETURNS TRIGGER AS $func$
    DECLARE
      base_slug TEXT;
      test_slug TEXT;
      suffix TEXT;
    BEGIN
      IF NEW.slug IS NULL THEN
        base_slug := generate_team_slug(NEW.name);
        test_slug := base_slug;

        WHILE EXISTS (SELECT 1 FROM teams WHERE slug = test_slug) LOOP
          suffix := SUBSTRING(gen_random_uuid()::text, 1, 4);
          test_slug := base_slug || '-' || suffix;
        END LOOP;

        NEW.slug := test_slug;
      END IF;
      RETURN NEW;
    END;
    $func$ LANGUAGE plpgsql;

    CREATE TRIGGER team_slug_trigger
    BEFORE INSERT ON teams
    FOR EACH ROW
    EXECUTE FUNCTION generate_team_slug_trigger();

    ALTER TABLE teams
    ADD CONSTRAINT teams_slug_unique UNIQUE (slug);

    ALTER TABLE teams
    ALTER COLUMN slug SET NOT NULL;

  END IF;
END $$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

DROP TRIGGER IF EXISTS team_slug_trigger ON teams;
DROP FUNCTION IF EXISTS generate_team_slug_trigger();
DROP FUNCTION IF EXISTS generate_team_slug(TEXT);
ALTER TABLE teams DROP CONSTRAINT IF EXISTS teams_slug_unique;
ALTER TABLE teams DROP COLUMN IF EXISTS slug;           

-- +goose StatementEnd
