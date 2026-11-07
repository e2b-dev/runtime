-- +goose NO TRANSACTION
-- +goose Up
-- Each DROP TRIGGER locks its table exclusively. Without a transaction, the
-- lock on teams is released before the one on snapshots is requested.
SET lock_timeout = '5s';

-- Every team insert supplies its slug.
DROP TRIGGER IF EXISTS team_slug_trigger ON public.teams;

-- UpsertSnapshot creates the snapshot's env with source 'snapshot'.
DROP TRIGGER IF EXISTS trg_sync_env_source_on_snapshot ON public.snapshots;

RESET lock_timeout;

DROP FUNCTION IF EXISTS public.generate_team_slug_trigger();
DROP FUNCTION IF EXISTS public.generate_team_slug(text);
DROP FUNCTION IF EXISTS public.sync_env_source_on_snapshot_insert();
DROP FUNCTION IF EXISTS public.generate_team_api_key();
DROP FUNCTION IF EXISTS public.generate_default_team();

-- Discards the team names 20260723120001 renamed.
DROP TABLE IF EXISTS public._backup_teams_default_team_rename;

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.generate_default_team() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
    team_id                 uuid;
BEGIN
    INSERT INTO public.teams(name, is_default, tier, email) VALUES (NEW.email, true, 'base', NEW.email) RETURNING id INTO team_id;
    INSERT INTO public.users_teams(user_id, team_id) VALUES (NEW.id, team_id);
    RAISE NOTICE 'Created default team for user % and team %', NEW.id, team_id;
    RETURN NEW;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.generate_team_api_key() RETURNS text
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
    team_api_key_prefix TEXT := 'e2b_';
    generated_key TEXT;
BEGIN
    generated_key := encode(extensions.gen_random_bytes(20), 'hex');
    RETURN team_api_key_prefix || generated_key;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.generate_team_slug(name text) RETURNS text
    LANGUAGE plpgsql
    AS $$
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
    $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.generate_team_slug_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
    $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.sync_env_source_on_snapshot_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE "public"."envs" SET source = 'snapshot' WHERE id = NEW.env_id;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

SET lock_timeout = '5s';

CREATE OR REPLACE TRIGGER team_slug_trigger
    BEFORE INSERT ON public.teams
    FOR EACH ROW EXECUTE FUNCTION public.generate_team_slug_trigger();

CREATE OR REPLACE TRIGGER trg_sync_env_source_on_snapshot
    AFTER INSERT ON public.snapshots
    FOR EACH ROW EXECUTE FUNCTION public.sync_env_source_on_snapshot_insert();

RESET lock_timeout;

-- The discarded names are not restored.
CREATE TABLE IF NOT EXISTS public._backup_teams_default_team_rename (
    id uuid PRIMARY KEY,
    old_name text NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now()
);
