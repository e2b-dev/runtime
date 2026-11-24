-- +goose Up
-- +goose NO TRANSACTION

-- Phase 1: DDL changes (brief ACCESS EXCLUSIVE per table, no row scanning)

ALTER TABLE public.users DROP CONSTRAINT IF EXISTS users_id_fkey;

ALTER TABLE public.access_tokens
  DROP CONSTRAINT access_tokens_users_access_tokens,
  ADD CONSTRAINT access_tokens_users_access_tokens
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE CASCADE
    NOT VALID;

ALTER TABLE public.users_teams
  DROP CONSTRAINT users_teams_users_users,
  ADD CONSTRAINT users_teams_users_users
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE CASCADE
    NOT VALID;

ALTER TABLE public.team_api_keys
  DROP CONSTRAINT team_api_keys_users_created_api_keys,
  ADD CONSTRAINT team_api_keys_users_created_api_keys
    FOREIGN KEY (created_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.envs
  DROP CONSTRAINT envs_users_created_envs,
  ADD CONSTRAINT envs_users_created_envs
    FOREIGN KEY (created_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.users_teams
  DROP CONSTRAINT users_teams_added_by_user,
  ADD CONSTRAINT users_teams_added_by_user
    FOREIGN KEY (added_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.addons
  DROP CONSTRAINT addons_users_addons,
  ADD CONSTRAINT addons_users_addons
    FOREIGN KEY (added_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE NO ACTION
    NOT VALID;

-- Phase 2: Move post_user_signup from auth.users to public.users.
-- This eliminates the trigger ordering concern: the sync trigger fires on auth.users
-- and inserts into public.users, which then fires post_user_signup naturally.
DROP TRIGGER IF EXISTS post_user_signup ON auth.users;

CREATE TRIGGER post_user_signup
    AFTER INSERT ON public.users
    FOR EACH ROW EXECUTE FUNCTION public.post_user_signup();

-- Phase 3: Add delete-sync trigger so auth.users deletions propagate to public.users
-- (which then cascades to access_tokens, users_teams, etc. via the re-pointed FKs).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.sync_delete_auth_users_to_public_users_trigger() RETURNS TRIGGER
LANGUAGE plpgsql
AS $func$
BEGIN
    DELETE FROM public.users WHERE id = OLD.id;
    RETURN OLD;
END;
$func$ SECURITY DEFINER SET search_path = public;
-- +goose StatementEnd

CREATE TRIGGER sync_deletes_to_public_users
    AFTER DELETE ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.sync_delete_auth_users_to_public_users_trigger();

-- Phase 4: Validate constraints (ShareUpdateExclusiveLock only, allows concurrent DML)
ALTER TABLE public.access_tokens VALIDATE CONSTRAINT access_tokens_users_access_tokens;
ALTER TABLE public.users_teams VALIDATE CONSTRAINT users_teams_users_users;
ALTER TABLE public.team_api_keys VALIDATE CONSTRAINT team_api_keys_users_created_api_keys;
ALTER TABLE public.envs VALIDATE CONSTRAINT envs_users_created_envs;
ALTER TABLE public.users_teams VALIDATE CONSTRAINT users_teams_added_by_user;
ALTER TABLE public.addons VALIDATE CONSTRAINT addons_users_addons;

-- +goose Down
-- +goose NO TRANSACTION

-- NOTE: This down migration is only safe within a narrow window after deploy.
-- Once new users sign up (inserted into public.users via triggers), rolling back
-- will fail if those users don't have corresponding auth.users rows.

-- Move post_user_signup back to auth.users
DROP TRIGGER IF EXISTS post_user_signup ON public.users;

CREATE TRIGGER post_user_signup
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.post_user_signup();

-- Drop delete-sync trigger and function
DROP TRIGGER IF EXISTS sync_deletes_to_public_users ON auth.users;
DROP FUNCTION IF EXISTS public.sync_delete_auth_users_to_public_users_trigger();

-- Restore FK from public.users to auth.users
ALTER TABLE public.users
  ADD CONSTRAINT users_id_fkey FOREIGN KEY (id) REFERENCES auth.users(id) ON DELETE CASCADE
  NOT VALID;

ALTER TABLE public.access_tokens
  DROP CONSTRAINT access_tokens_users_access_tokens,
  ADD CONSTRAINT access_tokens_users_access_tokens
    FOREIGN KEY (user_id) REFERENCES auth.users(id) ON UPDATE NO ACTION ON DELETE CASCADE
    NOT VALID;

ALTER TABLE public.users_teams
  DROP CONSTRAINT users_teams_users_users,
  ADD CONSTRAINT users_teams_users_users
    FOREIGN KEY (user_id) REFERENCES auth.users(id) ON UPDATE NO ACTION ON DELETE CASCADE
    NOT VALID;

ALTER TABLE public.team_api_keys
  DROP CONSTRAINT team_api_keys_users_created_api_keys,
  ADD CONSTRAINT team_api_keys_users_created_api_keys
    FOREIGN KEY (created_by) REFERENCES auth.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.envs
  DROP CONSTRAINT envs_users_created_envs,
  ADD CONSTRAINT envs_users_created_envs
    FOREIGN KEY (created_by) REFERENCES auth.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.users_teams
  DROP CONSTRAINT users_teams_added_by_user,
  ADD CONSTRAINT users_teams_added_by_user
    FOREIGN KEY (added_by) REFERENCES auth.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.addons
  DROP CONSTRAINT addons_users_addons,
  ADD CONSTRAINT addons_users_addons
    FOREIGN KEY (added_by) REFERENCES auth.users(id) ON UPDATE NO ACTION ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.users VALIDATE CONSTRAINT users_id_fkey;
ALTER TABLE public.access_tokens VALIDATE CONSTRAINT access_tokens_users_access_tokens;
ALTER TABLE public.users_teams VALIDATE CONSTRAINT users_teams_users_users;
ALTER TABLE public.team_api_keys VALIDATE CONSTRAINT team_api_keys_users_created_api_keys;
ALTER TABLE public.envs VALIDATE CONSTRAINT envs_users_created_envs;
ALTER TABLE public.users_teams VALIDATE CONSTRAINT users_teams_added_by_user;
ALTER TABLE public.addons VALIDATE CONSTRAINT addons_users_addons;
