-- +goose Up
-- +goose NO TRANSACTION

-- Phase 1: DDL changes (brief ACCESS EXCLUSIVE per table, no row scanning)

ALTER TABLE public.users DROP CONSTRAINT IF EXISTS users_id_fkey;

ALTER TABLE public.access_tokens
  DROP CONSTRAINT IF EXISTS access_tokens_users_access_tokens,
  ADD CONSTRAINT access_tokens_users_access_tokens
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE CASCADE
    NOT VALID;

ALTER TABLE public.users_teams
  DROP CONSTRAINT IF EXISTS users_teams_users_users,
  ADD CONSTRAINT users_teams_users_users
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE CASCADE
    NOT VALID;

ALTER TABLE public.team_api_keys
  DROP CONSTRAINT IF EXISTS team_api_keys_users_created_api_keys,
  ADD CONSTRAINT team_api_keys_users_created_api_keys
    FOREIGN KEY (created_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.envs
  DROP CONSTRAINT IF EXISTS envs_users_created_envs,
  ADD CONSTRAINT envs_users_created_envs
    FOREIGN KEY (created_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.users_teams
  DROP CONSTRAINT IF EXISTS users_teams_added_by_user,
  ADD CONSTRAINT users_teams_added_by_user
    FOREIGN KEY (added_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE SET NULL
    NOT VALID;

ALTER TABLE public.addons
  DROP CONSTRAINT IF EXISTS addons_users_addons,
  ADD CONSTRAINT addons_users_addons
    FOREIGN KEY (added_by) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE NO ACTION
    NOT VALID;

-- Phase 2: post_user_signup fires on public.users. Databases created before
-- this migration carry it on auth.users.
DROP TRIGGER IF EXISTS post_user_signup ON auth.users;

CREATE TRIGGER post_user_signup
    AFTER INSERT ON public.users
    FOR EACH ROW EXECUTE FUNCTION public.post_user_signup();

-- Phase 3: Validate constraints (ShareUpdateExclusiveLock only, allows concurrent DML)
ALTER TABLE public.access_tokens VALIDATE CONSTRAINT access_tokens_users_access_tokens;
ALTER TABLE public.users_teams VALIDATE CONSTRAINT users_teams_users_users;
ALTER TABLE public.team_api_keys VALIDATE CONSTRAINT team_api_keys_users_created_api_keys;
ALTER TABLE public.envs VALIDATE CONSTRAINT envs_users_created_envs;
ALTER TABLE public.users_teams VALIDATE CONSTRAINT users_teams_added_by_user;
ALTER TABLE public.addons VALIDATE CONSTRAINT addons_users_addons;

-- +goose Down
-- +goose NO TRANSACTION

DROP TRIGGER IF EXISTS post_user_signup ON public.users;

ALTER TABLE public.access_tokens DROP CONSTRAINT IF EXISTS access_tokens_users_access_tokens;
ALTER TABLE public.users_teams DROP CONSTRAINT IF EXISTS users_teams_users_users;
ALTER TABLE public.team_api_keys DROP CONSTRAINT IF EXISTS team_api_keys_users_created_api_keys;
ALTER TABLE public.envs DROP CONSTRAINT IF EXISTS envs_users_created_envs;
ALTER TABLE public.users_teams DROP CONSTRAINT IF EXISTS users_teams_added_by_user;
ALTER TABLE public.addons DROP CONSTRAINT IF EXISTS addons_users_addons;
