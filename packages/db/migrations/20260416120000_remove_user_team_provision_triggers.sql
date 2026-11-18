-- +goose Up

-- The application now owns auth user projection and default team bootstrap.
-- Remove the legacy database triggers/functions that used to keep public.users
-- in sync and auto-create default teams on signup.

DROP TRIGGER IF EXISTS sync_inserts_to_public_users ON auth.users;
DROP TRIGGER IF EXISTS sync_updates_to_public_users ON auth.users;
DROP TRIGGER IF EXISTS sync_deletes_to_public_users ON auth.users;
DROP TRIGGER IF EXISTS post_user_signup ON public.users;

DROP FUNCTION IF EXISTS public.sync_insert_auth_users_to_public_users_trigger();
DROP FUNCTION IF EXISTS public.sync_update_auth_users_to_public_users_trigger();
DROP FUNCTION IF EXISTS public.sync_delete_auth_users_to_public_users_trigger();
DROP FUNCTION IF EXISTS public.post_user_signup();
DROP FUNCTION IF EXISTS public.extra_for_post_user_signup(uuid, uuid);

-- trigger_user exists only where an earlier release created it.
-- +goose StatementBegin
DO $revoke$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'trigger_user') THEN
        REVOKE INSERT ON public.users FROM trigger_user;
        REVOKE SELECT (id) ON public.users FROM trigger_user;
        REVOKE UPDATE ON public.users FROM trigger_user;
        REVOKE DELETE ON public.users FROM trigger_user;

        REVOKE SELECT, INSERT, TRIGGER ON public.teams FROM trigger_user;
        REVOKE INSERT ON public.users_teams FROM trigger_user;
        REVOKE INSERT ON public.team_api_keys FROM trigger_user;
        REVOKE INSERT ON public.access_tokens FROM trigger_user;

        REVOKE CREATE, USAGE ON SCHEMA public FROM trigger_user;
        REVOKE USAGE ON SCHEMA auth FROM trigger_user;
    END IF;
END
$revoke$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

CREATE OR REPLACE FUNCTION public.extra_for_post_user_signup(user_id uuid, team_id uuid)
    RETURNS void
    LANGUAGE plpgsql
AS $extra_for_post_user_signup$
DECLARE
BEGIN
END
$extra_for_post_user_signup$ SECURITY DEFINER SET search_path = public;

CREATE OR REPLACE FUNCTION public.post_user_signup()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $post_user_signup$
DECLARE
    team_id uuid;
BEGIN
    RAISE NOTICE 'Creating default team for user %', NEW.id;
    INSERT INTO public.teams(name, tier, email) VALUES (NEW.email, 'base_v1', NEW.email) RETURNING id INTO team_id;
    INSERT INTO public.users_teams(user_id, team_id, is_default) VALUES (NEW.id, team_id, true);
    RAISE NOTICE 'Created default team for user % and team %', NEW.id, team_id;

    PERFORM public.extra_for_post_user_signup(NEW.id, team_id);

    RETURN NEW;
END
$post_user_signup$ SECURITY DEFINER SET search_path = public;

CREATE OR REPLACE FUNCTION public.sync_insert_auth_users_to_public_users_trigger() RETURNS TRIGGER
LANGUAGE plpgsql
AS $func$
BEGIN
    INSERT INTO public.users (id, email)
    VALUES (NEW.id, NEW.email);

    RETURN NEW;
END;
$func$ SECURITY DEFINER SET search_path = public;

CREATE OR REPLACE FUNCTION public.sync_update_auth_users_to_public_users_trigger() RETURNS TRIGGER
LANGUAGE plpgsql
AS $func$
BEGIN
    UPDATE public.users
    SET email = NEW.email,
        updated_at = now()
    WHERE id = NEW.id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'User with id % does not exist in public.users', NEW.id;
    END IF;

    RETURN NEW;
END;
$func$ SECURITY DEFINER SET search_path = public;

CREATE OR REPLACE FUNCTION public.sync_delete_auth_users_to_public_users_trigger() RETURNS TRIGGER
LANGUAGE plpgsql
AS $func$
BEGIN
    DELETE FROM public.users WHERE id = OLD.id;
    RETURN OLD;
END;
$func$ SECURITY DEFINER SET search_path = public;

CREATE TRIGGER sync_inserts_to_public_users
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.sync_insert_auth_users_to_public_users_trigger();

CREATE TRIGGER sync_updates_to_public_users
    AFTER UPDATE ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.sync_update_auth_users_to_public_users_trigger();

CREATE TRIGGER sync_deletes_to_public_users
    AFTER DELETE ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.sync_delete_auth_users_to_public_users_trigger();

CREATE TRIGGER post_user_signup
    AFTER INSERT ON public.users
    FOR EACH ROW EXECUTE FUNCTION public.post_user_signup();

-- +goose StatementEnd
