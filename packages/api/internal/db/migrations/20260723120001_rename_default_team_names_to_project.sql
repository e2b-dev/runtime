-- +goose Up
-- +goose StatementBegin
-- Data-only migration: rename generator-produced default team names to the
-- project naming introduced by the dashboard-api provisioning change [EN-1885].
-- Matches ONLY the two exact forms the old generator ever produced
-- (provisioning/names.go): the bare form is exact-matched, the possessive
-- form is suffix-anchored. Customer-chosen names that merely contain the
-- phrase (e.g. 'My Default Team Stuff') are untouched.
--
-- Rollback map: affected rows are captured into
-- public._backup_teams_default_team_rename atomically with the update, in
-- this same transaction. Restore (if ever needed):
--   UPDATE public.teams t SET name = b.old_name
--   FROM public._backup_teams_default_team_rename b WHERE t.id = b.id;
-- A follow-up migration drops the backup table after the soak period.
--
-- Idempotent: a re-run matches zero rows (both output forms fail the
-- predicates) and ON CONFLICT keeps the original capture.
DO $$
DECLARE
  backed_up INT;
  bare_renamed INT;
  possessive_renamed INT;
BEGIN
  CREATE TABLE IF NOT EXISTS public._backup_teams_default_team_rename (
    id uuid PRIMARY KEY,
    old_name text NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now()
  );

  INSERT INTO public._backup_teams_default_team_rename (id, old_name)
  SELECT id, name
  FROM public.teams
  WHERE name = 'Default Team'
     OR name LIKE '%''s Default Team'
  ON CONFLICT (id) DO NOTHING;
  GET DIAGNOSTICS backed_up = ROW_COUNT;

  UPDATE public.teams
  SET name = 'Personal Project'
  WHERE name = 'Default Team';
  GET DIAGNOSTICS bare_renamed = ROW_COUNT;

  UPDATE public.teams
  SET name = regexp_replace(name, '''s Default Team$', '''s Project')
  WHERE name LIKE '%''s Default Team';
  GET DIAGNOSTICS possessive_renamed = ROW_COUNT;

  RAISE NOTICE 'rename_default_team_names_to_project: backed up %, bare renamed %, possessive renamed %',
    backed_up, bare_renamed, possessive_renamed;
END $$;
-- +goose StatementEnd

-- +goose Down
-- Intentional no-op: a pattern-based reverse would rewrite any team
-- legitimately named 'Personal Project' / '...''s Project' before this
-- migration ran. Exact restore uses the backup table (see Up comment);
-- catastrophic rollback is an AlloyDB point-in-time restore.
SELECT 1;
