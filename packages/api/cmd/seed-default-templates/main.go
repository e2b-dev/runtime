// Seeds mock `e2b/*` public templates and marks them as env_defaults so the
// dashboard's default-template UI/logic can be exercised locally — without
// running real builds. Intended for the fake sandbox backend.
//
// Owner is a dedicated `e2b` team (NOT the local-dev team), so local-dev-team
// sees these as defaults (is_default=true), mirroring prod's `e2b/` templates.
//
// Each template gets a bare alias (name) AND an `e2b/`-namespaced alias, matching
// prod's public-template display (name + e2b/ alias on hover). Bare aliases use
// ON CONFLICT DO NOTHING so they never hijack an existing bare name (e.g. `base`).
//
// Per template it inserts: envs (public, source='template') + env_aliases (bare +
// namespaced) + env_builds (status_group='ready') +
// env_build_assignments (tag='default') + env_defaults. All idempotent; templates
// owned by the e2b team but no longer listed are reconciled away on re-run.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"

	authdb "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth"
)

// Dedicated owner team for public default templates (distinct from local-dev team).
var e2bTeamID = uuid.MustParse("e2b00000-0000-4000-8000-000000000001")

const e2bNamespace = "e2b"

type defaultTemplate struct {
	id          string
	aliases     []string // bare names; each gets a (alias,NULL) and a (alias,'e2b') row
	description string
	vcpu        int
	ramMB       int
	freeDiskMB  int
	totalDiskMB int
	spawnCount  int
}

var templates = []defaultTemplate{
	{"e2b-code-interpreter", []string{"code-interpreter"}, "Python data-analysis sandbox", 2, 1024, 1024, 2048, 42},
	{"e2b-desktop", []string{"desktop"}, "Desktop environment sandbox", 4, 4096, 2048, 8192, 7},
	{"e2b-python", []string{"python"}, "Python sandbox", 2, 512, 512, 1024, 13},
}

const (
	kernelVersion      = "vmlinux-6.1.158"
	firecrackerVersion = "v1.12.1"
	envdVersion        = "0.6.1"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "seed-default-templates: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Seeded %d default templates (owner team %s).\n", len(templates), e2bTeamID)
}

func run(ctx context.Context) error {
	connectionString := os.Getenv("POSTGRES_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "postgresql://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
	}

	db, err := authdb.NewClient(ctx, connectionString)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.teams (id, email, name, is_blocked, slug)
VALUES ($1, $2, $3, FALSE, $4)
ON CONFLICT (id) DO UPDATE SET slug = EXCLUDED.slug, name = EXCLUDED.name;
`, e2bTeamID, "defaults@e2b-dev.local", "E2B", e2bNamespace); err != nil {
		return fmt.Errorf("upsert e2b owner team: %w", err)
	}

	// Reconcile: drop e2b-owned templates no longer in the list. Scoped strictly
	// by team_id, so it can't touch the real base or user templates. env_defaults
	// must go first (FK to envs has no cascade); envs delete cascades the rest.
	wanted := make([]string, len(templates))
	for i, t := range templates {
		wanted[i] = t.id
	}
	if err := db.TestsRawSQL(ctx, `
DELETE FROM public.env_defaults
WHERE env_id IN (SELECT id FROM public.envs WHERE team_id = $1) AND env_id <> ALL($2);
`, e2bTeamID, wanted); err != nil {
		return fmt.Errorf("reconcile env_defaults: %w", err)
	}
	if err := db.TestsRawSQL(ctx, `
DELETE FROM public.envs WHERE team_id = $1 AND id <> ALL($2);
`, e2bTeamID, wanted); err != nil {
		return fmt.Errorf("reconcile envs: %w", err)
	}

	for _, t := range templates {
		if err := seedTemplate(ctx, db, t); err != nil {
			return fmt.Errorf("seed %q: %w", t.id, err)
		}
	}

	return nil
}

func seedTemplate(ctx context.Context, db *authdb.Client, t defaultTemplate) error {
	// 1. envs row (public template owned by the e2b team).
	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.envs (id, team_id, public, updated_at, source, spawn_count)
VALUES ($1, $2, TRUE, NOW(), 'template', $3)
ON CONFLICT (id) DO UPDATE SET updated_at = NOW(), spawn_count = EXCLUDED.spawn_count;
`, t.id, e2bTeamID, t.spawnCount); err != nil {
		return fmt.Errorf("upsert envs: %w", err)
	}

	// 2. Aliases: a namespaced `e2b/<alias>` row plus a bare row (the displayed name).
	for _, alias := range t.aliases {
		// Namespaced: we own the `e2b` namespace, so reassign env_id on conflict.
		if err := db.TestsRawSQL(ctx, `
INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace)
VALUES ($1, $2, FALSE, $3)
ON CONFLICT (alias, namespace) DO UPDATE SET env_id = EXCLUDED.env_id;
`, alias, t.id, e2bNamespace); err != nil {
			return fmt.Errorf("upsert namespaced alias %q: %w", alias, err)
		}

		// Bare (namespace NULL): DO NOTHING so we never hijack an existing bare
		// name owned by another template (e.g. the real `base`).
		if err := db.TestsRawSQL(ctx, `
INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace)
VALUES ($1, $2, FALSE, NULL)
ON CONFLICT (alias, namespace) DO NOTHING;
`, alias, t.id); err != nil {
			return fmt.Errorf("insert bare alias %q: %w", alias, err)
		}
	}

	// 3. Fresh env_builds row. status_group='ready' satisfies BOTH
	// default-listing queries. Wipe prior fake builds first (assignments
	// cascade on delete).
	if err := db.TestsRawSQL(ctx, `DELETE FROM public.env_builds WHERE env_id = $1;`, t.id); err != nil {
		return fmt.Errorf("clear env_builds: %w", err)
	}

	buildID := uuid.New()
	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.env_builds (
    id, env_id, team_id, updated_at, status_group,
    ram_mb, vcpu, kernel_version, firecracker_version,
    free_disk_size_mb, total_disk_size_mb, envd_version, finished_at
) VALUES ($1, $2, (SELECT team_id FROM public.envs WHERE id = $2), NOW(), 'ready', $3, $4, $5, $6, $7, $8, $9, NOW());
`, buildID, t.id, t.ramMB, t.vcpu, kernelVersion, firecrackerVersion,
		t.freeDiskMB, t.totalDiskMB, envdVersion); err != nil {
		return fmt.Errorf("insert env_builds: %w", err)
	}

	// 4. Explicit 'default'/'app' assignment (the sync trigger also adds a
	// 'trigger' one on insert; this mirrors production code).
	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.env_build_assignments (env_id, build_id, tag, source)
VALUES ($1, $2, 'default', 'app');
`, t.id, buildID); err != nil {
		return fmt.Errorf("insert env_build_assignments: %w", err)
	}

	// 5. Mark as default.
	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.env_defaults (env_id, description)
VALUES ($1, $2)
ON CONFLICT (env_id) DO UPDATE SET description = EXCLUDED.description;
`, t.id, t.description); err != nil {
		return fmt.Errorf("insert env_defaults: %w", err)
	}

	return nil
}
