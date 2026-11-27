// Seeds a fake `base` template directly into Postgres so `e2b sbx create`
// works without running the real template-manager build pipeline.
//
// Intended for use with the fake sandbox backend (which does not
// implement template builds). For the production-equivalent flow, use
// `make -C packages/shared/scripts local-build-base-template` instead.
//
// What it inserts:
//   - public.envs(id='base', team_id=<seed team>, source='template')
//   - public.env_aliases(alias='base', env_id='base')
//   - public.env_builds(env_id='base', team_id=<seed team>, status_group='ready')
//   - public.env_build_assignments(env_id='base', tag=<build tag>)
//
// All inserts are idempotent: re-running the script is safe.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"

	authdb "github.com/e2b-dev/infra/packages/api/internal/db/pkg/auth"
)

// Must match the team seeded by cmd/seed-local.
var seedTeamID = uuid.MustParse("0b8a3ded-4489-4722-afd1-1d82e64ec2d5")

const (
	templateID = "base"
	buildTag   = "default"

	// Mirrors shared/pkg/featureflags defaults; values are cosmetic since the
	// fake orchestrator does not actually boot a VM.
	kernelVersion      = "vmlinux-6.1.158"
	firecrackerVersion = "v1.12.1"
	envdVersion        = "0.6.1"

	vcpu        = 2
	ramMB       = 512
	freeDiskMB  = 512
	totalDiskMB = 1024
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "seed-base-template: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Seeded template %q (team %s).\n", templateID, seedTeamID)
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
	defer db.Close()

	// 1. envs row. ON CONFLICT bumps updated_at so re-runs are observable.
	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.envs (id, team_id, public, updated_at, source)
VALUES ($1, $2, FALSE, NOW(), 'template')
ON CONFLICT (id) DO UPDATE SET updated_at = NOW();
`, templateID, seedTeamID); err != nil {
		return fmt.Errorf("upsert envs row: %w", err)
	}

	// 2. env_aliases row so the API can resolve "base" -> templateID.
	if err := db.TestsRawSQL(ctx, `
-- env_aliases is uniquely indexed on (alias, namespace) NULLS NOT DISTINCT,
-- so the ON CONFLICT target must list both columns.
INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace)
VALUES ($1, $2, FALSE, NULL)
ON CONFLICT (alias, namespace) DO UPDATE SET env_id = EXCLUDED.env_id;
`, templateID, templateID); err != nil {
		return fmt.Errorf("upsert env_aliases row: %w", err)
	}

	// 3. env_builds row with status_group='ready'. We need a *new* row each time
	// (because env_build_assignments uses build_id as part of its identity
	// and old assignments stay valid), but we only want one *ready* build
	// per template — so first wipe prior fake builds for this template.
	//
	// We scope the delete to this template only; env_build_assignments and
	// active_template_builds cascade on env_builds delete.
	if err := db.TestsRawSQL(ctx, `
DELETE FROM public.env_builds
WHERE env_id = $1;
`, templateID); err != nil {
		return fmt.Errorf("clear prior env_builds: %w", err)
	}

	buildID := uuid.New()
	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.env_builds (
    id,
    env_id,
    team_id,
    updated_at,
    status_group,
    ram_mb,
    vcpu,
    kernel_version,
    firecracker_version,
    free_disk_size_mb,
    total_disk_size_mb,
    envd_version,
    finished_at
) VALUES (
    $1, $2, (SELECT team_id FROM public.envs WHERE id = $2), NOW(), 'ready',
    $3, $4,
    $5, $6,
    $7, $8,
    $9, NOW()
);
`,
		buildID, templateID,
		ramMB, vcpu,
		kernelVersion, firecrackerVersion,
		freeDiskMB, totalDiskMB,
		envdVersion,
	); err != nil {
		return fmt.Errorf("insert env_builds row: %w", err)
	}

	// 4. env_build_assignments row. The sync_env_build_assignment trigger
	// inserts a ('default','trigger') row automatically on env_builds insert
	// above, but only if env_id is set on the row. Belt-and-suspenders: also
	// upsert an 'app'-source assignment which is what production code does.
	if err := db.TestsRawSQL(ctx, `
INSERT INTO public.env_build_assignments (env_id, build_id, tag, source)
VALUES ($1, $2, $3, 'app');
`, templateID, buildID, buildTag); err != nil {
		return fmt.Errorf("insert env_build_assignments row: %w", err)
	}

	return nil
}
