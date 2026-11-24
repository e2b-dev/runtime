package aliases

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

func TestAdminResolveTemplateAliases_ReturnsNakedAndNamespacedHolders(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	published := testutils.CreateTestTemplate(t, db, teamID)
	other := testutils.CreateTestTemplate(t, db, teamID)
	deleted := testutils.CreateTestTemplate(t, db, teamID)
	testutils.CreateTestTemplateAliasWithName(t, db, published, "base", new("vendor"))
	testutils.CreateTestTemplateAliasWithName(t, db, other, "base", nil)
	testutils.CreateTestTemplateAliasWithName(t, db, other, "base", new("someone-else"))
	testutils.CreateTestTemplateAliasWithName(t, db, deleted, "gone", new("vendor"))
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`, deleted))

	rows, err := db.SqlcClient.AdminResolveTemplateAliases(ctx, queries.AdminResolveTemplateAliasesParams{
		Names:     []string{"base", "gone", "missing"},
		Namespace: "vendor",
	})
	require.NoError(t, err)

	got := map[string]string{}
	for _, row := range rows {
		namespace := "<naked>"
		if row.Namespace != nil {
			namespace = *row.Namespace
		}
		got[row.Alias+"@"+namespace] = row.TemplateID
	}
	assert.Equal(t, map[string]string{
		"base@vendor":  published,
		"base@<naked>": other,
	}, got, "other namespaces and deleted templates are left out")
}

// The resolve query must reach env_aliases through the unique (alias,
// namespace) index and envs through an index on id, never a sequential scan,
// on a table large enough that the planner would prefer one if it could.
func TestAdminResolveTemplateAliases_UsesIndexes(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `
		INSERT INTO public.envs (id, team_id, public, updated_at, source)
		SELECT 'plan-env-' || i, $1::uuid, true, NOW(), 'template'
		FROM generate_series(1, 20000) AS i`, teamID))
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `
		INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace)
		SELECT 'name-' || (i % 10000), 'plan-env-' || i, true,
		       CASE WHEN i <= 10000 THEN 'ns-' || (i % 50) ELSE NULL END
		FROM generate_series(1, 20000) AS i`))
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `ANALYZE public.envs, public.env_aliases`))

	conn, err := pgx.Connect(ctx, db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close(context.WithoutCancel(ctx))) })

	names := make([]string, 0, 40)
	for i := range 40 {
		names = append(names, fmt.Sprintf("name-%d", i*7))
	}

	q := queries.New(&resolvePlan{DBTX: conn, t: t})
	_, err = q.AdminResolveTemplateAliases(ctx, queries.AdminResolveTemplateAliasesParams{
		Names:     names,
		Namespace: "ns-1",
	})
	require.NoError(t, err)
}

type resolvePlan struct {
	queries.DBTX

	t *testing.T
}

func (p *resolvePlan) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.t.Helper()

	var raw []byte
	require.NoError(p.t, p.DBTX.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&raw))
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	require.NoError(p.t, json.Unmarshal(raw, &plans))
	require.Len(p.t, plans, 1)

	var indexes []string
	envsByIndex := false
	var walk func(node planNode)
	walk = func(node planNode) {
		assert.NotEqual(p.t, "Seq Scan", node.NodeType, "%s is read by a sequential scan:\n%s", node.RelationName, raw)
		if node.IndexName != "" {
			indexes = append(indexes, node.IndexName)
			envsByIndex = envsByIndex || node.RelationName == "envs"
		}
		for _, child := range node.Plans {
			walk(child)
		}
	}
	walk(plans[0].Plan)

	assert.Contains(p.t, indexes, "idx_env_aliases_alias_namespace_unique", "plan:\n%s", raw)
	assert.True(p.t, envsByIndex, "envs is looked up by an index on id, plan:\n%s", raw)

	return p.DBTX.Query(ctx, sql, args...)
}

type planNode struct {
	NodeType     string     `json:"Node Type"`
	RelationName string     `json:"Relation Name"`
	IndexName    string     `json:"Index Name"`
	Plans        []planNode `json:"Plans"`
}
