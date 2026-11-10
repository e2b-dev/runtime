package orchestrator

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

func TestCreateSnapshotTemplate_NodeRefusalsFollowCommittedTemplateRows(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		code     codes.Code
		userCode orchestrator.UserErrorCode
		want     error
	}{
		"checkpoint in progress":   {codes.FailedPrecondition, orchestrator.UserErrorCode_CHECKPOINT_IN_PROGRESS, PauseRefusedCheckpointInProgressError{}},
		"guest agent unresponsive": {codes.ResourceExhausted, orchestrator.UserErrorCode_ENVD_UNRESPONSIVE, PauseRefusedEnvdUnresponsiveError{}},
		"node queue exhausted":     {codes.ResourceExhausted, orchestrator.UserErrorCode_UNKNOWN_USER_ERROR_CODE, PauseQueueExhaustedError{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			refusal, err := status.New(tc.code, "refused").WithDetails(&orchestrator.UserError{Code: tc.userCode, HttpStatus: 503})
			require.NoError(t, err)
			f := newKindFixture(t, refusal.Err())
			alias, namespace := "refused-snapshot", "refusal-team"

			_, err = f.o.CreateSnapshotTemplate(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, SnapshotTemplateOpts{
				Tag: id.DefaultTag, Alias: &alias, Namespace: &namespace,
			})
			require.ErrorIs(t, err, tc.want)

			var templates, failedBuilds, aliases int
			require.NoError(t, f.db.SqlcClient.TestsRawSQLQuery(t.Context(),
				`SELECT count(DISTINCT e.id),
				        count(DISTINCT b.id) FILTER (WHERE b.status_group = $2),
				        count(DISTINCT a.alias) FILTER (WHERE a.alias = $3 AND a.namespace = $4)
				 FROM public.snapshot_templates st
				 JOIN public.envs e ON e.id = st.env_id AND e.source = 'snapshot_template'
				 JOIN public.env_builds b ON b.id = st.build_id
				 LEFT JOIN public.env_aliases a ON a.env_id = e.id
				 WHERE st.sandbox_id = $1`,
				func(rows pgx.Rows) error {
					rows.Next()

					return rows.Scan(&templates, &failedBuilds, &aliases)
				},
				f.sbx.SandboxID, types.BuildStatusGroupFailed, alias, namespace,
			))
			assert.Equal(t, 1, templates, "the refused snapshot's template row is committed")
			assert.Equal(t, 1, failedBuilds, "its build is committed and marked failed")
			assert.Equal(t, 1, aliases, "the requested alias is committed")
		})
	}
}
