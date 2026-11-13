package api_templates

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	testutils "github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestTemplateBuildWhiteoutImage(t *testing.T) {
	t.Parallel()

	const name = "test-oci-whiteout-image"
	const image = "swerebench/sweb.eval.x86_64.astanin_1776_python-tabulate-76@sha256:ae96cbc67f928c4b504a0620cfe6f71e6474343de52059b59b813237b1135006"
	c := setup.GetAPIClient()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		resp, err := c.DeleteTemplatesTemplateIDWithResponse(ctx, name, setup.WithAPIKey())
		if assert.NoError(t, err) {
			assert.Contains(t, []int{http.StatusNoContent, http.StatusNotFound}, resp.StatusCode())
		}
	})

	extracted := false
	started := time.Now()
	template := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name:    name,
		Timeout: 10 * time.Minute,
		BuildData: api.TemplateBuildStartV2{
			Force:     new(true),
			FromImage: new(image),
			Steps: new([]api.TemplateStep{{
				Type: "RUN",
				Args: new([]string{"test -x /usr/bin/mount && test ! -e /usr/share/doc/mount/examples/fstab && printf 'whiteout-live-ok\\n' > /whiteout-regression", "root"}),
			}}),
		},
		LogHandler: func(alias string, entry api.BuildLogEntry) {
			extracted = extracted || strings.Contains(entry.Message, "Uncompressing layer")
			testutils.DefaultBuildLogHandler(t)(alias, entry)
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})
	require.True(t, extracted, "must exercise image extraction rather than a cached base")
	t.Logf("Uncached whiteout image build completed in %s; template=%s build=%s", time.Since(started), template.TemplateID, template.BuildID)

	sbx := testutils.SetupSandboxWithCleanup(t, c, testutils.WithTemplateID(template.TemplateID))
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	output, err := testutils.ExecCommandAsRootWithOutput(t, ctx, sbx, setup.GetEnvdClient(t, ctx), "/bin/sh", "-c", "test -x /usr/bin/mount && test ! -e /usr/share/doc/mount/examples/fstab && cat /whiteout-regression")
	require.NoError(t, err)
	assert.Equal(t, "whiteout-live-ok\n", output)
}
