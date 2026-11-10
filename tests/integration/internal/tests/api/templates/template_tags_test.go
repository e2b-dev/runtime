package api_templates

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/envd"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	testutils "github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestTemplateTagAssign(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Build a template to work with
	template := testutils.BuildSimpleTemplate(t, "test-tag-assign", setup.WithAPIKey())

	// Assign a custom tag to the template
	// POST /templates/tags with body { target: source, names: [targets] }
	tagResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: template.TemplateID + ":" + id.DefaultTag,
		Tags:   []string{"v1"},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Tag response: %s", string(tagResp.Body))
		}
	})

	assert.Equal(t, http.StatusCreated, tagResp.StatusCode())
	require.NotNil(t, tagResp.JSON201)
	assert.Equal(t, []string{"v1"}, tagResp.JSON201.Tags)
	assert.Equal(t, template.BuildID, tagResp.JSON201.BuildID.String())
}

func TestTemplateTagAssignFromSourceTag(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Build a template to work with
	template := testutils.BuildSimpleTemplate(t, "test-tag-source", setup.WithAPIKey())

	// First assign a source tag (from the default build)
	sourceTag := "staging"
	stagingResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: template.TemplateID + ":" + id.DefaultTag,
		Tags:   []string{sourceTag},
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, stagingResp.StatusCode())

	// Assign a new tag from the source tag (staging)
	prodTag := "production"
	tagResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: template.TemplateID + ":" + sourceTag,
		Tags:   []string{prodTag},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	assert.Equal(t, http.StatusCreated, tagResp.StatusCode())
	require.NotNil(t, tagResp.JSON201)
	assert.Equal(t, []string{prodTag}, tagResp.JSON201.Tags)
	// Both tags should point to the same build
	assert.Equal(t, template.BuildID, tagResp.JSON201.BuildID.String())
}

func TestTemplateTagDeleteLatestNotAllowed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// The default-tag deletion guard fires before any build lookup, so a
	// template without a build is enough for this test.
	template := testutils.RequestTemplateWithoutBuild(t, "test-tag-delete-latest", setup.WithAPIKey())

	// Try to delete the 'default' tag - should fail
	deleteResp, err := c.DeleteTemplatesTagsWithResponse(ctx, api.DeleteTemplateTagsRequest{
		Name: template.TemplateID,
		Tags: []string{id.DefaultTag},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, deleteResp.StatusCode())
}

func TestSandboxCreateWithTag(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Build a template
	template := testutils.BuildSimpleTemplate(t, "test-sbx-tag", setup.WithAPIKey())

	// Assign a version tag
	_, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: template.TemplateID + ":" + id.DefaultTag,
		Tags:   []string{"v1.0"},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	// Create a sandbox using the tagged template (templateID:tag format)
	testutils.AcquireSandboxSlot(t)
	sbxTimeout := int32(60)
	resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: template.TemplateID + ":v1.0",
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(resp.Body))
		}

		if resp.JSON201 != nil {
			testutils.TeardownSandbox(t, c, resp.JSON201.SandboxID)
		}
	})

	assert.Equal(t, http.StatusCreated, resp.StatusCode())
	require.NotNil(t, resp.JSON201)
}

func TestSandboxCreateWithDefaultTag(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Create a sandbox using explicit :default tag - should work same as without tag
	testutils.AcquireSandboxSlot(t)
	sbxTimeout := int32(60)
	resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID + ":" + id.DefaultTag,
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(resp.Body))
		}

		if resp.JSON201 != nil {
			testutils.TeardownSandbox(t, c, resp.JSON201.SandboxID)
		}
	})

	assert.Equal(t, http.StatusCreated, resp.StatusCode())
}

func TestSandboxCreateWithNonExistentTag(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Try to create a sandbox with a non-existent tag
	testutils.AcquireSandboxSlot(t)
	sbxTimeout := int32(60)
	resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID + ":nonexistent-tag",
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, resp.StatusCode())
}

func TestSandboxCreateWithAliasAndTag(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Build a template with a specific alias
	alias := "test-alias-tag"
	testutils.BuildSimpleTemplate(t, alias, setup.WithAPIKey())

	// Assign a version tag using the alias
	tagResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: alias + ":" + id.DefaultTag,
		Tags:   []string{"stable"},
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, tagResp.StatusCode())

	// Create a sandbox using alias:tag format (API should resolve alias to templateID)
	testutils.AcquireSandboxSlot(t)
	sbxTimeout := int32(60)
	resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: alias + ":stable",
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(resp.Body))
		}

		if resp.JSON201 != nil {
			testutils.TeardownSandbox(t, c, resp.JSON201.SandboxID)
		}
	})

	assert.Equal(t, http.StatusCreated, resp.StatusCode())
	require.NotNil(t, resp.JSON201)
}

func TestTemplateTagNotFoundForNonExistentTemplate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	// Try to assign a tag to a non-existent template
	tagResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: "nonexistent-template-id:" + id.DefaultTag,
		Tags:   []string{"v1"},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	assert.Equal(t, http.StatusNotFound, tagResp.StatusCode())
}

func TestTemplateTagInvalidTagName(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	// Try to assign a tag with empty name
	tagResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: setup.SandboxTemplateID + ":" + id.DefaultTag,
		Tags:   []string{""},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, tagResp.StatusCode())
}

func TestMultipleTagsOnSameTemplate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Build a template
	template := testutils.BuildSimpleTemplate(t, "test-multi-tags", setup.WithAPIKey())

	// Assign multiple tags to the same build
	tags := []string{"dev", "staging", "prod", "v1.0.0"}
	for _, tag := range tags {
		tagResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
			Target: template.TemplateID + ":" + id.DefaultTag,
			Tags:   []string{tag},
		}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, tagResp.StatusCode())
		assert.Equal(t, template.BuildID, tagResp.JSON201.BuildID.String())
	}

	// Verify we can create sandboxes with each tag
	release := testutils.AcquireSandboxSlot(t)
	for _, tag := range tags {
		sbxTimeout := int32(60)
		resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
			TemplateID: template.TemplateID + ":" + tag,
			Timeout:    &sbxTimeout,
		}, setup.WithAPIKey())
		require.NoError(t, err)

		if resp.JSON201 != nil {
			testutils.TeardownSandbox(t, c, resp.JSON201.SandboxID)
		}

		assert.Equal(t, http.StatusCreated, resp.StatusCode(), "Failed to create sandbox with tag: %s", tag)
	}
	release()
}

func TestTagReassignment(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	alias := "test-tag-reassign"

	// Build first version of template
	template1 := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: alias,
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{
					Type:  "ENV",
					Force: new(true),
					Args:  new([]string{"VERSION", "1"}),
				},
			}),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	// Assign 'stable' tag to first build using the alias
	stableResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: alias + ":" + id.DefaultTag,
		Tags:   []string{"stable"},
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, stableResp.StatusCode())

	// Build second version (will use the same template since same alias)
	template2 := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: alias,
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{
					Type:  "ENV",
					Force: new(true),
					Args:  new([]string{"VERSION", "2"}),
				},
			}),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	// Both builds should have the same template ID since they use the same alias
	require.Equal(t, template1.TemplateID, template2.TemplateID, "Both builds should be for the same template")

	// Reassign 'stable' tag to second build (by using 'default' as source)
	tagResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: alias + ":" + id.DefaultTag,
		Tags:   []string{"stable"},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	assert.Equal(t, http.StatusCreated, tagResp.StatusCode())
	require.NotNil(t, tagResp.JSON201)
	assert.Equal(t, template2.BuildID, tagResp.JSON201.BuildID.String(), "stable tag should now point to second build")
}

func TestTemplateBuildWithTags(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Build a template with tags specified during creation
	name := "test-build-with-tags"
	tags := []string{"v1.0.0", "stable"}
	testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: name,
		Tags: new(tags),
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	// Verify we can create sandboxes with each tag that was specified during creation
	release := testutils.AcquireSandboxSlot(t)
	for _, tag := range tags {
		sbxTimeout := int32(60)
		sbxResp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
			TemplateID: name + ":" + tag,
			Timeout:    &sbxTimeout,
		}, setup.WithAPIKey())
		require.NoError(t, err)

		if sbxResp.JSON201 != nil {
			testutils.TeardownSandbox(t, c, sbxResp.JSON201.SandboxID)
		}

		assert.Equal(t, http.StatusCreated, sbxResp.StatusCode(), "Failed to create sandbox with tag: %s", name)
	}
	release()
}

func TestTemplateBuildWithTagsAndSandboxCreation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	name := "test-prod-tag:production"

	// Build a template with 'production' tag specified during creation
	testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: name,
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	// Create sandbox using alias:tag format
	testutils.AcquireSandboxSlot(t)
	sbxTimeout := int32(60)
	sbxResp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: name,
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(sbxResp.Body))
		}

		if sbxResp.JSON201 != nil {
			testutils.TeardownSandbox(t, c, sbxResp.JSON201.SandboxID)
		}
	})

	assert.Equal(t, http.StatusCreated, sbxResp.StatusCode())
	require.NotNil(t, sbxResp.JSON201)
}

func TestTemplateBuildWithTagInAlias(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	// Build a template with tag in alias (e.g., "my-alias:my-tag")
	// Without providing explicit tags in the request body
	// The tag from the alias should be used automatically
	template := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: "test-alias-with-tag:v2.0",
		// Tags: intentionally not provided - should be inferred from alias
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	// Create sandbox using the tag that was embedded in the alias
	testutils.AcquireSandboxSlot(t)
	sbxTimeout := int32(60)
	sbxResp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: template.TemplateID + ":v2.0",
		Timeout:    &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(sbxResp.Body))
		}

		if sbxResp.JSON201 != nil {
			testutils.TeardownSandbox(t, c, sbxResp.JSON201.SandboxID)
		}
	})

	assert.Equal(t, http.StatusCreated, sbxResp.StatusCode())
	require.NotNil(t, sbxResp.JSON201)
}

// TestAssignmentOrderingLatestWins verifies that when multiple builds are assigned
// to the same tag, the latest assignment (by created_at DESC) is used for sandbox creation.
// This is a critical behavior for the tag reassignment feature.
func TestAssignmentOrderingLatestWins(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	alias := "test-ordering-latest"
	versionFilePath := "/home/user/version.txt"

	// Build first version - write "build-1" to a file
	template1 := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: alias,
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{
					Type:  "RUN",
					Force: new(true),
					Args:  new([]string{"echo -n 'build-1' > " + versionFilePath}),
				},
			}),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	// Build second version (same template, new build) - write "build-2" to a file
	template2 := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: alias,
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{
					Type:  "RUN",
					Force: new(true),
					Args:  new([]string{"echo -n 'build-2' > " + versionFilePath}),
				},
			}),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	require.Equal(t, template1.TemplateID, template2.TemplateID, "Same alias should produce same template ID")
	require.NotEqual(t, template1.BuildID, template2.BuildID, "Each build should have unique build ID")

	// The default tag should now point to the latest build (template2)
	// Create a sandbox and verify it uses the latest build
	sbx := testutils.SetupSandboxWithCleanup(t, c, testutils.WithTemplateID(alias+":"+id.DefaultTag))

	// Read the version file from the sandbox to verify it's using the latest build
	envdClient := setup.GetEnvdClient(t, ctx)
	fileResp, err := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &versionFilePath, Username: new("user")},
		setup.WithSandbox(t, sbx),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, fileResp.StatusCode(), "Failed to read version file")

	// The file should contain "build-2" since the latest build should be used
	assert.Equal(t, "build-2", string(fileResp.Body),
		"Sandbox should use the latest build (build-2)")
}

// TestAssignmentOrderingAfterTagReassignment verifies that after reassigning a tag
// to a different build, sandbox creation uses the newly assigned build.
func TestAssignmentOrderingAfterTagReassignment(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()

	c := setup.GetAPIClient()

	alias := "test-ordering-reassign"
	versionFilePath := "/home/user/version.txt"

	// Build two versions - each writes a different version to a file
	template1 := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: alias,
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{Type: "RUN", Force: new(true), Args: new([]string{"echo -n 'version-1' > " + versionFilePath})},
			}),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	template2 := testutils.BuildTemplate(t, testutils.TemplateBuildOptions{
		Name: alias,
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{Type: "RUN", Force: new(true), Args: new([]string{"echo -n 'version-2' > " + versionFilePath})},
			}),
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	// Create a 'stable' tag pointing to the first build
	_, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: template1.TemplateID + ":" + template1.BuildID,
		Tags:   []string{"stable"},
	}, setup.WithAPIKey())
	require.NoError(t, err)

	// Reassign 'stable' tag to the second build
	reassignResp, err := c.PostTemplatesTagsWithResponse(ctx, api.AssignTemplateTagsRequest{
		Target: template2.TemplateID + ":" + template2.BuildID,
		Tags:   []string{"stable"},
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, reassignResp.StatusCode())

	// Create sandbox with 'stable' tag and verify it uses the latest assignment
	sbx := testutils.SetupSandboxWithCleanup(t, c, testutils.WithTemplateID(alias+":stable"))

	// Read the version file from the sandbox to verify it's using the reassigned build
	envdClient := setup.GetEnvdClient(t, ctx)
	fileResp, err := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &versionFilePath, Username: new("user")},
		setup.WithSandbox(t, sbx),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, fileResp.StatusCode(), "Failed to read version file")

	// The file should contain "version-2" since the stable tag was reassigned to the second build
	assert.Equal(t, "version-2", string(fileResp.Body),
		"Sandbox should use the reassigned build (version-2)")
}
