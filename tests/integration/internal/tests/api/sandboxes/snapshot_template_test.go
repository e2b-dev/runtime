package sandboxes

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func createSnapshotTemplate(t *testing.T, c *api.ClientWithResponses, sbxId string, name *string) *api.PostSandboxesSandboxIDSnapshotsResponse {
	t.Helper()

	return createSnapshotTemplateWithBody(t, c, sbxId, api.PostSandboxesSandboxIDSnapshotsJSONRequestBody{Name: name})
}

func createSnapshotTemplateWithBody(t *testing.T, c *api.ClientWithResponses, sbxId string, body api.PostSandboxesSandboxIDSnapshotsJSONRequestBody) *api.PostSandboxesSandboxIDSnapshotsResponse {
	t.Helper()

	resp, err := c.PostSandboxesSandboxIDSnapshotsWithResponse(t.Context(), sbxId, body, setup.WithAPIKey())
	require.NoError(t, err)

	return resp
}

func createSnapshotTemplateWithCleanup(t *testing.T, c *api.ClientWithResponses, sbxId string, name *string) *api.SnapshotInfo {
	t.Helper()

	resp := createSnapshotTemplate(t, c, sbxId, name)
	require.Equal(t, http.StatusCreated, resp.StatusCode())
	require.NotNil(t, resp.JSON201)

	snapshot := resp.JSON201
	t.Cleanup(func() {
		c.DeleteTemplatesTemplateIDWithResponse(t.Context(), snapshot.SnapshotID, setup.WithAPIKey())
	})

	return snapshot
}

func TestSnapshotTemplateCreate(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("create snapshot without name", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, nil)

		assert.NotEmpty(t, snapshot.SnapshotID)
		// Without a name, the names list should be empty
		assert.Empty(t, snapshot.Names)
	})

	t.Run("create snapshot with name", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		name := "my-snapshot-" + sbx.SandboxID
		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, &name)

		assert.NotEmpty(t, snapshot.SnapshotID)
		assert.Contains(t, snapshot.SnapshotID, name, "snapshotID should contain the alias")
		assert.Contains(t, snapshot.SnapshotID, ":default", "snapshotID should contain the tag")
		require.NotEmpty(t, snapshot.Names)
		assert.Contains(t, snapshot.Names[0], name)
		assert.NotContains(t, snapshot.Names[0], ":default", "names should not include the tag suffix")

		// Creating again with the same name should reuse the same template
		resp2 := createSnapshotTemplate(t, c, sbx.SandboxID, &name)
		require.Equal(t, http.StatusCreated, resp2.StatusCode())
		require.NotNil(t, resp2.JSON201)
		assert.Equal(t, snapshot.SnapshotID, resp2.JSON201.SnapshotID, "Same name should return the same snapshot template ID")
		assert.Equal(t, snapshot.Names, resp2.JSON201.Names, "Same name should return the same names")
	})

	t.Run("create snapshot with name and tag", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		nameV1 := "tagged-snap-" + sbx.SandboxID + ":v1"
		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, &nameV1)
		require.NotEmpty(t, snapshot.Names)
		assert.NotContains(t, snapshot.Names[0], ":", "names should be tagless")

		// Same alias with different tag should reuse the template
		nameV2 := "tagged-snap-" + sbx.SandboxID + ":v2"
		resp2 := createSnapshotTemplate(t, c, sbx.SandboxID, &nameV2)
		require.Equal(t, http.StatusCreated, resp2.StatusCode())
		require.NotNil(t, resp2.JSON201)
		templateID1, _, _ := strings.Cut(snapshot.SnapshotID, ":")
		templateID2, _, _ := strings.Cut(resp2.JSON201.SnapshotID, ":")
		assert.Equal(t, templateID1, templateID2, "Same alias with different tag should reuse the same template")
		assert.Contains(t, snapshot.SnapshotID, ":v1")
		assert.Contains(t, resp2.JSON201.SnapshotID, ":v2")
		require.NotEmpty(t, resp2.JSON201.Names)
		assert.NotContains(t, resp2.JSON201.Names[0], ":", "names should be tagless")
	})

	t.Run("create snapshot for non-existent sandbox", func(t *testing.T) {
		t.Parallel()
		resp := createSnapshotTemplate(t, c, "nonexistentsandbox0", nil)
		require.Equal(t, http.StatusNotFound, resp.StatusCode())
	})
}

func TestSnapshotTemplateList(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("list snapshots", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, nil)

		listResp, err := c.GetSnapshotsWithResponse(t.Context(), &api.GetSnapshotsParams{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode())
		require.NotNil(t, listResp.JSON200)

		found := false
		for _, snap := range *listResp.JSON200 {
			if snap.SnapshotID == snapshot.SnapshotID {
				found = true

				break
			}
		}
		assert.True(t, found, "Created snapshot should appear in the list")
	})

	t.Run("list snapshots filtered by sandbox ID", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))
		sbxId := sbx.SandboxID

		createSnapshotTemplateWithCleanup(t, c, sbxId, nil)

		listResp, err := c.GetSnapshotsWithResponse(t.Context(), &api.GetSnapshotsParams{
			SandboxID: &sbxId,
		}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode())
		require.NotNil(t, listResp.JSON200)

		for _, snap := range *listResp.JSON200 {
			assert.NotEmpty(t, snap.SnapshotID)
		}
	})

	t.Run("list snapshots filtered by name", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		name := "list-by-name-" + sbx.SandboxID
		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, &name)

		listSnapshots := func(filter string) []api.SnapshotInfo {
			listResp, err := c.GetSnapshotsWithResponse(t.Context(), &api.GetSnapshotsParams{
				Name: &filter,
			}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, listResp.StatusCode())
			require.NotNil(t, listResp.JSON200)

			return *listResp.JSON200
		}

		snapshots := listSnapshots(name)
		require.Len(t, snapshots, 1)
		assert.Equal(t, snapshot.SnapshotID, snapshots[0].SnapshotID)

		// The namespaced form returned in Names should also match
		require.NotEmpty(t, snapshot.Names)
		snapshots = listSnapshots(snapshot.Names[0])
		require.Len(t, snapshots, 1)
		assert.Equal(t, snapshot.SnapshotID, snapshots[0].SnapshotID)

		// A name that doesn't resolve returns an empty list
		assert.Empty(t, listSnapshots("missing-"+sbx.SandboxID))

		// A tag that has no build on the snapshot returns an empty list
		assert.Empty(t, listSnapshots(name+":missing-tag"))

		// The default tag has a build, so it matches
		snapshots = listSnapshots(name + ":default")
		require.Len(t, snapshots, 1)
		assert.Equal(t, snapshot.SnapshotID, snapshots[0].SnapshotID)
	})

	t.Run("list snapshots filtered by older tag shows that tag", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		base := "list-by-tag-" + sbx.SandboxID
		snapDefault := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, &base)
		require.Contains(t, snapDefault.SnapshotID, ":default")

		nameV2 := base + ":v2"
		resp2 := createSnapshotTemplate(t, c, sbx.SandboxID, &nameV2)
		require.Equal(t, http.StatusCreated, resp2.StatusCode())
		require.NotNil(t, resp2.JSON201)

		listSnapshots := func(filter string) []api.SnapshotInfo {
			listResp, err := c.GetSnapshotsWithResponse(t.Context(), &api.GetSnapshotsParams{
				Name: &filter,
			}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, listResp.StatusCode())
			require.NotNil(t, listResp.JSON200)

			return *listResp.JSON200
		}

		// Filtering by an older tag (here the explicit default) must match and
		// show that tag's build, not the newest one
		snapshots := listSnapshots(base + ":default")
		require.Len(t, snapshots, 1)
		assert.Equal(t, snapDefault.SnapshotID, snapshots[0].SnapshotID)

		snapshots = listSnapshots(nameV2)
		require.Len(t, snapshots, 1)
		assert.Equal(t, resp2.JSON201.SnapshotID, snapshots[0].SnapshotID)

		// A bare name matches builds of any tag and shows the newest
		snapshots = listSnapshots(base)
		require.Len(t, snapshots, 1)
		assert.Equal(t, resp2.JSON201.SnapshotID, snapshots[0].SnapshotID)
	})

	t.Run("list unnamed snapshot filtered by its ID", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, nil)
		templateID, _, _ := strings.Cut(snapshot.SnapshotID, ":")

		listResp, err := c.GetSnapshotsWithResponse(t.Context(), &api.GetSnapshotsParams{
			Name: &templateID,
		}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode())
		require.NotNil(t, listResp.JSON200)
		require.Len(t, *listResp.JSON200, 1)
		assert.Equal(t, snapshot.SnapshotID, (*listResp.JSON200)[0].SnapshotID)
	})
}

func TestSnapshotTemplateDelete(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("delete unnamed snapshot by name", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, nil)

		deleteResp, err := c.DeleteTemplatesTemplateIDWithResponse(t.Context(), snapshot.SnapshotID, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, deleteResp.StatusCode())

		listResp, err := c.GetSnapshotsWithResponse(t.Context(), &api.GetSnapshotsParams{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode())

		for _, snap := range *listResp.JSON200 {
			assert.NotEqual(t, snapshot.SnapshotID, snap.SnapshotID, "Deleted snapshot should not appear in list")
		}
	})

	t.Run("delete named snapshot by name", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		name := "del-snap-" + sbx.SandboxID
		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, &name)

		require.NotEmpty(t, snapshot.Names)
		deleteResp, err := c.DeleteTemplatesTemplateIDWithResponse(t.Context(), snapshot.Names[0], setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, deleteResp.StatusCode())

		listResp, err := c.GetSnapshotsWithResponse(t.Context(), &api.GetSnapshotsParams{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode())

		for _, snap := range *listResp.JSON200 {
			assert.NotEqual(t, snapshot.SnapshotID, snap.SnapshotID, "Deleted snapshot should not appear in list")
		}
	})

	t.Run("delete non-existent snapshot", func(t *testing.T) {
		t.Parallel()
		deleteResp, err := c.DeleteTemplatesTemplateIDWithResponse(t.Context(), "non-existent-snapshot", setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, deleteResp.StatusCode())
	})
}

func TestSnapshotTemplateCreateSandbox(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("create sandbox from unnamed snapshot using name", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, nil)

		utils.AcquireSandboxSlot(t)
		createResp, err := c.PostSandboxesWithResponse(
			t.Context(),
			api.PostSandboxesJSONRequestBody{
				TemplateID: snapshot.SnapshotID,
			},
			setup.WithAPIKey(),
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, createResp.StatusCode())
		require.NotNil(t, createResp.JSON201)

		newSandbox := createResp.JSON201
		t.Cleanup(func() {
			c.DeleteSandboxesSandboxIDWithResponse(t.Context(), newSandbox.SandboxID, setup.WithAPIKey())
		})

		assert.NotEqual(t, sbx.SandboxID, newSandbox.SandboxID)
		snapshotTemplateID, _, _ := strings.Cut(snapshot.SnapshotID, ":")
		assert.Equal(t, snapshotTemplateID, newSandbox.TemplateID)
	})

	t.Run("create sandbox from named snapshot using name", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		name := "sbx-snap-" + sbx.SandboxID
		snapshot := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, &name)

		require.NotEmpty(t, snapshot.Names)
		utils.AcquireSandboxSlot(t)
		createResp, err := c.PostSandboxesWithResponse(
			t.Context(),
			api.PostSandboxesJSONRequestBody{
				TemplateID: snapshot.Names[0],
			},
			setup.WithAPIKey(),
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, createResp.StatusCode())
		require.NotNil(t, createResp.JSON201)

		newSandbox := createResp.JSON201
		t.Cleanup(func() {
			c.DeleteSandboxesSandboxIDWithResponse(t.Context(), newSandbox.SandboxID, setup.WithAPIKey())
		})

		assert.NotEqual(t, sbx.SandboxID, newSandbox.SandboxID)
		assert.NotEmpty(t, newSandbox.TemplateID)
	})

	t.Run("overwritten snapshot build is served immediately on sandbox create", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		envdClient := setup.GetEnvdClient(t, ctx)
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		name := "overwrite-snap-" + sbx.SandboxID

		// First snapshot — creates a new template with build B1 (no marker file)
		snap := createSnapshotTemplateWithCleanup(t, c, sbx.SandboxID, &name)
		require.NotEmpty(t, snap.Names)

		tagsResp1, err := c.GetTemplatesTemplateIDTagsWithResponse(ctx, snap.SnapshotID, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, tagsResp1.StatusCode())
		require.NotNil(t, tagsResp1.JSON200)
		require.NotEmpty(t, *tagsResp1.JSON200)
		build1ID := findDefaultTagBuildID(t, *tagsResp1.JSON200)

		// Write a marker file into the running sandbox so the next snapshot captures it
		err = utils.ExecCommand(t, ctx, sbx, envdClient, "/bin/sh", "-c", "echo snapshot-v2 > /tmp/snapshot-marker")
		require.NoError(t, err, "failed to write marker file")

		// Second snapshot with the same name — reuses the template, creates build B2 (has marker)
		snap2Resp := createSnapshotTemplate(t, c, sbx.SandboxID, &name)
		require.Equal(t, http.StatusCreated, snap2Resp.StatusCode())
		require.NotNil(t, snap2Resp.JSON201)
		assert.Equal(t, snap.SnapshotID, snap2Resp.JSON201.SnapshotID, "same template ID should be reused")

		// Verify the default tag now points to a different (newer) build
		tagsResp2, err := c.GetTemplatesTemplateIDTagsWithResponse(ctx, snap.SnapshotID, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, tagsResp2.StatusCode())
		require.NotNil(t, tagsResp2.JSON200)
		require.NotEmpty(t, *tagsResp2.JSON200)
		build2ID := findDefaultTagBuildID(t, *tagsResp2.JSON200)
		require.NotEqual(t, build1ID, build2ID, "second snapshot should produce a new build for the same tag")

		// Create a sandbox from the snapshot name — the template cache must have been
		// invalidated so the latest build (B2) is resolved, not the stale first one (B1).
		newSbx := utils.SetupSandboxWithCleanup(t, c,
			utils.WithTemplateID(snap.Names[0]),
			utils.WithAutoPause(false),
		)
		assert.NotEmpty(t, newSbx.TemplateID)

		// Read the marker file — it only exists in B2. If the cache served
		// stale B1, the file would be missing.
		output, err := utils.ExecCommandWithOutput(t, ctx, newSbx, envdClient, nil, "user", "cat", "/tmp/snapshot-marker")
		require.NoError(t, err, "marker file missing: sandbox was likely created from stale cached build")
		assert.Equal(t, "snapshot-v2\n", output)
	})
}

// A filesystem-only snapshot template keeps the source sandbox running
// without a reboot, persists the files written before it was taken, and
// cold-boots the sandboxes created from it.
func TestSnapshotTemplateFilesystemOnly(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	ctx := t.Context()
	envdClient := setup.GetEnvdClient(t, ctx)
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

	bootID := func(target *api.Sandbox) string {
		out, err := utils.ExecCommandWithOutput(t, ctx, target, envdClient, nil, "user", "cat", "/proc/sys/kernel/random/boot_id")
		require.NoError(t, err)
		require.NotEmpty(t, strings.TrimSpace(out))

		return out
	}
	sourceBoot := bootID(sbx)

	err := utils.ExecCommand(t, ctx, sbx, envdClient, "/bin/sh", "-c", "echo fs-only > /tmp/fs-only-marker")
	require.NoError(t, err)

	memory := false
	resp := createSnapshotTemplateWithBody(t, c, sbx.SandboxID, api.PostSandboxesSandboxIDSnapshotsJSONRequestBody{Memory: &memory})
	if resp.StatusCode() == http.StatusBadRequest && resp.JSON400 != nil && resp.JSON400.ErrorCode != nil && *resp.JSON400.ErrorCode == "snapshot_filesystem_only_disabled" {
		t.Skip("filesystem-only snapshots are disabled in this environment (filesystem-only-checkpoint flag)")
	}
	require.Equal(t, http.StatusCreated, resp.StatusCode())
	require.NotNil(t, resp.JSON201)
	snapshot := resp.JSON201
	t.Cleanup(func() {
		c.DeleteTemplatesTemplateIDWithResponse(t.Context(), snapshot.SnapshotID, setup.WithAPIKey())
	})

	// The source sandbox is still running in the same boot: the checkpoint was
	// taken in place, not by rebooting it.
	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode())
	require.NotNil(t, res.JSON200)
	assert.Equal(t, api.Running, res.JSON200.State)
	assert.Equal(t, sourceBoot, bootID(sbx), "the source sandbox must not reboot for a filesystem-only snapshot")

	// A sandbox created from the template has the files, in a fresh boot.
	newSbx := utils.SetupSandboxWithCleanup(t, c,
		utils.WithTemplateID(snapshot.SnapshotID),
		utils.WithAutoPause(false),
	)
	output, err := utils.ExecCommandWithOutput(t, ctx, newSbx, envdClient, nil, "user", "cat", "/tmp/fs-only-marker")
	require.NoError(t, err)
	assert.Equal(t, "fs-only\n", output)
	assert.NotEqual(t, sourceBoot, bootID(newSbx), "a sandbox created from a filesystem-only snapshot must cold-boot")
}

func findDefaultTagBuildID(t *testing.T, tags []api.TemplateTag) openapi_types.UUID {
	t.Helper()

	for _, tag := range tags {
		if tag.Tag == "default" {
			return tag.BuildID
		}
	}

	t.Fatal("no 'default' tag found in template tags")

	return openapi_types.UUID{}
}

// waitForSnapshotting polls the database until a build with status 'snapshotting'
// appears for the given sandbox, or the timeout expires.
func waitForSnapshotting(t *testing.T, db *setup.Database, sandboxID string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var count int

		err := db.Db.TestsRawSQLQuery(t.Context(),
			`SELECT COUNT(*) FROM env_builds eb
			 JOIN env_build_assignments eba ON eba.build_id = eb.id
			 JOIN snapshots s ON s.env_id = eba.env_id
			 WHERE s.sandbox_id = $1 AND eb.status = 'snapshotting'`,
			func(rows pgx.Rows) error {
				if rows.Next() {
					return rows.Scan(&count)
				}

				return nil
			},
			sandboxID,
		)
		require.NoError(t, err)

		if count > 0 {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for sandbox %s to enter snapshotting state", sandboxID)
}

// startSnapshotInBackground fires a snapshot creation request in a goroutine.
// Returns a channel that closes when the request completes.
func startSnapshotInBackground(t *testing.T, c *api.ClientWithResponses, sandboxID string) <-chan struct{} {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		resp, err := c.PostSandboxesSandboxIDSnapshotsWithResponse(
			t.Context(), sandboxID,
			api.PostSandboxesSandboxIDSnapshotsJSONRequestBody{},
			setup.WithAPIKey(),
		)
		if err != nil {
			t.Logf("snapshot request error: %v", err)

			return
		}

		t.Logf("snapshot response: %d", resp.StatusCode())

		if resp.StatusCode() == http.StatusCreated && resp.JSON201 != nil {
			t.Cleanup(func() {
				c.DeleteTemplatesTemplateIDWithResponse(t.Context(), resp.JSON201.SnapshotID, setup.WithAPIKey())
			})
		}
	}()

	return done
}

func TestSnapshotTemplateConcurrentOperations(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	t.Run("pause during snapshot creation", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshotDone := startSnapshotInBackground(t, c, sbx.SandboxID)
		waitForSnapshotting(t, db, sbx.SandboxID)

		pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, pauseResp.StatusCode(),
			"pause during snapshotting should wait and succeed, body: %s", string(pauseResp.Body))

		<-snapshotDone
	})

	t.Run("kill during snapshot creation", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshotDone := startSnapshotInBackground(t, c, sbx.SandboxID)
		waitForSnapshotting(t, db, sbx.SandboxID)

		killResp, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
		require.NoError(t, err)
		// Kill waits for the snapshot to complete, then proceeds normally.
		assert.Equal(t, http.StatusNoContent, killResp.StatusCode(),
			"kill during snapshotting should succeed after waiting, body: %s", string(killResp.Body))

		<-snapshotDone
	})

	t.Run("resume during snapshot creation", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshotDone := startSnapshotInBackground(t, c, sbx.SandboxID)
		waitForSnapshotting(t, db, sbx.SandboxID)

		resumeResp, err := c.PostSandboxesSandboxIDResumeWithResponse(
			t.Context(),
			sbx.SandboxID,
			api.PostSandboxesSandboxIDResumeJSONRequestBody{},
			setup.WithAPIKey(),
		)
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, resumeResp.StatusCode(),
			"resume during snapshotting should return conflict, body: %s", string(resumeResp.Body))

		<-snapshotDone
	})

	t.Run("concurrent snapshot returns conflict", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

		snapshotDone := startSnapshotInBackground(t, c, sbx.SandboxID)
		waitForSnapshotting(t, db, sbx.SandboxID)

		resp, err := c.PostSandboxesSandboxIDSnapshotsWithResponse(
			t.Context(),
			sbx.SandboxID,
			api.PostSandboxesSandboxIDSnapshotsJSONRequestBody{},
			setup.WithAPIKey(),
		)
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, resp.StatusCode(),
			"second snapshot during snapshotting should return conflict, body: %s", string(resp.Body))

		<-snapshotDone
	})
}
