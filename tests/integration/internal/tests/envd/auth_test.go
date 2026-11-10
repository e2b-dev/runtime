package envd

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/filesystem"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/envd"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func createSandbox(t *testing.T, reqEditors ...api.RequestEditorFn) *api.PostSandboxesResponse {
	t.Helper()

	utils.AcquireSandboxSlot(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()

	sbxTimeout := int32(10)
	resp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID,
		Timeout:    &sbxTimeout,
	}, reqEditors...)

	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode())

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Response: %s", string(resp.Body))
		}

		if resp.JSON201 != nil {
			utils.TeardownSandbox(t, setup.GetAPIClient(), resp.JSON201.SandboxID)
		}
	})

	return resp
}

func TestAccessToAuthorizedPathWithoutToken(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sbx := createSandbox(t, setup.WithAPIKey())
	require.NotNil(t, sbx.JSON201)
	require.NotNil(t, sbx.JSON201.EnvdAccessToken)

	envdClient := setup.GetEnvdClient(t, ctx)

	// set up the request to list the directory
	req := connect.NewRequest(&filesystem.ListDirRequest{Path: "/"})
	setup.SetInsecureSandboxHeader(t, req.Header(), sbx.JSON201.SandboxID)
	setup.SetUserHeader(t, req.Header(), "user")

	_, err := envdClient.FilesystemClient.ListDir(ctx, req)
	require.Error(t, err)

	assert.Equal(t, "unauthenticated: 401 Unauthorized", err.Error())
}

// TestInitIsNotReachableThroughTheSandboxURL pins envd's control plane shut from
// the outside. /init carries the orchestrator's env vars, access token and CA
// bundle, and is exempt from envd's own access-token check, so the sandbox proxy
// refuses it outright — including for the sandbox's own owner holding a valid
// token, who has no legitimate reason to drive envd's initialization.
//
// The orchestrator reaches /init at the sandbox slot IP, not through this URL, so
// nothing here constrains it.
func TestInitIsNotReachableThroughTheSandboxURL(t *testing.T) { //nolint:tparallel // the subtests must not queue; see the loop below
	t.Parallel()

	sbx := createSandbox(t, setup.WithAPIKey())
	require.NotNil(t, sbx.JSON201)
	require.NotNil(t, sbx.JSON201.EnvdAccessToken)

	envdClient := setup.GetEnvdClient(t, t.Context())
	wrongToken := "wrong-token"

	// Deliberately sequential. createSandbox gives the sandbox a 10s TTL and
	// nothing on the proxy path renews it, while a parallel subtest waits for a
	// slot behind every other parallel test in this package — long enough that the
	// sandbox is evicted before the request is sent, and the proxy answers "sandbox
	// not found" instead of refusing the route. Three sub-second HTTP calls against
	// one sandbox gain nothing from running in parallel.
	for _, tt := range []struct { //nolint:paralleltest // sequential by design, see above
		name  string
		token *string
	}{
		{name: "no token"},
		{name: "wrong token", token: &wrongToken},
		{name: "the sandbox's own token", token: sbx.JSON201.EnvdAccessToken},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			reqSetup := []envd.RequestEditorFn{setup.WithInsecureSandbox(t, sbx.JSON201.SandboxID)}
			if tt.token != nil {
				reqSetup = append(reqSetup, setup.WithEnvdAccessToken(t, *tt.token))
			}

			res, err := envdClient.HTTPClient.PostInitWithResponse(ctx, envd.PostInitJSONRequestBody{}, reqSetup...)
			require.NoError(t, err)

			require.Equal(t, http.StatusNotFound, res.StatusCode(), string(res.Body))
			// The body distinguishes the proxy's refusal from any 404 envd itself
			// might return, which is the whole claim: the request never arrives.
			assert.Contains(t, string(res.Body), "reserved for the E2B control plane", "the 404 should come from the proxy, not from envd")
		})
	}
}

func TestAccessAuthorizedPathWithResumedSandboxWithValidAccessToken(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sbx := createSandbox(t, setup.WithAPIKey())
	require.NotNil(t, sbx.JSON201)
	require.NotNil(t, sbx.JSON201.EnvdAccessToken)

	envdClient := setup.GetEnvdClient(t, ctx)
	sbxMeta := sbx.JSON201

	// set up the request to list the directory
	req := connect.NewRequest(&filesystem.ListDirRequest{Path: "/"})
	setup.SetSandboxHeader(t, req.Header(), sbxMeta)
	setup.SetUserHeader(t, req.Header(), "user")

	filePath := "demo.txt"
	fileContent := "Hello, world!"

	// create a test file
	utils.UploadFile(t, ctx, sbxMeta, envdClient, filePath, fileContent)

	c := setup.GetAPIClient()

	// stop sandbox
	_, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbxMeta.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

	sbxIdWithClient := sbxMeta.SandboxID + "-" + sbxMeta.ClientID

	// resume sandbox
	sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(ctx, sbxIdWithClient, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, http.StatusCreated, sbxResume.StatusCode())

	// try to get the file with the valid access token
	fileResponse, err := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &filePath, Username: new("user")},
		setup.WithSandbox(t, sbx.JSON201),
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, http.StatusOK, fileResponse.StatusCode())
	assert.Equal(t, fileContent, string(fileResponse.Body))
}

func TestAccessAuthorizedPathWithResumedSandboxWithoutAccessToken(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sbx := createSandbox(t, setup.WithAPIKey())
	require.NotNil(t, sbx.JSON201)
	require.NotNil(t, sbx.JSON201.EnvdAccessToken)

	envdClient := setup.GetEnvdClient(t, ctx)
	sbxMeta := sbx.JSON201

	// set up the request to list the directory
	req := connect.NewRequest(&filesystem.ListDirRequest{Path: "/"})
	setup.SetSandboxHeader(t, req.Header(), sbxMeta)
	setup.SetUserHeader(t, req.Header(), "user")

	filePath := "demo.txt"
	fileContent := "Hello, world!"

	// create a test file
	utils.UploadFile(t, ctx, sbxMeta, envdClient, filePath, fileContent)

	c := setup.GetAPIClient()

	// stop sandbox
	_, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbxMeta.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

	sbxIdWithClient := sbxMeta.SandboxID + "-" + sbxMeta.ClientID

	// resume sandbox
	sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(ctx, sbxIdWithClient, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, http.StatusCreated, sbxResume.StatusCode())

	fileResponse, err := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &filePath, Username: new("user")},
		setup.WithInsecureSandbox(t, sbx.JSON201.SandboxID),
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, http.StatusUnauthorized, fileResponse.StatusCode())
}
