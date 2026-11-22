package sandboxes

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func pauseSandbox(t *testing.T, c *api.ClientWithResponses, sandboxID string) {
	t.Helper()

	pauseSandboxResponse, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())

	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, pauseSandboxResponse.StatusCode())
}

func TestSandboxList(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create a sandbox for testing
	sbx := utils.SetupSandboxWithCleanup(t, c)

	// Test basic list functionality
	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())

	assert.GreaterOrEqual(t, len(*listResponse.JSON200), 1)

	// Verify our sandbox is in the list
	found := false
	for _, s := range *listResponse.JSON200 {
		if s.SandboxID == sbx.SandboxID {
			found = true

			break
		}
	}
	assert.True(t, found)
}

func TestSandboxListWithFilter(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// standard sandbox
	_ = utils.SetupSandboxWithCleanup(t, c)

	metadataKey := "favouriteColor"
	metadataValue := "blue"
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	// sandbox with custom metadata
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))

	// List with filter
	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Metadata: &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	assert.Len(t, *listResponse.JSON200, 1)
	assert.Equal(t, sbx.SandboxID, (*listResponse.JSON200)[0].SandboxID)
}

func TestSandboxListWithTemplateAndStartedAfter(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	state := []api.SandboxState{api.Running}
	detailResponse, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, detailResponse.StatusCode())
	require.NotNil(t, detailResponse.JSON200)

	startedBeforeSandbox := detailResponse.JSON200.StartedAt.Add(-time.Second)
	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		State:        &state,
		Metadata:     &metadataString,
		Template:     &sbx.TemplateID,
		StartedAfter: &startedBeforeSandbox,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	require.Len(t, *listResponse.JSON200, 1)
	assert.Equal(t, sbx.SandboxID, (*listResponse.JSON200)[0].SandboxID)

	startedAfterSandbox := detailResponse.JSON200.StartedAt.Add(time.Second)
	listResponse, err = c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		State:        &state,
		Metadata:     &metadataString,
		Template:     &sbx.TemplateID,
		StartedAfter: &startedAfterSandbox,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	assert.Empty(t, *listResponse.JSON200)

	// The bound is documented as inclusive, so a sandbox's own startedAt -- handed
	// back verbatim at the full precision the API reports it in, which is the
	// documented use of an at-or-after bound -- must still return it. The API
	// truncates the bound onto the microsecond grid its keyset values sit on;
	// without that, the sub-microsecond bits push the sandbox below its own start
	// time and it drops out here while the paused page would still return it.
	atSandboxStart := detailResponse.JSON200.StartedAt
	listResponse, err = c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		State:        &state,
		Metadata:     &metadataString,
		Template:     &sbx.TemplateID,
		StartedAfter: &atSandboxStart,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	require.Len(t, *listResponse.JSON200, 1)
	assert.Equal(t, sbx.SandboxID, (*listResponse.JSON200)[0].SandboxID)
	assert.Equal(t, "1", listResponse.HTTPResponse.Header.Get("X-Total-Running"),
		"the inclusive bound must not undercount the running total either")
}

func TestSandboxListRunning(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	uniqueString := id.Generate()
	metadataString := fmt.Sprintf("sandboxType=%s", uniqueString)

	// Create a sandbox
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{"sandboxType": uniqueString}))

	// List running sandboxes
	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Metadata: &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	assert.GreaterOrEqual(t, len(*listResponse.JSON200), 1)

	// Verify our running sandbox is in the list
	found := false
	for _, s := range *listResponse.JSON200 {
		if s.SandboxID == sbx.SandboxID {
			found = true
			assert.Equal(t, api.Running, s.State)

			break
		}
	}
	assert.True(t, found)
}

func TestSandboxListRunning_NoMetadata(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithoutAnyMetadata())
	sandboxID := sbx.SandboxID

	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		State: &[]api.SandboxState{api.Running},
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode(), string(listResponse.Body))

	sandboxIds := sharedUtils.Map(*listResponse.JSON200, func(s api.ListedSandbox) string {
		return s.SandboxID
	})
	assert.Contains(t, sandboxIds, sandboxID)
}

func TestSandboxListPaused(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sandboxID := sbx.SandboxID

	pauseSandbox(t, c, sandboxID)

	// List paused sandboxes
	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		State:    &[]api.SandboxState{api.Paused},
		Metadata: &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	assert.GreaterOrEqual(t, len(*listResponse.JSON200), 1)

	// Verify our paused sandbox is in the list
	found := false
	for _, s := range *listResponse.JSON200 {
		if s.SandboxID == sandboxID {
			found = true
			assert.Equal(t, api.Paused, s.State)

			break
		}
	}
	assert.True(t, found)
}

func TestSandboxListPausing(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sandboxID := sbx.SandboxID

	wg := errgroup.Group{}
	wg.Go(func() error {
		pauseSandboxResponse, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		if err != nil {
			return err
		}

		if pauseSandboxResponse.StatusCode() != http.StatusNoContent {
			return fmt.Errorf("expected status code %d, got %d", http.StatusNoContent, pauseSandboxResponse.StatusCode())
		}

		return nil
	})

	require.Eventually(t, func() bool {
		// List paused sandboxes
		listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
			State:    &[]api.SandboxState{api.Running, api.Paused},
			Metadata: &metadataString,
		}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResponse.StatusCode())
		assert.GreaterOrEqual(t, len(*listResponse.JSON200), 1)

		// Verify our paused sandbox is in the list
		found := false
		for _, s := range *listResponse.JSON200 {
			if s.SandboxID == sandboxID {
				found = true
				if s.State == api.Paused {
					return true
				}
			}
		}

		// The sandbox has to be always present
		require.True(t, found)

		return false
	}, 10*time.Second, 100*time.Millisecond, "Sandbox did not reach paused state in time")

	err := wg.Wait()
	require.NoError(t, err)
}

func TestSandboxListPaused_NoMetadata(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithoutAnyMetadata())
	sandboxID := sbx.SandboxID

	pauseSandbox(t, c, sandboxID)

	// List paused sandboxes
	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		State: &[]api.SandboxState{api.Paused},
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())

	sandboxIds := sharedUtils.Map(*listResponse.JSON200, func(s api.ListedSandbox) string {
		return s.SandboxID
	})
	assert.Contains(t, sandboxIds, sandboxID)
}

func TestSandboxListPaginationRunning(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbx1 := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sandbox1ID := sbx1.SandboxID

	sbx2 := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sandbox2ID := sbx2.SandboxID

	sbx3 := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sandbox3ID := sbx3.SandboxID

	// Test pagination with limit
	var limit int32 = 1

	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Limit:    &limit,
		State:    &[]api.SandboxState{api.Running},
		Metadata: &metadataString,
	}, setup.WithAPIKey())

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	require.Len(t, *listResponse.JSON200, 1)
	assert.Equal(t, sandbox3ID, (*listResponse.JSON200)[0].SandboxID)

	totalHeader := listResponse.HTTPResponse.Header.Get("X-Total-Running")
	total, err := strconv.Atoi(totalHeader)
	require.NoError(t, err)
	assert.Equal(t, 3, total)

	// Get second page using the next token from first response
	nextToken := listResponse.HTTPResponse.Header.Get("X-Next-Token")
	assert.NotEmpty(t, nextToken)

	secondPageResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Limit:     &limit,
		NextToken: &nextToken,
		State:     &[]api.SandboxState{api.Running},
		Metadata:  &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, secondPageResponse.StatusCode())
	require.Len(t, *secondPageResponse.JSON200, 1)
	assert.Equal(t, sandbox2ID, (*secondPageResponse.JSON200)[0].SandboxID)

	// Get third page using the next token from second response
	nextToken = secondPageResponse.HTTPResponse.Header.Get("X-Next-Token")
	assert.NotEmpty(t, nextToken)

	thirdPageResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Limit:     &limit,
		NextToken: &nextToken,
		State:     &[]api.SandboxState{api.Running},
		Metadata:  &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, thirdPageResponse.StatusCode())
	require.Len(t, *thirdPageResponse.JSON200, 1)
	assert.Equal(t, sandbox1ID, (*thirdPageResponse.JSON200)[0].SandboxID)

	// No more pages
	nextToken = thirdPageResponse.HTTPResponse.Header.Get("X-Next-Token")
	assert.Empty(t, nextToken)
}

func TestSandboxListPaginationRunningLargerLimit(t *testing.T) { //nolint:tparallel // uses counts of a shared resource
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbxsCount := 12
	sandboxes := make([]string, sbxsCount)
	for i := range sbxsCount {
		// Default 30s timeout is shorter than creating 12 sandboxes plus the
		// list assertions under load, so the sandboxes would expire mid-test.
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(300), utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
		sandboxes[sbxsCount-i-1] = sbx.SandboxID

		t.Logf("Created sandbox %d/%d: %s", i+1, sbxsCount, sbx.SandboxID)
	}

	require.EventuallyWithT(t, func(cT *assert.CollectT) {
		listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
			Limit:    new(int32(sbxsCount)),
			State:    &[]api.SandboxState{api.Running},
			Metadata: &metadataString,
		}, setup.WithAPIKey())
		require.NoError(cT, err)
		require.NotNil(cT, listResponse)
		require.Equal(cT, http.StatusOK, listResponse.StatusCode())
		require.NotNil(cT, listResponse.JSON200)
		require.Len(cT, *listResponse.JSON200, sbxsCount)
	}, time.Minute, time.Second)

	t.Run("check all sandboxes list", func(t *testing.T) {
		t.Parallel()
		listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
			Limit:    new(int32(sbxsCount)),
			State:    &[]api.SandboxState{api.Running},
			Metadata: &metadataString,
		}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResponse.StatusCode())
		require.Len(t, *listResponse.JSON200, sbxsCount)
		for i := range sbxsCount {
			assert.Equal(t, sandboxes[i], (*listResponse.JSON200)[i].SandboxID)
		}
	})

	t.Run("check paginated list", func(t *testing.T) {
		t.Parallel()
		// Test pagination with limit
		limit := 2

		var nextToken *string
		for i := 0; i < sbxsCount; i += limit {
			sbxID := sandboxes[i]

			listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
				Limit:     new(int32(limit)),
				NextToken: nextToken,
				State:     &[]api.SandboxState{api.Running},
				Metadata:  &metadataString,
			}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, listResponse.StatusCode())
			require.Len(t, *listResponse.JSON200, limit)
			assert.Equal(t, sbxID, (*listResponse.JSON200)[0].SandboxID, "page starting at %d should start with sandbox %s, token %s", i, sbxID, sharedUtils.Sprintp(nextToken))

			totalHeader := listResponse.HTTPResponse.Header.Get("X-Total-Running")
			total, err := strconv.Atoi(totalHeader)
			require.NoError(t, err)
			assert.Equal(t, sbxsCount, total)

			nextToken = new(listResponse.HTTPResponse.Header.Get("X-Next-Token"))

			if i+limit == sbxsCount {
				assert.Empty(t, *nextToken)
			} else {
				assert.NotEmpty(t, *nextToken)
			}
		}
	})
}

func TestSandboxListPaginationPaused(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbx1 := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sandbox1ID := sbx1.SandboxID
	pauseSandbox(t, c, sandbox1ID)

	sbx2 := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sandbox2ID := sbx2.SandboxID
	pauseSandbox(t, c, sandbox2ID)

	// Test pagination with limit
	var limit int32 = 1

	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Limit:    &limit,
		State:    &[]api.SandboxState{api.Paused},
		Metadata: &metadataString,
	}, setup.WithAPIKey())

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	require.Len(t, *listResponse.JSON200, 1)
	assert.Equal(t, sandbox2ID, (*listResponse.JSON200)[0].SandboxID)

	// Get second page using the next token from first response
	nextToken := listResponse.HTTPResponse.Header.Get("X-Next-Token")
	assert.NotEmpty(t, nextToken)

	secondPageResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Limit:     &limit,
		NextToken: &nextToken,
		State:     &[]api.SandboxState{api.Paused},
		Metadata:  &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, secondPageResponse.StatusCode())
	require.Len(t, *secondPageResponse.JSON200, 1)
	assert.Equal(t, sandbox1ID, (*secondPageResponse.JSON200)[0].SandboxID)

	// No more pages
	nextToken = secondPageResponse.HTTPResponse.Header.Get("X-Next-Token")
	assert.Empty(t, nextToken)
}

func TestSandboxListPaginationRunningAndPaused(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbx1 := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))
	sbx2 := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))

	sandbox1ID := sbx1.SandboxID
	sandbox2ID := sbx2.SandboxID

	// Pause the first sandbox
	pauseSandbox(t, c, sandbox1ID)

	// Test pagination with limit
	var limit int32 = 1

	listResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Limit:    &limit,
		State:    &[]api.SandboxState{api.Running, api.Paused},
		Metadata: &metadataString,
	}, setup.WithAPIKey())

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	require.Len(t, *listResponse.JSON200, 1)
	assert.Equal(t, sandbox2ID, (*listResponse.JSON200)[0].SandboxID)

	// Get second page using the next token from first response
	nextToken := listResponse.HTTPResponse.Header.Get("X-Next-Token")
	assert.NotEmpty(t, nextToken)

	secondPageResponse, err := c.GetV2SandboxesWithResponse(t.Context(), &api.GetV2SandboxesParams{
		Limit:     &limit,
		NextToken: &nextToken,
		State:     &[]api.SandboxState{api.Running, api.Paused},
		Metadata:  &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, secondPageResponse.StatusCode())
	require.Len(t, *secondPageResponse.JSON200, 1)
	assert.Equal(t, sandbox1ID, (*secondPageResponse.JSON200)[0].SandboxID)

	// No more pages
	nextToken = secondPageResponse.HTTPResponse.Header.Get("X-Next-Token")
	assert.Empty(t, nextToken)
}

// legacy tests
func TestSandboxListRunningV1(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))

	// List running sandboxes
	listResponse, err := c.GetSandboxesWithResponse(t.Context(), &api.GetSandboxesParams{
		Metadata: &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	assert.GreaterOrEqual(t, len(*listResponse.JSON200), 1)

	// Verify our running sandbox is in the list
	found := false
	for _, s := range *listResponse.JSON200 {
		if s.SandboxID == sbx.SandboxID {
			found = true
			assert.Equal(t, api.Running, s.State)

			break
		}
	}
	assert.True(t, found)
}

func TestSandboxListWithFilterV1(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	metadataKey := "uniqueIdentifier"
	metadataValue := id.Generate()
	metadataString := fmt.Sprintf("%s=%s", metadataKey, metadataValue)

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithMetadata(api.SandboxMetadata{metadataKey: metadataValue}))

	// List with filter
	listResponse, err := c.GetSandboxesWithResponse(t.Context(), &api.GetSandboxesParams{
		Metadata: &metadataString,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	require.Len(t, *listResponse.JSON200, 1)
	assert.Equal(t, sbx.SandboxID, (*listResponse.JSON200)[0].SandboxID)
}

func TestSandboxListSortedV1(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create three sandboxes
	sbx1 := utils.SetupSandboxWithCleanup(t, c)
	sbx2 := utils.SetupSandboxWithCleanup(t, c)
	sbx3 := utils.SetupSandboxWithCleanup(t, c)

	// List with filter
	listResponse, err := c.GetSandboxesWithResponse(t.Context(), nil, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResponse.StatusCode())
	require.GreaterOrEqual(t, len(*listResponse.JSON200), 3)

	// Verify all sandboxes are in the list
	contains := 0
	for _, sbx := range *listResponse.JSON200 {
		switch sbx.SandboxID {
		case sbx1.SandboxID, sbx2.SandboxID, sbx3.SandboxID:
			contains++
		}
	}

	assert.Equal(t, 3, contains)

	// Verify the order of the sandboxes
	for i := range len(*listResponse.JSON200) - 1 {
		assert.True(t, (*listResponse.JSON200)[i].StartedAt.After((*listResponse.JSON200)[i+1].StartedAt))
	}
}
