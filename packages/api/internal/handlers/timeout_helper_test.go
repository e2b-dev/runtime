package handlers

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	typesteam "github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func testTeamWithMaxLengthHours(hours int64) *typesteam.Team {
	return &typesteam.Team{
		Limits: &typesteam.TeamLimits{
			MaxLengthHours: hours,
		},
	}
}

// TestCalculateTimeoutSeconds verifies create-time timeout handling:
// no timeout -> do not persist, short timeout -> min floor, long timeout -> team cap.
func TestCalculateTimeoutSeconds(t *testing.T) {
	t.Parallel()
	team := testTeamWithMaxLengthHours(1)
	minTimeout := time.Minute

	// Create without explicit timeout should floor to the anti-thrash minimum.
	timeout := calculateTimeoutSeconds(0, minTimeout, team)
	require.Equal(t, uint64(60), timeout)

	// Very short requests are floored to the anti-thrash minimum.
	timeout = calculateTimeoutSeconds(15*time.Second, minTimeout, team)
	require.Equal(t, uint64(60), timeout)

	// Very long requests are capped by the team's maximum sandbox length.
	timeout = calculateTimeoutSeconds(2*time.Hour, minTimeout, team)
	require.Equal(t, uint64(3600), timeout)
}

// TestCalculateAutoResumeTimeout verifies resume-time timeout handling:
// default fallback, persisted timeout minimum floor, and team cap.
func TestCalculateAutoResumeTimeout(t *testing.T) {
	t.Parallel()
	team := testTeamWithMaxLengthHours(1)
	minTimeout := time.Minute

	// Older snapshots without persisted value should use the proxy fallback timeout.
	timeout := calculateAutoResumeTimeout(nil, minTimeout, team)
	require.Equal(t, 5*time.Minute, timeout)

	// Persisted values below minimum are floored to the anti-thrash minimum.
	timeout = calculateAutoResumeTimeout(
		&dbtypes.SandboxAutoResumeConfig{Timeout: 20},
		minTimeout,
		team,
	)
	require.Equal(t, time.Minute, timeout)

	// Persisted values above plan limit are capped by the team limit.
	timeout = calculateAutoResumeTimeout(
		&dbtypes.SandboxAutoResumeConfig{Timeout: 7200},
		minTimeout,
		team,
	)
	require.Equal(t, time.Hour, timeout)
}

func TestValidateAndParseTimeout(t *testing.T) {
	t.Parallel()

	maxHours := int64(24)

	t.Run("nil timeout uses default", func(t *testing.T) {
		t.Parallel()
		d, err := validateAndParseTimeout(nil, maxHours)
		require.Nil(t, err)
		require.Equal(t, 15*time.Second, d)
	})

	t.Run("valid timeout", func(t *testing.T) {
		t.Parallel()
		v := int32(60)
		d, err := validateAndParseTimeout(&v, maxHours)
		require.Nil(t, err)
		require.Equal(t, 60*time.Second, d)
	})

	t.Run("zero timeout returns 400 error", func(t *testing.T) {
		t.Parallel()
		v := int32(0)
		_, err := validateAndParseTimeout(&v, maxHours)
		require.NotNil(t, err)
		require.Equal(t, http.StatusBadRequest, err.Code)
		require.Equal(t, "Timeout must be greater than 0", err.ClientMsg)
	})

	t.Run("negative timeout returns 400 error", func(t *testing.T) {
		t.Parallel()
		v := int32(-10)
		_, err := validateAndParseTimeout(&v, maxHours)
		require.NotNil(t, err)
		require.Equal(t, http.StatusBadRequest, err.Code)
		require.Equal(t, "Timeout must be greater than 0", err.ClientMsg)
	})

	t.Run("timeout exceeding max hours returns 400 error", func(t *testing.T) {
		t.Parallel()
		v := int32(90000)
		_, err := validateAndParseTimeout(&v, maxHours)
		require.NotNil(t, err)
		require.Equal(t, http.StatusBadRequest, err.Code)
		require.Equal(t, "Timeout cannot be greater than 24 hours", err.ClientMsg)
	})
}
