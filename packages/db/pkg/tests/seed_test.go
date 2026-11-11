package tests

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func seedTeam(t *testing.T, sqlDB *sql.DB, slug string) uuid.UUID {
	t.Helper()

	teamID := uuid.New()
	_, err := sqlDB.ExecContext(t.Context(), `
		INSERT INTO public.teams (id, name, tier, email, slug)
		VALUES ($1, $2, 'base_v1', $3, $4)
	`, teamID, slug, slug+"@example.com", slug+"-"+teamID.String()[:8])
	require.NoError(t, err)

	return teamID
}
