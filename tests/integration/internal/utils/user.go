package utils

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

func CreateUser(t *testing.T, db *setup.Database) uuid.UUID {
	t.Helper()

	userID := uuid.New()

	err := db.AuthDb.UpsertPublicUser(t.Context(), userID)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.AuthDb.DeletePublicUser(t.Context(), userID)
	})

	return userID
}
