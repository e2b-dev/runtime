package handlers

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	handlersmocks "github.com/e2b-dev/infra/packages/api/internal/handlers/mocks"
)

func TestResolveFilesystemOnlySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		memory   *bool
		flag     *bool
		wantCode int
	}{
		// A nil flag expectation makes the mock fail on any consultation.
		{"absent field: full snapshot, flag not consulted", nil, nil, 0},
		{"memory true: full snapshot, flag not consulted", new(true), nil, 0},
		{"memory false, flag off: rejected, never downgraded", new(false), new(false), http.StatusBadRequest},
		{"memory false, flag on: allowed", new(false), new(true), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flags := handlersmocks.NewMockFeatureFlagsClient(t)
			if tt.flag != nil {
				flags.EXPECT().BoolFlag(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(*tt.flag)
			}

			apiErr := resolveFilesystemOnlySnapshot(t.Context(), flags, tt.memory, "team", "sbx")

			if tt.wantCode == 0 {
				assert.Nil(t, apiErr)

				return
			}
			assert.NotNil(t, apiErr)
			assert.Equal(t, tt.wantCode, apiErr.Code)
			assert.Equal(t, errCodeFilesystemOnlySnapshotDisabled, apiErr.ErrorCode)
			assert.Contains(t, apiErr.ClientMsg, "memory: false")
		})
	}
}
