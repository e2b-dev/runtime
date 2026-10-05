package apiid_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/apiid"
)

func TestWorkspaceIDCanonicalInput(t *testing.T) {
	t.Parallel()
	internal := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	public := "wrk_7mfb0gpp6c8dsaasre0asc7n3s"
	want := apiid.WorkspaceID{UUID: internal, PublicID: public}
	require.Equal(t, want, apiid.NewWorkspaceID(internal))
	for _, input := range []string{internal.String(), public} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			var value apiid.WorkspaceID
			require.NoError(t, value.UnmarshalText([]byte(input)))
			require.Equal(t, want, value)
			require.NoError(t, value.UnmarshalText([]byte(input)))
			require.Equal(t, want, value)
			require.NoError(t, value.Bind(input))
			require.Equal(t, want, value)
			require.NoError(t, value.UnmarshalParam(input))
			require.Equal(t, want, value)
			require.NoError(t, json.Unmarshal([]byte(`"`+input+`"`), &value))
			require.Equal(t, want, value)
			encoded, err := json.Marshal(value)
			require.NoError(t, err)
			require.JSONEq(t, `"`+public+`"`, string(encoded))
			encoded, err = value.MarshalText()
			require.NoError(t, err)
			require.Equal(t, public, string(encoded))
		})
	}
}

func TestWorkspaceIDRejectsInvalidInputWithoutMutation(t *testing.T) {
	t.Parallel()
	original := apiid.NewWorkspaceID(uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479"))
	for _, input := range []string{"", "garbage", "prj_7mfb0gpp6c8dsaasre0asc7n3s", "WRK_7MFB0GPP6C8DSAASRE0ASC7N3S", "wrk_7mfb0gpp6c8dsaasre0asc7n3", "wrk_8mfb0gpp6c8dsaasre0asc7n3s", "wrk_imfb0gpp6c8dsaasre0asc7n3s"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			value := original
			require.Error(t, value.UnmarshalText([]byte(input)))
			require.Equal(t, original, value)
		})
	}
	for _, input := range []string{`null`, `12`, `true`, `{}`, `[]`, `""`, `"prj_7mfb0gpp6c8dsaasre0asc7n3s"`} {
		t.Run("json/"+input, func(t *testing.T) {
			t.Parallel()
			value := original
			require.Error(t, json.Unmarshal([]byte(input), &value))
			require.Equal(t, original, value)
		})
	}
}

func TestWorkspaceIDEncodingDerivesPublicIDFromUUID(t *testing.T) {
	t.Parallel()
	value := apiid.WorkspaceID{UUID: uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479"), PublicID: "stale"}
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `"wrk_7mfb0gpp6c8dsaasre0asc7n3s"`, string(encoded))
}
