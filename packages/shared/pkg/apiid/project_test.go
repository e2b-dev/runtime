package apiid_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/apiid"
)

func TestProjectIDCanonicalInput(t *testing.T) {
	t.Parallel()
	internal := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	public := "prj_7mfb0gpp6c8dsaasre0asc7n3s"
	want := apiid.ProjectID{UUID: internal, PublicID: public}
	require.Equal(t, want, apiid.NewProjectID(internal))
	for _, input := range []string{internal.String(), public} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			var value apiid.ProjectID
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

func TestProjectIDRejectsInvalidInputWithoutMutation(t *testing.T) {
	t.Parallel()
	original := apiid.NewProjectID(uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479"))
	for _, input := range []string{"", "garbage", "wrk_7mfb0gpp6c8dsaasre0asc7n3s", "PRJ_7MFB0GPP6C8DSAASRE0ASC7N3S", "prj_7mfb0gpp6c8dsaasre0asc7n3", "prj_8mfb0gpp6c8dsaasre0asc7n3s", "prj_imfb0gpp6c8dsaasre0asc7n3s"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			value := original
			require.Error(t, value.UnmarshalText([]byte(input)))
			require.Equal(t, original, value)
		})
	}
	for _, input := range []string{`null`, `12`, `true`, `{}`, `[]`, `""`, `"wrk_7mfb0gpp6c8dsaasre0asc7n3s"`} {
		t.Run("json/"+input, func(t *testing.T) {
			t.Parallel()
			value := original
			require.Error(t, json.Unmarshal([]byte(input), &value))
			require.Equal(t, original, value)
		})
	}
}

func TestProjectIDEncodingDerivesPublicIDFromUUID(t *testing.T) {
	t.Parallel()
	value := apiid.ProjectID{UUID: uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479"), PublicID: "stale"}
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, `"prj_7mfb0gpp6c8dsaasre0asc7n3s"`, string(encoded))
}
