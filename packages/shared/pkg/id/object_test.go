package id

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sumup/typeid"
)

func TestObjectIDString(t *testing.T) {
	t.Parallel()

	v4 := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	v7 := uuid.MustParse("019fa519-bf79-724d-8811-a2bfda9755fa")

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"project", ProjectID(v4).String(), "prj_7mfb0gpp6c8dsaasre0asc7n3s"},
		{"workspace", WorkspaceID(v4).String(), "wrk_7mfb0gpp6c8dsaasre0asc7n3s"},
		{"secret", SecretID(v7).String(), "sec_01kyjhkfvse96rg4d2qzd9enft"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, tc.got)
			assert.Len(t, tc.got, ObjectIDLen)
			assert.Equal(t, strings.ToLower(tc.got), tc.got, "want lowercase")
		})
	}
}

func TestObjectIDRoundTrip(t *testing.T) {
	t.Parallel()

	v4 := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	v7 := uuid.MustParse("019fa519-bf79-724d-8811-a2bfda9755fa")

	project, err := ParseProjectID(ProjectID(v4).String())
	require.NoError(t, err)
	assert.Equal(t, v4, uuid.UUID(project))

	workspace, err := ParseWorkspaceID(WorkspaceID(v4).String())
	require.NoError(t, err)
	assert.Equal(t, v4, uuid.UUID(workspace))

	secret, err := ParseSecretID(SecretID(v7).String())
	require.NoError(t, err)
	assert.Equal(t, v7, uuid.UUID(secret))
}

func TestObjectIDParseRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	validProject := ProjectID(uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")).String()
	validSecret := SecretID(uuid.MustParse("019fa519-bf79-724d-8811-a2bfda9755fa")).String()

	cases := []struct {
		name  string
		parse func(string) error
		input string
	}{
		{"project wrong kind", parseProjectError, "wrk_" + validProject[len(projectIDPrefix):]},
		{"project uppercase", parseProjectError, strings.ToUpper(validProject)},
		{"project short", parseProjectError, validProject[:len(validProject)-1]},
		{"project bad alphabet", parseProjectError, validProject[:len(validProject)-1] + "i"},
		{"workspace wrong kind", parseWorkspaceError, validProject},
		{"secret wrong kind", parseSecretError, "prj_" + validSecret[len(SecretIDPrefix):]},
		{"secret uppercase", parseSecretError, strings.ToUpper(validSecret)},
		{"secret short", parseSecretError, validSecret[:len(validSecret)-1]},
		{"secret bad alphabet", parseSecretError, validSecret[:len(validSecret)-1] + "i"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.parse(tc.input)
			require.Error(t, err)
			assert.ErrorIs(t, err, typeid.ErrParse)
		})
	}
}

func TestObjectIDTypesRemainUUIDBacked(t *testing.T) {
	t.Parallel()

	types := []reflect.Type{
		reflect.TypeFor[ProjectID](),
		reflect.TypeFor[WorkspaceID](),
		reflect.TypeFor[SecretID](),
	}
	for _, typ := range types {
		assert.Equal(t, reflect.Array, typ.Kind(), "%s should be a UUID-backed array", typ)
		assert.Equal(t, reflect.TypeFor[uuid.UUID]().Size(), typ.Size(), "%s should be a UUID-backed array", typ)
	}
	require.NotEqual(t, types[0], types[1])
	require.NotEqual(t, types[0], types[2])
	require.NotEqual(t, types[1], types[2])

	u := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	assert.Equal(t, u, uuid.UUID(ProjectID(u)))
}

func TestConvertTeamIDToProjectID(t *testing.T) {
	t.Parallel()

	teamID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	projectID := ConvertTeamIDToProjectID(teamID)

	assert.Equal(t, teamID, uuid.UUID(projectID))
	assert.Equal(t, "prj_2n1t201rmv87aae5j4csam8000", projectID.String())
}

func TestParseTeamIDAcceptsUUIDAndProjectID(t *testing.T) {
	t.Parallel()

	teamID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	for _, input := range []string{teamID.String(), "prj_2n1t201rmv87aae5j4csam8000"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			got, err := ParseTeamID(input)
			require.NoError(t, err)
			assert.Equal(t, teamID, got)
		})
	}
}

func TestParseTeamIDRejectsOtherInput(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"",
		"team",
		"wrk_2n1t201rmv87aae5j4csam8000",
		"PRJ_2N1T201RMV87AAE5J4CSAM8000",
		"prj_2n1t201rmv87aae5j4csam800",
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			_, err := ParseTeamID(input)
			assert.Error(t, err)
		})
	}
}

// The legacy and TypeID suffix alphabets overlap, so parsing always uses the
// new TypeID interpretation rather than attempting an ambiguous fallback.
func TestLegacyBodyUsesTypeIDInterpretation(t *testing.T) {
	t.Parallel()

	projectID, err := ParseProjectID("prj_6zhag5xcjyf4sadahkmb6xrfqf")
	require.NoError(t, err)

	want := uuid.MustParse("df8aa05e-b25e-7932-a6aa-33a2cddc3eef")
	assert.Equal(t, want, uuid.UUID(projectID))
}

func parseProjectError(s string) error {
	_, err := ParseProjectID(s)

	return err
}

func parseWorkspaceError(s string) error {
	_, err := ParseWorkspaceID(s)

	return err
}

func parseSecretError(s string) error {
	_, err := ParseSecretID(s)

	return err
}
