package id

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
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

			if tc.got != tc.want {
				t.Errorf("String() = %q, want %q", tc.got, tc.want)
			}
			if len(tc.got) != ObjectIDLen {
				t.Errorf("len(String()) = %d, want %d", len(tc.got), ObjectIDLen)
			}
			if tc.got != strings.ToLower(tc.got) {
				t.Errorf("String() = %q, want lowercase", tc.got)
			}
		})
	}
}

func TestObjectIDRoundTrip(t *testing.T) {
	t.Parallel()

	v4 := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	v7 := uuid.MustParse("019fa519-bf79-724d-8811-a2bfda9755fa")

	project, err := ParseProjectID(ProjectID(v4).String())
	if err != nil {
		t.Fatalf("ParseProjectID(): %v", err)
	}
	if got := uuid.UUID(project); got != v4 {
		t.Errorf("ParseProjectID() = %v, want %v", got, v4)
	}

	workspace, err := ParseWorkspaceID(WorkspaceID(v4).String())
	if err != nil {
		t.Fatalf("ParseWorkspaceID(): %v", err)
	}
	if got := uuid.UUID(workspace); got != v4 {
		t.Errorf("ParseWorkspaceID() = %v, want %v", got, v4)
	}

	secret, err := ParseSecretID(SecretID(v7).String())
	if err != nil {
		t.Fatalf("ParseSecretID(): %v", err)
	}
	if got := uuid.UUID(secret); got != v7 {
		t.Errorf("ParseSecretID() = %v, want %v", got, v7)
	}
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
			if err == nil {
				t.Fatalf("parse(%q) returned no error", tc.input)
			}
			if !errors.Is(err, typeid.ErrParse) {
				t.Errorf("parse(%q) = %v, want typeid.ErrParse", tc.input, err)
			}
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
		if typ.Kind() != reflect.Array || typ.Size() != reflect.TypeFor[uuid.UUID]().Size() {
			t.Errorf("%s is %v of size %d, want a UUID-backed array", typ, typ.Kind(), typ.Size())
		}
	}
	if types[0] == types[1] || types[0] == types[2] || types[1] == types[2] {
		t.Fatalf("object ID types are not distinct: %v", types)
	}

	u := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	if got := uuid.UUID(ProjectID(u)); got != u {
		t.Errorf("UUID conversion = %v, want %v", got, u)
	}
}

func TestConvertTeamIDToProjectID(t *testing.T) {
	t.Parallel()

	teamID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	projectID := ConvertTeamIDToProjectID(teamID)

	if got := uuid.UUID(projectID); got != teamID {
		t.Errorf("ConvertTeamIDToProjectID() = %v, want %v", got, teamID)
	}
	if got, want := projectID.String(), "prj_2n1t201rmv87aae5j4csam8000"; got != want {
		t.Errorf("ProjectID.String() = %q, want %q", got, want)
	}
}

func TestParseTeamIDAcceptsUUIDAndProjectID(t *testing.T) {
	t.Parallel()

	teamID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	for _, input := range []string{teamID.String(), "prj_2n1t201rmv87aae5j4csam8000"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			got, err := ParseTeamID(input)
			if err != nil {
				t.Fatalf("ParseTeamID(%q): %v", input, err)
			}
			if got != teamID {
				t.Errorf("ParseTeamID(%q) = %v, want %v", input, got, teamID)
			}
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

			if got, err := ParseTeamID(input); err == nil {
				t.Errorf("ParseTeamID(%q) = %v, want an error", input, got)
			}
		})
	}
}

// The legacy and TypeID suffix alphabets overlap, so parsing always uses the
// new TypeID interpretation rather than attempting an ambiguous fallback.
func TestLegacyBodyUsesTypeIDInterpretation(t *testing.T) {
	t.Parallel()

	projectID, err := ParseProjectID("prj_6zhag5xcjyf4sadahkmb6xrfqf")
	if err != nil {
		t.Fatalf("ParseProjectID(): %v", err)
	}

	want := uuid.MustParse("df8aa05e-b25e-7932-a6aa-33a2cddc3eef")
	if got := uuid.UUID(projectID); got != want {
		t.Errorf("ParseProjectID() = %v, want %v", got, want)
	}
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
