package guest

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type namedProfile struct {
	defaultProfile

	name  string
	label string
}

func (p namedProfile) Name() string { return p.name }

func (p namedProfile) Label() string {
	switch p.label {
	case "":
		return p.name
	case "-":
		return ""
	default:
		return p.label
	}
}

func TestDefaultRegistryClaimsOnlyTheEmptyName(t *testing.T) {
	t.Parallel()

	r, err := NewRegistry()
	require.NoError(t, err)

	p, err := r.Lookup("")
	require.NoError(t, err)
	assert.Equal(t, DefaultLabel, p.Label())

	_, err = r.Lookup("other")
	require.ErrorIs(t, err, ErrUnclaimed)
}

func TestRegistryResolvesARegisteredProfile(t *testing.T) {
	t.Parallel()

	r, err := NewRegistry(namedProfile{name: "other"})
	require.NoError(t, err)

	p, err := r.Lookup("other")
	require.NoError(t, err)
	assert.Equal(t, "other", p.Name())
}

func TestRegistryRefusesBadProfiles(t *testing.T) {
	t.Parallel()

	for name, profiles := range map[string][]Profile{
		"nil":             {nil},
		"typed nil":       {(*pointerProfile)(nil)},
		"empty":           {namedProfile{name: ""}},
		"duplicate":       {namedProfile{name: "other"}, namedProfile{name: "other"}},
		"duplicate label": {namedProfile{name: "a", label: "x"}, namedProfile{name: "b", label: "x"}},
		"default label":   {namedProfile{name: "a", label: DefaultLabel}},
		"empty label":     {namedProfile{name: "a", label: "-"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewRegistry(profiles...)
			require.Error(t, err)
		})
	}
}

func TestProcessRegistryDefaultsToTheDefaultProfile(t *testing.T) {
	t.Parallel()

	_, err := Lookup(DefaultName)
	require.NoError(t, err)
	_, err = Lookup("other")
	require.ErrorIs(t, err, ErrUnclaimed)
}

// A zero value is the default behaviour, so the default profile returns the zero
// value from every method but Name and Label. Each such method is exported, takes
// no arguments and returns a single value.
func TestDefaultProfileReturnsZeroValues(t *testing.T) {
	t.Parallel()

	p := reflect.ValueOf(Default())
	typ := reflect.TypeFor[Profile]()
	for m := range typ.Methods() {
		if m.Name == "Name" || m.Name == "Label" {
			continue
		}
		require.True(t, m.IsExported(), m.Name)
		require.Zero(t, m.Type.NumIn(), m.Name)
		require.Equal(t, 1, m.Type.NumOut(), m.Name)
		out := p.MethodByName(m.Name).Call(nil)
		assert.True(t, out[0].IsZero(), "%s returns a non-zero value", m.Name)
	}
	assert.Equal(t, DefaultName, Default().Name())
	assert.Equal(t, DefaultLabel, Default().Label())
}

func TestResetDefaults(t *testing.T) {
	t.Parallel()

	r := Default().Reset()
	assert.Equal(t, DefaultBootIdentityCommand, r.IdentityCommand())
	assert.True(t, r.IsSameBoot("a\n", "a"))
	assert.False(t, r.IsSameBoot("a", "b"))

	custom := Reset{BootIdentityCommand: "probe", SameBoot: func(_, _ string) bool { return true }}
	assert.Equal(t, "probe", custom.IdentityCommand())
	assert.True(t, custom.IsSameBoot("a", "b"))
}

func TestInstallRefusesNil(t *testing.T) {
	t.Parallel()

	require.Error(t, Install(nil))
}

func TestTemplateBuildSkips(t *testing.T) {
	t.Parallel()

	build := TemplateBuild{SkipPhases: []Phase{PhaseUser, PhaseOptimize}}
	assert.True(t, build.Skips(PhaseUser))
	assert.True(t, build.Skips(PhaseOptimize))
	assert.False(t, build.Skips(PhaseConfigure))
	assert.False(t, TemplateBuild{}.Skips(PhaseUser))
}

// pointerProfile is a profile with a pointer receiver, so a typed nil of it is
// a non-nil interface.
type pointerProfile struct {
	defaultProfile
}

func (p *pointerProfile) Name() string { return "pointer" }
