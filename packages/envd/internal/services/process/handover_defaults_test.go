//go:build unix

package process

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func newDefaults(user string, workdir *string) *execcontext.Defaults {
	return &execcontext.Defaults{
		User: user, UserDelivered: true, Workdir: workdir, EnvVars: utils.NewEnvVars(),
	}
}

// TestSchemaForDefaults pins the stamping rule. Stamping too HIGH is not a cosmetic
// problem: a reader that refuses the blob aborts post-execve, where there is no old
// binary to fall back to, so the sandbox is torn down. Stamping too LOW loses the
// identity silently, which is the bug this field exists to prevent.
func TestSchemaForDefaults(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		st   *upgrade.HandoverState
		want uint32
	}{
		{"no defaults at all", &upgrade.HandoverState{}, handoverSchemaBase},
		{
			"default equals the builtin fallback",
			&upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{User: execcontext.BuiltinDefaultUser}},
			handoverSchemaBase,
		},
		{
			"a non-root default an older reader would drop",
			&upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{User: "user"}},
			handoverSchemaDefaults,
		},
		{
			"builtin user but a recorded workdir",
			&upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{
				User: execcontext.BuiltinDefaultUser, Workdir: "/opt/wd", HasWorkdir: true,
			}},
			handoverSchemaDefaults,
		},
		{
			"a workdir with no delivered user still has to be understood",
			&upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{
				Workdir: "/opt/wd", HasWorkdir: true,
			}},
			handoverSchemaDefaults,
		},
		{
			"guest-frozen cgroups still stamp their own schema",
			&upgrade.HandoverState{GuestFrozenCgroups: []string{"customer/c1"}},
			handoverSchemaGuestFrozen,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, schemaFor(tc.st))
		})
	}
}

// TestDefaultsCarryingBlobOutranksAPreDefaultsReader states the cost of stamping, so it is a
// checked property rather than an argument in a design doc.
//
// A blob carrying an exec context is stamped above every schema that predates the field, so a
// binary built without the field refuses it — and refusal is fatal, because the read happens
// after the execve with no old image to fall back to (TestResumeFromHandover_RejectsNewerSchema
// covers that half). The consequence: reverting this field is not a rollback while live
// upgrades are enabled, and turning the flag off is.
func TestDefaultsCarryingBlobOutranksAPreDefaultsReader(t *testing.T) {
	t.Parallel()

	for name, st := range map[string]*upgrade.HandoverState{
		"a delivered user": {Defaults: &upgrade.HandoverDefaults{User: "user"}},
		"a workdir alone": {Defaults: &upgrade.HandoverDefaults{
			Workdir: "/opt/wd", HasWorkdir: true,
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Greater(t, schemaFor(st), uint32(handoverSchemaGuestFrozen),
				"a reader that predates the defaults field must refuse this blob, not drop the field")
		})
	}
}

// TestHandoverDefaultsRoundTrip covers the wire contract, including the distinction
// between an unset workdir and one set to empty — collapsing those would make the
// incoming envd invent a workdir the outgoing one never had.
func TestHandoverDefaultsRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		in      *execcontext.Defaults
		wantWd  bool
		wantVal string
	}{
		{"user, no workdir", newDefaults("user", nil), false, ""},
		{"user with workdir", newDefaults("user", new("/opt/wd")), true, "/opt/wd"},
		{"user with empty workdir", newDefaults("user", new("")), true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := &upgrade.HandoverState{Defaults: handoverDefaults(tc.in)}
			b, err := proto.Marshal(st)
			require.NoError(t, err)

			got := &upgrade.HandoverState{}
			require.NoError(t, proto.Unmarshal(b, got))
			assert.Equal(t, tc.in.User, got.GetDefaults().GetUser())
			assert.Equal(t, tc.wantWd, got.GetDefaults().GetHasWorkdir())
			assert.Equal(t, tc.wantVal, got.GetDefaults().GetWorkdir())
		})
	}
}

// TestHandoverDefaultsSkipsUndelivered: an envd nothing ever told must not tell the next
// image that it was told. The incoming binary already has the same compiled-in fallback, so
// carrying it would turn "never told" into "told", and the warn that exists to surface the
// loss would go quiet exactly when it matters.
func TestHandoverDefaultsSkipsUndelivered(t *testing.T) {
	t.Parallel()

	d := &execcontext.Defaults{
		User: execcontext.BuiltinDefaultUser, UserDelivered: false, EnvVars: utils.NewEnvVars(),
	}
	assert.Nil(t, handoverDefaults(d))
}

// TestHandoverDefaultsCarriesFieldsIndependently: the two fields are delivered separately, so
// they must be carried separately. An /init can supply a workdir with an empty user, and
// dropping the workdir along with the undelivered user loses it for good — no later memory
// resume re-sends one, and the next blob would carry the absence forward faithfully.
func TestHandoverDefaultsCarriesFieldsIndependently(t *testing.T) {
	t.Parallel()

	t.Run("a workdir survives an undelivered user", func(t *testing.T) {
		t.Parallel()

		d := &execcontext.Defaults{
			User: execcontext.BuiltinDefaultUser, UserDelivered: false,
			Workdir: new("/opt/wd"), EnvVars: utils.NewEnvVars(),
		}
		hd := handoverDefaults(d)
		require.NotNil(t, hd, "a delivered workdir is worth carrying on its own")
		assert.Empty(t, hd.GetUser(), "an undelivered user must not be passed off as delivered")
		assert.True(t, hd.GetHasWorkdir())
		assert.Equal(t, "/opt/wd", hd.GetWorkdir())
	})

	t.Run("neither field means no blob", func(t *testing.T) {
		t.Parallel()

		d := &execcontext.Defaults{
			User: execcontext.BuiltinDefaultUser, UserDelivered: false, EnvVars: utils.NewEnvVars(),
		}
		assert.Nil(t, handoverDefaults(d))
	})
}

// TestApplyHandoverDefaults is the incoming side. The absent-blob case is the one that
// matters most: an outgoing envd predating the field must leave the new image on its
// compiled-in fallback rather than blank the user.
func TestApplyHandoverDefaults(t *testing.T) {
	t.Parallel()

	t.Run("absent defaults leave the fallback in place", func(t *testing.T) {
		t.Parallel()

		d := &execcontext.Defaults{User: execcontext.BuiltinDefaultUser, EnvVars: utils.NewEnvVars()}
		user, workdir := ApplyHandoverDefaults(&upgrade.HandoverState{}, d)
		assert.False(t, user)
		assert.False(t, workdir)
		assert.Equal(t, execcontext.BuiltinDefaultUser, d.User)
		assert.False(t, d.UserDelivered, "an absent blob must not claim delivery")
		assert.Nil(t, d.Workdir)
	})

	t.Run("empty carried user is not applied", func(t *testing.T) {
		t.Parallel()

		d := &execcontext.Defaults{User: execcontext.BuiltinDefaultUser, EnvVars: utils.NewEnvVars()}
		st := &upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{User: ""}}
		user, workdir := ApplyHandoverDefaults(st, d)
		assert.False(t, user)
		assert.False(t, workdir)
		assert.Equal(t, execcontext.BuiltinDefaultUser, d.User)
	})

	t.Run("a carried workdir is applied without a user", func(t *testing.T) {
		t.Parallel()

		d := &execcontext.Defaults{User: execcontext.BuiltinDefaultUser, EnvVars: utils.NewEnvVars()}
		st := &upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{
			Workdir: "/opt/wd", HasWorkdir: true,
		}}
		user, workdir := ApplyHandoverDefaults(st, d)
		assert.False(t, user, "no user was carried, so none was restored")
		assert.True(t, workdir)
		require.NotNil(t, d.Workdir)
		assert.Equal(t, "/opt/wd", *d.Workdir)
		assert.False(t, d.UserDelivered, "a workdir must not imply a delivered identity")
	})

	t.Run("carried user and workdir are restored", func(t *testing.T) {
		t.Parallel()

		d := &execcontext.Defaults{User: execcontext.BuiltinDefaultUser, EnvVars: utils.NewEnvVars()}
		st := &upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{
			User: "user", Workdir: "/opt/wd", HasWorkdir: true,
		}}
		user, workdir := ApplyHandoverDefaults(st, d)
		assert.True(t, user)
		assert.True(t, workdir)
		assert.Equal(t, "user", d.User)
		assert.True(t, d.UserDelivered, "a carried user is a delivered user")
		require.NotNil(t, d.Workdir)
		assert.Equal(t, "/opt/wd", *d.Workdir)
	})

	t.Run("carried user without a workdir does not invent one", func(t *testing.T) {
		t.Parallel()

		d := &execcontext.Defaults{User: execcontext.BuiltinDefaultUser, EnvVars: utils.NewEnvVars()}
		st := &upgrade.HandoverState{Defaults: &upgrade.HandoverDefaults{User: "user"}}
		user, workdir := ApplyHandoverDefaults(st, d)
		assert.True(t, user)
		assert.False(t, workdir)
		assert.Equal(t, "user", d.User)
		assert.Nil(t, d.Workdir)
	})
}
