//go:build unix

package process

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
)

func TestSchemaFor_LocalhostForwardingOff(t *testing.T) {
	t.Parallel()

	t.Run("off stamps the newest schema", func(t *testing.T) {
		t.Parallel()

		st := &upgrade.HandoverState{
			FromVer:                "0.10.2",
			GuestFrozenCgroups:     []string{"customer/c1"},
			Defaults:               &upgrade.HandoverDefaults{User: "user"},
			LocalhostForwardingOff: true,
		}
		assert.Equal(t, uint32(handoverSchemaLocalhostForwarding), schemaFor(st))
	})

	t.Run("on costs no refusal", func(t *testing.T) {
		t.Parallel()

		st := &upgrade.HandoverState{FromVer: "0.10.2"}
		assert.Equal(t, uint32(handoverSchemaBase), schemaFor(st))
	})
}

//nolint:paralleltest // mutates the package-global HandoverPath; must run serially
func TestResumeFromHandover_CarriesLocalhostForwardingOff(t *testing.T) {
	orig := HandoverPath
	t.Cleanup(func() { HandoverPath = orig })
	HandoverPath = filepath.Join(t.TempDir(), "handover.pb")

	st := &upgrade.HandoverState{FromVer: "0.10.2", LocalhostForwardingOff: true}
	st.Schema = schemaFor(st)
	require.NoError(t, os.WriteFile(HandoverPath, mustMarshalHandover(t, st), 0o600))

	s := newHandoverTestService(t, &spyCgroupManager{})
	res, err := s.ResumeFromHandover(nil)
	require.NoError(t, err)
	require.NotNil(t, res.LocalhostForwardingOff)
	assert.True(t, *res.LocalhostForwardingOff)
}

// A handover that was not decoded carries no setting, so the forwarder keeps the
// boot's record rather than reading it as on.
//
//nolint:paralleltest // mutates the package-global HandoverPath; must run serially
func TestResumeFromHandover_WithoutABlobCarriesNoLocalhostForwarding(t *testing.T) {
	orig := HandoverPath
	t.Cleanup(func() { HandoverPath = orig })
	HandoverPath = filepath.Join(t.TempDir(), "handover.pb")

	s := newHandoverTestService(t, &spyCgroupManager{})
	res, err := s.ResumeFromHandover(nil)
	require.NoError(t, err)
	assert.Nil(t, res.LocalhostForwardingOff)

	require.NoError(t, os.WriteFile(HandoverPath, []byte("not a handover"), 0o600))
	res, err = s.ResumeFromHandover(nil)
	require.Error(t, err)
	assert.Nil(t, res.LocalhostForwardingOff)
}

func TestHandoverState_CarriesLocalhostForwardingOff(t *testing.T) {
	t.Parallel()

	s := newHandoverTestService(t, &spyCgroupManager{})

	st := s.handoverState("0.10.2", nil, nil, nil, true, nil)

	assert.True(t, st.GetLocalhostForwardingOff())
	assert.Equal(t, uint32(handoverSchemaLocalhostForwarding), st.GetSchema())
}
