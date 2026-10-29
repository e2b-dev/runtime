package api_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// TestSpecAutoPauseDefaultMatchesConstant pins the OpenAPI spec's declared
// default for `autoPause` to the Go constant the create handler actually
// applies (sandboxtypes.AutoPauseDefault). The two are separate copies of the
// same default — the spec value is documentation (the generated field is
// `*bool omitempty`, so the default is never applied at decode time), while the
// constant is what DerefOrDefault uses when the request omits autoPause. This
// is the "currently kill" default the SDKs document and resolve local
// validation against; a drift between the spec and the constant would make the
// published contract lie about what an omitted autoPause does. Assert they
// agree so the spec stays the single documented source of the default.
//
// An external test package (api_test) is required: sandboxtypes imports the api
// package, so an in-package test importing sandboxtypes would be an import cycle.
func TestSpecAutoPauseDefaultMatchesConstant(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	require.NotNil(t, spec.Components)

	// Both create-body schemas carry autoPause; both must agree with the constant.
	for _, schemaName := range []string{"NewSandbox", "NewSandboxV2"} {
		t.Run(schemaName, func(t *testing.T) {
			t.Parallel()

			ref, ok := spec.Components.Schemas[schemaName]
			require.True(t, ok, "schema %q not found in spec", schemaName)
			require.NotNil(t, ref.Value, "schema %q has nil value", schemaName)

			prop, ok := ref.Value.Properties["autoPause"]
			require.True(t, ok, "%q is missing the autoPause property", schemaName)
			require.NotNil(t, prop.Value, "%q autoPause has nil value", schemaName)

			require.NotNil(t, prop.Value.Default,
				"%q autoPause must declare a default so the omitted-value behavior is documented", schemaName)

			defaultBool, ok := prop.Value.Default.(bool)
			require.Truef(t, ok, "%q autoPause default must be a boolean, got %T", schemaName, prop.Value.Default)

			require.Equalf(t, sandboxtypes.AutoPauseDefault, defaultBool,
				"%q autoPause default in the spec (%v) does not match sandboxtypes.AutoPauseDefault (%v); "+
					"the spec documents the default the SDKs rely on, so they must stay in sync",
				schemaName, defaultBool, sandboxtypes.AutoPauseDefault)
		})
	}
}
