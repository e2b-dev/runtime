package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Whether a memory image lacks an MMDS store, and the token its envd then
// holds, follow that memory through every copy-constructor that keeps it and
// survive the file; BasedOn drops them, since its build cold-boots with a store.
func TestCopyConstructorsCarryEnvdToken(t *testing.T) {
	t.Parallel()

	base := Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "build"}}.WithEnvdToken(true, "held")
	next := TemplateMetadata{BuildID: "child"}

	carried := map[string]func(Template) Template{
		"SameVersionTemplate":    func(t Template) Template { return t.SameVersionTemplate(next) },
		"NewVersionTemplate":     func(t Template) Template { return t.NewVersionTemplate(next) },
		"WithPrefetch":           func(t Template) Template { return t.WithPrefetch(nil) },
		"WithBalloon":            func(t Template) Template { return t.WithBalloon(true, false) },
		"WithDeviceNames":        func(t Template) Template { return t.WithDeviceNames("root", "net0", "guest.disk") },
		"WithVcpuCount":          func(t Template) Template { return t.WithVcpuCount(2) },
		"WithRunningEnvdVersion": func(t Template) Template { return t.WithRunningEnvdVersion("0.9.0") },
		"file round trip": func(t Template) Template {
			r, err := serialize(t)
			if err != nil {
				panic(err)
			}
			got, err := deserialize(r)
			if err != nil {
				panic(err)
			}

			return got
		},
	}
	for name, fn := range carried {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := fn(base)
			assert.True(t, got.NoMmdsStore)
			assert.Equal(t, "held", got.EnvdAccessTokenHash)
		})
	}

	child := base.BasedOn(FromTemplate{Alias: "a", BuildID: "build"})
	assert.False(t, child.NoMmdsStore, "a build from a template cold-boots with a store")
	assert.Empty(t, child.EnvdAccessTokenHash)

	stored := base.WithEnvdToken(false, "held")
	assert.False(t, stored.NoMmdsStore)
	assert.Empty(t, stored.EnvdAccessTokenHash, "a microVM with a store records no token")
}
