package metadata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Balloon is lineage state: the copy-constructors carry it, the JSON round trip
// keeps it, and a template written before the field existed reads nil.
func TestBalloonCarriedAndOptional(t *testing.T) {
	t.Parallel()
	tmpl := Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "b1"}}.WithBalloon(false, true)
	require.NotNil(t, tmpl.Balloon)

	same := tmpl.SameVersionTemplate(TemplateMetadata{BuildID: "b2"})
	assert.Equal(t, &Balloon{Hinting: true}, same.Balloon, "SameVersionTemplate carries it across a pause")
	assert.Equal(t, &Balloon{Hinting: true}, tmpl.WithPrefetch(nil).Balloon)

	raw, err := json.Marshal(tmpl)
	require.NoError(t, err)
	var back Template
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.Equal(t, &Balloon{Hinting: true}, back.Balloon)

	var legacy Template
	require.NoError(t, json.Unmarshal([]byte(`{"version":2,"template":{"build_id":"b0"}}`), &legacy))
	assert.Nil(t, legacy.Balloon, "an older template carries no mode: it is read from the device")
}
