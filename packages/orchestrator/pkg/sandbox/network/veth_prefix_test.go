//go:build linux

package network

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateVethPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{name: "default ok", prefix: "e2b-", wantErr: false},
		{name: "legacy ok", prefix: "veth-", wantErr: false},
		{name: "custom ok", prefix: "sbx-", wantErr: false},
		{name: "underscore ok", prefix: "sbx_", wantErr: false},
		{name: "dot ok", prefix: "sbx.", wantErr: false},
		{name: "max length 10 ok", prefix: "012345678-", wantErr: false},
		{name: "allowed punctuation", prefix: "a_b.c-", wantErr: false},
		{name: "empty prefix", prefix: "", wantErr: true},
		{name: "too long 11 chars", prefix: "0123456789-", wantErr: true},
		{name: "invalid chars space", prefix: "e2b -", wantErr: true},
		{name: "invalid chars colon", prefix: "e2b:1", wantErr: true},
		{name: "invalid chars slash", prefix: "e2b/1", wantErr: true},
		{name: "missing separator e2b", prefix: "e2b", wantErr: true},
		{name: "missing separator e2brt", prefix: "e2brt", wantErr: true},
		{name: "alphanumeric end rejected", prefix: "0123456789", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateVethPrefix(tt.prefix)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSlot_VethName(t *testing.T) {
	t.Parallel()

	// Default prefix when unset
	sDefault := &Slot{Idx: 42} //nolint:exhaustruct_v5 // only slot naming is tested
	assert.Equal(t, "e2b-42", sDefault.VethName())
	assert.Equal(t, "e2b-", sDefault.VethPrefix())

	// Configured custom prefix
	sCustom := &Slot{ //nolint:exhaustruct_v5 // only slot naming is tested
		Idx:    101,
		config: Config{VethPrefix: "sbx-"}, //nolint:exhaustruct_v5 // only the prefix matters
	}
	assert.Equal(t, "sbx-101", sCustom.VethName())
	assert.Equal(t, "sbx-", sCustom.VethPrefix())
}

func TestSlot_TeardownVethNames(t *testing.T) {
	t.Parallel()

	// Before create / after NewSlot: [configured, legacy]
	slotNew, err := NewSlot("slot-new", 5, Config{VethPrefix: "e2b-"}, nil) //nolint:exhaustruct_v5 // only slot naming and teardown are tested
	require.NoError(t, err)
	assert.Equal(t, []string{"e2b-5", "veth-5"}, slotNew.TeardownVethNames())

	// Configured differs from legacy, not marked created: [configured, legacy]
	sCustom := &Slot{ //nolint:exhaustruct_v5 // only slot naming is tested
		Idx:    5,
		config: Config{VethPrefix: "e2b-"}, //nolint:exhaustruct_v5 // only the prefix matters
	}
	assert.Equal(t, []string{"e2b-5", "veth-5"}, sCustom.TeardownVethNames())

	// After create is started but not finished (marked false): [configured, legacy]
	sCustom.MarkNetworkCreated(false)
	assert.Equal(t, []string{"e2b-5", "veth-5"}, sCustom.TeardownVethNames())

	// After create fully succeeds (marked true): [configured]
	sCustom.MarkNetworkCreated(true)
	assert.Equal(t, []string{"e2b-5"}, sCustom.TeardownVethNames())

	// Configured is legacy: [legacy] deduped whether marked created or not
	sLegacy := &Slot{ //nolint:exhaustruct_v5 // only slot naming is tested
		Idx:    5,
		config: Config{VethPrefix: "veth-"}, //nolint:exhaustruct_v5 // only the prefix matters
	}
	assert.Equal(t, []string{"veth-5"}, sLegacy.TeardownVethNames())

	sLegacy.MarkNetworkCreated(true)
	assert.Equal(t, []string{"veth-5"}, sLegacy.TeardownVethNames())
}
