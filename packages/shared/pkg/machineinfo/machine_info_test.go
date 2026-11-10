package machineinfo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCompatibleWith(t *testing.T) {
	t.Parallel()

	// olderModel/newerModel correspond to the asymmetric pair registered in
	// compatibleNodeModels (older build may resume on a newer node, not vice versa).
	const (
		olderModel    = IceLakeModel       // Intel Ice Lake (older)
		newerModel    = EmeraldRapidsModel // Intel Emerald Rapids (newer)
		unlistedModel = "79"               // not part of any compatible generation
	)

	tests := []struct {
		name  string
		build MachineInfo
		node  MachineInfo
		want  bool
	}{
		{
			name:  "exact match",
			build: MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			node:  MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			want:  true,
		},
		{
			name:  "architecture mismatch",
			build: MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			node:  MachineInfo{CPUArchitecture: "aarch64", CPUFamily: "6", CPUModel: olderModel},
			want:  false,
		},
		{
			name:  "family mismatch",
			build: MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			node:  MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "23", CPUModel: olderModel},
			want:  false,
		},
		{
			name:  "older build on newer node is compatible",
			build: MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			node:  MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: newerModel},
			want:  true,
		},
		{
			name:  "newer build on older node is incompatible",
			build: MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: newerModel},
			node:  MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			want:  false,
		},
		{
			name:  "unlisted model mismatch",
			build: MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			node:  MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: unlistedModel},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.build.IsCompatibleWith(tt.node))
		})
	}
}

func TestIsExactMatch(t *testing.T) {
	t.Parallel()

	const (
		olderModel = IceLakeModel       // Intel Ice Lake (older)
		newerModel = EmeraldRapidsModel // Intel Emerald Rapids (newer)
	)

	tests := []struct {
		name string
		a    MachineInfo
		b    MachineInfo
		want bool
	}{
		{
			name: "exact match",
			a:    MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			b:    MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			want: true,
		},
		{
			name: "cross-generation is not an exact match",
			a:    MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			b:    MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: newerModel},
			want: false,
		},
		{
			name: "architecture mismatch",
			a:    MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			b:    MachineInfo{CPUArchitecture: "aarch64", CPUFamily: "6", CPUModel: olderModel},
			want: false,
		},
		{
			name: "family mismatch",
			a:    MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: olderModel},
			b:    MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "23", CPUModel: olderModel},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.a.IsExactMatch(tt.b))
		})
	}
}

func TestIsCompatibleWithExtra(t *testing.T) {
	t.Parallel()

	info := func(model string) MachineInfo {
		return MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: model}
	}
	extra := map[string][]string{IceLakeModel: {GraniteRapidsModel}}

	tests := []struct {
		name  string
		build MachineInfo
		node  MachineInfo
		extra map[string][]string
		want  bool
	}{
		{"listed extra pair", info(IceLakeModel), info(GraniteRapidsModel), extra, true},
		{"no extras", info(IceLakeModel), info(GraniteRapidsModel), nil, false},
		{"reverse is not implied", info(GraniteRapidsModel), info(IceLakeModel), extra, false},
		{"granite to emerald is not implied", info(GraniteRapidsModel), info(EmeraldRapidsModel), extra, false},
		{"hardcoded pair kept", info(IceLakeModel), info(EmeraldRapidsModel), extra, true},
		{"unlisted node model", info(IceLakeModel), info("79"), extra, false},
		{
			"architecture mismatch",
			info(IceLakeModel),
			MachineInfo{CPUArchitecture: "aarch64", CPUFamily: "6", CPUModel: GraniteRapidsModel},
			extra, false,
		},
		{
			"family mismatch",
			info(IceLakeModel),
			MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "23", CPUModel: GraniteRapidsModel},
			extra, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.build.IsCompatibleWithExtra(tt.node, tt.extra))
		})
	}
}
