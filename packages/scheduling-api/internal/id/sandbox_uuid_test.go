package id

import (
	"math/big"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Golden vectors were computed outside this package with Python's int(s, 36)
// and uuid.UUID(int=...); they pin the wire format.
var sandboxUUIDGolden = []struct {
	name      string
	sandboxID string
	uuid      string
}{
	{"zero", "i00000000000000000000", "e2b54000-0000-8000-8000-000000000000"},
	{"one", "i00000000000000000001", "e2b54000-0000-8000-8000-000000000001"},
	{"leading zeros", "i0000000000abcdefghij", "e2b54000-0000-8000-8003-b8c9b052f3ab"},
	{"representative", "i5f3k9q2m8x1c7v4b6n0p", "e2b54659-8cda-8bc6-abe3-090364ffe509"},
	{"max", "izzzzzzzzzzzzzzzzzzzz", "e2b56a2e-2d14-88a4-9fe8-20ffffffffff"},
}

func TestSandboxIDToUUIDGolden(t *testing.T) {
	t.Parallel()

	for _, tc := range sandboxUUIDGolden {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := SandboxIDToUUID(tc.sandboxID)
			require.NoError(t, err)
			assert.Equal(t, tc.uuid, got.String())
			assert.Equal(t, uuid.Version(8), got.Version())
			assert.Equal(t, uuid.RFC4122, got.Variant())
			assert.Equal(t, layoutWithBigInt(t, tc.sandboxID), got)

			back, ok := UUIDToSandboxID(uuid.MustParse(tc.uuid))
			require.True(t, ok)
			assert.Equal(t, tc.sandboxID, back)
		})
	}
}

// layoutWithBigInt builds the sandbox UUID from the documented bit layout
// with math/big, independently of the uint64 packing under test.
func layoutWithBigInt(t *testing.T, sandboxID string) uuid.UUID {
	t.Helper()

	p, ok := new(big.Int).SetString(sandboxID[1:], 36)
	require.True(t, ok)

	field := func(v *big.Int, from, width uint) *big.Int {
		mask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), width), big.NewInt(1))

		return new(big.Int).And(new(big.Int).Rsh(v, from), mask)
	}

	v := new(big.Int)
	place := func(x *big.Int, at uint) { v.Or(v, new(big.Int).Lsh(x, at)) }
	place(big.NewInt(0xe2b5), 112)
	place(big.NewInt(0b01), 110)
	place(field(p, 74, 30), 80)
	place(big.NewInt(0b1000), 76)
	place(field(p, 62, 12), 64)
	place(big.NewInt(0b10), 62)
	place(field(p, 0, 62), 0)

	var u uuid.UUID
	v.FillBytes(u[:])

	return u
}

func TestSandboxPayloadLimit(t *testing.T) {
	t.Parallel()

	limit := new(big.Int).Exp(big.NewInt(36), big.NewInt(sandboxIDDigits), nil)
	got := new(big.Int).Lsh(new(big.Int).SetUint64(sandboxPayloadLimitHi), 64)
	got.Or(got, new(big.Int).SetUint64(sandboxPayloadLimitLo))

	assert.Equal(t, 0, limit.Cmp(got))
	assert.LessOrEqual(t, limit.BitLen(), 104)
}

func TestSandboxIDUUIDRoundTripRandom(t *testing.T) {
	t.Parallel()

	for range 10_000 {
		var digits [sandboxIDDigits]byte
		for i := range digits {
			digits[i] = base36Alphabet[rand.IntN(len(base36Alphabet))]
		}
		sandboxID := sandboxIDPrefix + string(digits[:])

		u, err := SandboxIDToUUID(sandboxID)
		require.NoError(t, err)
		require.Equal(t, layoutWithBigInt(t, sandboxID), u)

		back, ok := UUIDToSandboxID(u)
		require.True(t, ok)
		require.Equal(t, sandboxID, back)
	}
}

func TestSandboxIDToUUIDRejectsInvalid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		sandboxID string
	}{
		{"empty", ""},
		{"prefix only", "i"},
		{"too short", "i0000000000000000000"},
		{"too long", "i000000000000000000000"},
		{"no prefix", "000000000000000000000"},
		{"build prefix", "b00000000000000000000"},
		{"uppercase prefix", "I00000000000000000000"},
		{"uppercase digit", "i0000000000000000000A"},
		{"hyphen", "i000000000-0000000000"},
		{"underscore", "i0000000000000000000_"},
		{"non ascii", "i000000000000000000\u00e9"},
		{"composite with client ID", "i00000000000000000000-abcdefgh"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := SandboxIDToUUID(tc.sandboxID)
			require.Error(t, err)
		})
	}
}

func TestUUIDToSandboxIDRejectsOtherUUIDs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		uuid string
	}{
		{"nil", "00000000-0000-0000-0000-000000000000"},
		{"max", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
		{"v4", "f47ac10b-58cc-4372-a567-0e02b2c3d479"},
		{"v7", "019fa519-bf79-724d-8811-a2bfda9755fa"},
		{"unmarked v8", "0123abcd-0000-8000-8000-000000000000"},
		{"other magic", "e2b44000-0000-8000-8000-000000000000"},
		{"revision 0", "e2b50000-0000-8000-8000-000000000000"},
		{"revision 2", "e2b58000-0000-8000-8000-000000000000"},
		{"revision 3", "e2b5c000-0000-8000-8000-000000000000"},
		{"marked but version 4", "e2b54000-0000-4000-8000-000000000000"},
		{"marked but version 7", "e2b54000-0000-7000-8000-000000000000"},
		{"marked but NCS variant", "e2b54000-0000-8000-0000-000000000000"},
		{"marked but Microsoft variant", "e2b54000-0000-8000-c000-000000000000"},
		{"payload 36^20", "e2b56a2e-2d14-88a4-9fe8-210000000000"},
		{"payload 2^104-1", "e2b57fff-ffff-8fff-bfff-ffffffffffff"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := UUIDToSandboxID(uuid.MustParse(tc.uuid))
			assert.False(t, ok)
			assert.Empty(t, got)
		})
	}
}

func TestUUIDToSandboxIDRejectsRandomUUIDs(t *testing.T) {
	t.Parallel()

	for range 10_000 {
		u4 := uuid.New()
		_, ok := UUIDToSandboxID(u4)
		require.False(t, ok, u4.String())

		u7 := uuid.Must(uuid.NewV7())
		_, ok = UUIDToSandboxID(u7)
		require.False(t, ok, u7.String())
	}
}

func FuzzUUIDToSandboxID(f *testing.F) {
	for _, tc := range sandboxUUIDGolden {
		u := uuid.MustParse(tc.uuid)
		f.Add(u[:])
	}
	outOfRange := uuid.MustParse("e2b56a2e-2d14-88a4-9fe8-210000000000")
	f.Add(outOfRange[:])

	f.Fuzz(func(t *testing.T, b []byte) {
		var u uuid.UUID
		copy(u[:], b)

		sandboxID, ok := UUIDToSandboxID(u)
		if !ok {
			return
		}

		require.Len(t, sandboxID, sandboxIDLen)
		require.True(t, strings.HasPrefix(sandboxID, sandboxIDPrefix))

		back, err := SandboxIDToUUID(sandboxID)
		require.NoError(t, err)
		require.Equal(t, u, back)
	})
}

func FuzzSandboxIDToUUID(f *testing.F) {
	for _, tc := range sandboxUUIDGolden {
		f.Add(tc.sandboxID)
	}
	f.Add("I00000000000000000000")

	f.Fuzz(func(t *testing.T, sandboxID string) {
		u, err := SandboxIDToUUID(sandboxID)
		if err != nil {
			return
		}

		back, ok := UUIDToSandboxID(u)
		require.True(t, ok)
		require.Equal(t, sandboxID, back)
	})
}
