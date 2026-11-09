// Package id converts current-format sandbox IDs to internal actor UUIDs, and
// back. Public APIs keep sandbox IDs: "i" followed by 20 lowercase base36
// characters.
package id

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/google/uuid"
)

// Sandbox ID UUID format, scheme revision 1. Never change this layout; add a
// new revision instead.
//
// A sandbox ID "i" + 20 base36 digits (0-9, a-z) is a 104-bit payload P with
// P < 36^20. It is packed into an RFC 9562 UUIDv8, bit 127 is the most
// significant bit:
//
//	127-112  magic 0xe2b5
//	111-110  scheme revision, 0b01
//	109-80   P bits 103-74
//	 79-76   UUID version, 0b1000
//	 75-64   P bits 73-62
//	 63-62   RFC 9562 variant, 0b10
//	 61-0    P bits 61-0
//
// The magic and revision mark the format only. They do not prove who
// generated the UUID, and an unrelated UUIDv8 can carry the same bits.
const (
	sandboxIDPrefix    = "i"
	sandboxIDDigits    = 20
	sandboxIDLen       = len(sandboxIDPrefix) + sandboxIDDigits
	sandboxUUIDMagic   = 0xe2b5
	sandboxUUIDScheme  = 0b01
	uuidVersion8       = 0b1000
	uuidVariantRFC9562 = 0b10

	// 36^20 split into the high 40 and low 64 payload bits.
	sandboxPayloadLimitHi = 0xa8b8b45229
	sandboxPayloadLimitLo = 0x1fe8210000000000

	low62Mask = 1<<62 - 1
)

func SandboxIDToUUID(sandboxID string) (uuid.UUID, error) {
	if len(sandboxID) != sandboxIDLen || sandboxID[:len(sandboxIDPrefix)] != sandboxIDPrefix {
		return uuid.Nil, fmt.Errorf("invalid sandbox ID %q: want %q followed by %d lowercase base36 characters", sandboxID, sandboxIDPrefix, sandboxIDDigits)
	}

	var hi, lo uint64
	for i := len(sandboxIDPrefix); i < sandboxIDLen; i++ {
		d, ok := base36Digit(sandboxID[i])
		if !ok {
			return uuid.Nil, fmt.Errorf("invalid sandbox ID %q: character %q is not lowercase base36", sandboxID, sandboxID[i])
		}

		carry, mulLo := bits.Mul64(lo, 36)
		hi = hi*36 + carry
		lo, carry = bits.Add64(mulLo, d, 0)
		hi += carry
	}

	upper := uint64(sandboxUUIDMagic)<<48 |
		uint64(sandboxUUIDScheme)<<46 |
		(hi>>10)<<16 |
		uint64(uuidVersion8)<<12 |
		(hi<<2|lo>>62)&0xfff
	lower := uint64(uuidVariantRFC9562)<<62 | lo&low62Mask

	var u uuid.UUID
	binary.BigEndian.PutUint64(u[:8], upper)
	binary.BigEndian.PutUint64(u[8:], lower)

	return u, nil
}

// UUIDToSandboxID returns the sandbox ID encoded in u. It returns false when u
// does not use the sandbox UUID format, scheme revision 1. Such a UUID is not
// invalid; this helper does not decode it.
func UUIDToSandboxID(u uuid.UUID) (string, bool) {
	upper := binary.BigEndian.Uint64(u[:8])
	lower := binary.BigEndian.Uint64(u[8:])

	if upper>>48 != sandboxUUIDMagic ||
		upper>>46&0b11 != sandboxUUIDScheme ||
		upper>>12&0xf != uuidVersion8 ||
		lower>>62 != uuidVariantRFC9562 {
		return "", false
	}

	hi := (upper>>16&(1<<30-1))<<10 | (upper&0xfff)>>2
	lo := (upper&0b11)<<62 | lower&low62Mask

	if hi > sandboxPayloadLimitHi || (hi == sandboxPayloadLimitHi && lo >= sandboxPayloadLimitLo) {
		return "", false
	}

	var out [sandboxIDLen]byte
	copy(out[:], sandboxIDPrefix)
	for i := sandboxIDLen - 1; i >= len(sandboxIDPrefix); i-- {
		var r uint64
		hi, r = bits.Div64(0, hi, 36)
		lo, r = bits.Div64(r, lo, 36)
		out[i] = base36Alphabet[r]
	}

	return string(out[:]), true
}

const base36Alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

func base36Digit(c byte) (uint64, bool) {
	switch {
	case c >= '0' && c <= '9':
		return uint64(c - '0'), true
	case c >= 'a' && c <= 'z':
		return uint64(c-'a') + 10, true
	default:
		return 0, false
	}
}
