package redis_utils

import "strings"

const slotCount = 16384

// crc16Table drives CRC16/XMODEM (polynomial 0x1021, initial value 0), the
// checksum Redis Cluster maps keys to slots with.
var crc16Table = func() (table [256]uint16) {
	for i := range table {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}

	return table
}()

// HashTag returns the part of key Redis Cluster hashes: the text between the
// first "{" and the first "}" after it, or the whole key when that text is
// empty or missing.
func HashTag(key string) string {
	if start := strings.IndexByte(key, '{'); start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			return key[start+1 : start+1+end]
		}
	}

	return key
}

// HashSlot returns the cluster slot of key, as CLUSTER KEYSLOT does.
func HashSlot(key string) uint16 {
	tag := HashTag(key)

	var crc uint16
	for i := range len(tag) {
		crc = crc<<8 ^ crc16Table[byte(crc>>8)^tag[i]]
	}

	return crc % slotCount
}

// AllSameSlot reports whether all keys map to one cluster slot, as a
// multi-key command or a script's KEYS must.
func AllSameSlot(keys ...string) bool {
	for _, key := range keys {
		if HashSlot(key) != HashSlot(keys[0]) {
			return false
		}
	}

	return true
}
