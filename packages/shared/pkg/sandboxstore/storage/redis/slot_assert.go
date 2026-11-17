package redis

import (
	"fmt"
	"strings"
	"sync/atomic"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// slotAssertions is set by the package's TestMain and stays false in
// production.
var slotAssertions atomic.Bool

// assertSameSlot panics, with slot assertions on, on keys that span hash
// slots, naming each key's slot: a standalone server runs such a command, a
// cluster refuses it.
func assertSameSlot(name string, keys []string) {
	if !slotAssertions.Load() || redis_utils.AllSameSlot(keys...) {
		return
	}
	slots := make([]string, 0, len(keys))
	for _, key := range keys {
		slots = append(slots, fmt.Sprintf("%q in slot %d", key, redis_utils.HashSlot(key)))
	}
	panic(fmt.Sprintf("%s: keys span hash slots: %s", name, strings.Join(slots, ", ")))
}
