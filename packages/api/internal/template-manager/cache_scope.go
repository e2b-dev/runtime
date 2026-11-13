package template_manager

import (
	"os"
	"sync"

	"github.com/google/uuid"
)

// templateCacheScopeEnv, when set to a non-empty value, overrides the per-team
// layer cache scope with a single shared scope so that identical content-
// addressed layers (and the buildID-keyed memfile/rootfs they resolve to) are
// stored and reused across teams instead of once per team.
//
// Default (unset): the scope is the teamID, preserving upstream per-team cache
// isolation. Only set this in a single trusted organization / self-hosted
// deployment where cross-team cache sharing is acceptable: a shared scope lets
// any team hit (and thus prove the existence of, and reuse) another team's
// cached layers.
const templateCacheScopeEnv = "TEMPLATE_CACHE_SCOPE"

var (
	cacheScopeOnce   sync.Once
	cacheScopeShared string
)

// cacheScopeFor returns the layer cache scope for a build owned by teamID.
// If TEMPLATE_CACHE_SCOPE is set it returns that shared scope for every team
// (cross-team dedup); otherwise it returns the teamID (per-team isolation).
func cacheScopeFor(teamID uuid.UUID) string {
	cacheScopeOnce.Do(func() {
		cacheScopeShared = os.Getenv(templateCacheScopeEnv)
	})
	if cacheScopeShared != "" {
		return cacheScopeShared
	}
	return teamID.String()
}
