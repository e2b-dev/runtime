//go:build linux

package server

import "maps"

// mergeTemplateEnvVars layers env vars for a sandbox start: the template's
// Docker image ENV vars are the base, and user-provided env vars (from the SDK
// create call) override them on conflict. Per-command env vars from
// process.start override both, applied later in envd. A nil/empty template map
// returns userVars unchanged (preserving nil), so the common no-image-ENV case
// adds nothing.
func mergeTemplateEnvVars(templateVars, userVars map[string]string) map[string]string {
	if len(templateVars) == 0 {
		return userVars
	}

	merged := maps.Clone(templateVars)
	maps.Copy(merged, userVars)

	return merged
}
