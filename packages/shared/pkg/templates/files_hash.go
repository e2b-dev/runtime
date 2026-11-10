package templates

import (
	"fmt"
	"regexp"
)

var filesHashRegex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ValidateFilesHash checks that a layer files hash is a lowercase hex SHA-256 digest.
// The hash names the stored layer files, so anything else is rejected before use.
func ValidateFilesHash(hash string) error {
	if !filesHashRegex.MatchString(hash) {
		return fmt.Errorf("invalid files hash: %q", hash)
	}

	return nil
}
