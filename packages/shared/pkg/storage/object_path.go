package storage

import (
	"errors"
	"fmt"
	"io/fs"
)

var errInvalidObjectPath = errors.New("invalid object path")

// validateObjectPath accepts only paths io/fs considers valid, minus its root
// ".": unrooted, slash-separated UTF-8 with no empty, "." or ".." element.
// Object stores take a path literally while the filesystem provider and the
// local cache resolve it against a directory, so a path in any other form could
// name a different object per provider, or one outside the provider's root.
func validateObjectPath(p string) error {
	if p == "." || !fs.ValidPath(p) {
		return fmt.Errorf("%w: %q", errInvalidObjectPath, p)
	}

	return nil
}
