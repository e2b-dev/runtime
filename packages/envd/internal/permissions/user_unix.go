//go:build unix

package permissions

import (
	"fmt"
	"os/user"
)

// GetUser resolves a request's username against the guest's account database.
func GetUser(username string) (*user.User, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return nil, fmt.Errorf("error looking up user '%s': %w", username, err)
	}

	return u, nil
}
