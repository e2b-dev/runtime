package permissions

import (
	"errors"
	"fmt"
	"os/user"
	"path/filepath"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
)

func expand(path, homedir string) (string, error) {
	if len(path) == 0 {
		return path, nil
	}

	if path[0] != '~' {
		return path, nil
	}

	if len(path) > 1 && path[1] != '/' && path[1] != '\\' {
		return "", errors.New("cannot expand user-specific home dir")
	}

	return filepath.Join(homedir, path[1:]), nil
}

func ExpandAndResolve(path string, user *user.User, defaultPath *string) (string, error) {
	path = execcontext.ResolveDefaultWorkdir(path, defaultPath)

	path, err := expand(path, user.HomeDir)
	if err != nil {
		return "", fmt.Errorf("failed to expand path '%s' for user '%s': %w", path, user.Username, err)
	}

	if filepath.IsAbs(path) {
		return path, nil
	}

	if err := refuseAmbiguousRelative(path); err != nil {
		return "", err
	}

	if rootedButNotAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("failed to resolve rooted path '%s': %w", path, err)
		}

		return abs, nil
	}

	// The filepath.Abs can correctly resolve paths like /home/user/../file
	path = filepath.Join(user.HomeDir, path)

	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path '%s' for user '%s' with home dir '%s': %w", path, user.Username, user.HomeDir, err)
	}

	return abs, nil
}
