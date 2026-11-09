package permissions

import (
	"errors"
	"fmt"
	"os"
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

// EnsureDirs creates every missing directory on the way to path and gives each
// one it creates to uid and gid through Chown, which decides per platform what
// ownership means.
func EnsureDirs(path string, uid, gid int) error {
	for _, subpath := range getSubpaths(path) {
		info, err := os.Stat(subpath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat directory: %w", err)
		}

		if err != nil && os.IsNotExist(err) {
			err = os.Mkdir(subpath, 0o755)
			if err != nil && !os.IsExist(err) {
				return fmt.Errorf("failed to create directory: %w", err)
			}

			// Chown even if another process created the directory (os.IsExist),
			// since it may not have called Chown yet. Chown is idempotent.
			err = Chown(subpath, uid, gid)
			if err != nil {
				return fmt.Errorf("failed to chown directory: %w", err)
			}

			continue
		}

		if !info.IsDir() {
			return fmt.Errorf("path is a file: %s", subpath)
		}
	}

	return nil
}
