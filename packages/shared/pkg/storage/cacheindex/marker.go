package cacheindex

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// MarkerName is the file at the cache root that holds the cache ID. It is a
// regular file, and the cleaner only treats directories as builds.
const MarkerName = ".cache-index-id"

// ReadID returns the cache ID stored in root's marker file.
func ReadID(root string) (string, error) {
	path := filepath.Join(root, MarkerName)
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(b))
	if _, err := uuid.Parse(id); err != nil {
		return "", fmt.Errorf("cache index marker %s: %w", path, err)
	}

	return id, nil
}

// EnsureID returns the cache ID in root's marker, creating the marker with a new
// ID when it is missing. When several callers race, one ID wins: the marker is
// published with link(2), which fails if the file exists, and every caller then
// reads back what was published.
func EnsureID(root string) (string, error) {
	id, err := ReadID(root)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}

	tmp, err := os.CreateTemp(root, MarkerName+".tmp.*")
	if err != nil {
		return "", fmt.Errorf("create cache index marker: %w", err)
	}
	defer os.Remove(tmp.Name())

	_, err = tmp.WriteString(uuid.NewString() + "\n")
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", fmt.Errorf("write cache index marker: %w", err)
	}

	if err := os.Link(tmp.Name(), filepath.Join(root, MarkerName)); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", fmt.Errorf("publish cache index marker: %w", err)
	}

	return ReadID(root)
}
