package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type CachePaths struct {
	Paths

	// CacheIdentifier is used to distinguish between each entry in the cache to prevent deleting the cache files when the template cache entry is being closed and a new one is being created.
	CacheIdentifier string

	config Config
}

func (p Paths) Cache(config Config) (CachePaths, error) {
	identifier, err := uuid.NewRandom()
	if err != nil {
		return CachePaths{}, fmt.Errorf("failed to generate identifier: %w", err)
	}

	paths := CachePaths{
		Paths:           p,
		CacheIdentifier: identifier.String(),
		config:          config,
	}

	cacheDir := paths.cacheDir()

	err = os.MkdirAll(cacheDir, os.ModePerm)
	if err != nil {
		return CachePaths{}, fmt.Errorf("failed to create cache dir '%s': %w", cacheDir, err)
	}

	return paths, nil
}

func (c CachePaths) CacheSnapfile() string {
	return filepath.Join(c.cacheDir(), SnapfileName)
}

func (c CachePaths) CacheMetadata() string {
	return filepath.Join(c.cacheDir(), MetadataName)
}

// cacheDir is one flat directory per instance, so Close leaves no parent behind.
func (c CachePaths) cacheDir() string {
	return filepath.Join(c.config.TemplateCacheDir, c.BuildID+"-"+c.CacheIdentifier)
}

func (c CachePaths) Close() error {
	return os.RemoveAll(c.cacheDir())
}
