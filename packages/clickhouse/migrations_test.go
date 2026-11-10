package clickhouse_test

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/clickhouse"
)

// The embed pattern names *.sql directly under migrations/, which is also the
// whole set a goose provider over the directory applies. A file the pattern
// missed would be applied by the migrator and skipped by every binary; this
// keeps the two sets equal.
func TestMigrationsEmbedsTheDirectory(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("migrations", "*.sql"))
	require.NoError(t, err)
	onDisk := make([]string, 0, len(paths))
	for _, path := range paths {
		onDisk = append(onDisk, filepath.Base(path))
	}

	embedded, err := fs.Glob(clickhouse.Migrations(), "*")
	require.NoError(t, err)

	require.NotEmpty(t, onDisk)
	require.ElementsMatch(t, onDisk, embedded)
}
