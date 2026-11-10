//go:build unix

package filesystem

// resolveListRoot resolves the directory a listing starts from.
func resolveListRoot(path string) (string, error) {
	return followSymlink(path)
}
