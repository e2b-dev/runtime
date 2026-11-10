//go:build unix

package permissions

import "os"

// Chown gives path to the user, as os.Chown.
func Chown(path string, uid, gid int) error { return os.Chown(path, uid, gid) }

// ChownFile gives an open file to the user, as (*os.File).Chown.
func ChownFile(f *os.File, uid, gid int) error { return f.Chown(uid, gid) }
