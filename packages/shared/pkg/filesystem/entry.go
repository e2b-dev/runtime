package filesystem

import (
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// statTimes holds platform-independent file timestamps and ownership info
// extracted from a *syscall.Stat_t.
type statTimes struct {
	atime time.Time
	ctime time.Time
	mtime time.Time
	uid   uint32
	gid   uint32
}

// GetEntryFromPath looks up the entry at path. When includeMetadata is true it
// also reads the file's user-defined xattr metadata; callers that don't surface
// metadata (e.g. the orchestrator volume service) pass false to skip the extra
// path-based xattr syscalls. The metadata read resolves path in the caller's
// mount namespace, so it's only correct when path is interpreted there.
func GetEntryFromPath(path string, includeMetadata bool) (EntryInfo, error) {
	fileInfo, err := os.Lstat(path)
	if err != nil {
		return EntryInfo{}, err
	}

	entry := GetEntryInfo(path, fileInfo)
	if includeMetadata {
		// Metadata is best-effort: a read failure shouldn't fail the entry
		// lookup (Size/Mode/times are still valid), and this helper has no
		// logger to report it through. Callers that need the error (e.g. the
		// upload handler) call ReadMetadata directly.
		entry.Metadata, _ = ReadMetadata(path)
	}

	return entry, nil
}

// SymlinkResolver resolves the symlink at path to its target and the target's
// file info. When the target cannot be stat'ed it returns the best target path
// it has and a nil info.
type SymlinkResolver func(path string) (target string, targetInfo os.FileInfo)

// GetEntryInfo builds an EntryInfo purely from fileInfo. It does not read xattr
// metadata; callers that surface metadata use GetEntryFromPath with
// includeMetadata set. A symlink is resolved against the host filesystem;
// callers that interpret paths elsewhere use GetEntryInfoWithResolver.
func GetEntryInfo(path string, fileInfo os.FileInfo) EntryInfo {
	return GetEntryInfoWithResolver(path, fileInfo, resolveHostSymlink)
}

// GetEntryInfoWithResolver is GetEntryInfo with the symlink resolution
// supplied by the caller. resolve is only called when fileInfo is a symlink.
func GetEntryInfoWithResolver(path string, fileInfo os.FileInfo, resolve SymlinkResolver) EntryInfo {
	fileMode := fileInfo.Mode()

	var symlinkTarget *string
	var targetInfo os.FileInfo
	if fileMode&os.ModeSymlink != 0 {
		// If we can't resolve the symlink target, we won't set the target
		target, info := resolve(path)
		symlinkTarget = &target
		targetInfo = info
	}

	var entryType FileType
	var mode os.FileMode

	switch {
	case symlinkTarget == nil:
		entryType = getEntryType(fileMode)
		mode = fileMode.Perm()
	case targetInfo == nil:
		// If it's a symlink, we need to determine the type of the target
		entryType = UnknownFileType
	default:
		entryType = getEntryType(targetInfo.Mode())
		mode = targetInfo.Mode().Perm()
	}

	entry := EntryInfo{
		Name:          fileInfo.Name(),
		Path:          path,
		Type:          entryType,
		Size:          fileInfo.Size(),
		Mode:          mode,
		Permissions:   fileMode.String(),
		ModifiedTime:  fileInfo.ModTime(),
		SymlinkTarget: symlinkTarget,
	}

	if base := getBase(fileInfo.Sys()); base != nil {
		times := extractStatTimes(base)
		entry.AccessedTime = times.atime
		entry.CreatedTime = times.ctime
		entry.ModifiedTime = times.mtime
		entry.UID = times.uid
		entry.GID = times.gid
	} else if !fileInfo.ModTime().IsZero() {
		entry.ModifiedTime = fileInfo.ModTime()
	}

	return entry
}

// getEntryType determines the type of file entry based on its mode and path.
// If the file is a symlink, it follows the symlink to determine the actual type.
func getEntryType(mode os.FileMode) FileType {
	switch {
	case mode.IsRegular():
		return FileFileType
	case mode.IsDir():
		return DirectoryFileType
	case mode&os.ModeSymlink == os.ModeSymlink:
		return SymlinkFileType
	default:
		return UnknownFileType
	}
}

func resolveHostSymlink(path string) (string, os.FileInfo) {
	target := followSymlink(path)

	targetInfo, err := os.Stat(target)
	if err != nil {
		return target, nil
	}

	return target, targetInfo
}

// followSymlink resolves a symbolic link to its target path.
func followSymlink(path string) string {
	// Resolve symlinks
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return path
	}

	return resolvedPath
}

func toTimestamp(spec syscall.Timespec) time.Time {
	if spec.Sec == 0 && spec.Nsec == 0 {
		return time.Time{}
	}

	return time.Unix(spec.Sec, spec.Nsec)
}

func getBase(sys any) *syscall.Stat_t {
	st, _ := sys.(*syscall.Stat_t)

	return st
}
