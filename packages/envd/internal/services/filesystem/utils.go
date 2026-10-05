package filesystem

import (
	"fmt"
	"os"
	"os/user"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/timestamppb"

	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	"github.com/e2b-dev/infra/packages/shared/pkg/filesystem"
)

// Filesystem magic numbers from Linux kernel (include/uapi/linux/magic.h)
const (
	nfsSuperMagic   = 0x6969
	cifsMagic       = 0xFF534D42
	smbSuperMagic   = 0x517B
	smb2MagicNumber = 0xFE534D42
	fuseSuperMagic  = 0x65735546
)

// IsPathOnNetworkMount checks if the given path is on a network filesystem mount.
// Returns true if the path is on NFS, CIFS, SMB, or FUSE filesystem.
func IsPathOnNetworkMount(path string) (bool, error) {
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(path, &statfs); err != nil {
		return false, fmt.Errorf("failed to statfs %s: %w", path, err)
	}

	switch statfs.Type {
	case nfsSuperMagic, cifsMagic, smbSuperMagic, smb2MagicNumber, fuseSuperMagic:
		return true, nil
	default:
		return false, nil
	}
}

func entryInfo(path string) (*rpc.EntryInfo, error) {
	info, err := filesystem.GetEntryFromPath(path, true)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("file not found: %w", err))
		}

		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error getting file info: %w", err))
	}

	owner, group := getFileOwnership(info)

	return &rpc.EntryInfo{
		Name:          info.Name,
		Type:          getEntryType(info.Type),
		Path:          info.Path,
		Size:          info.Size,
		Mode:          uint32(info.Mode),
		Permissions:   info.Permissions,
		Owner:         owner,
		Group:         group,
		ModifiedTime:  toTimestamp(info.ModifiedTime),
		SymlinkTarget: info.SymlinkTarget,
		Metadata:      info.Metadata,
		IsSymlink:     info.SymlinkTarget != nil,
	}, nil
}

// opCarriesEntry reports whether a filesystem event of the given type refers to an entry
// that is expected to still exist at the path, and may therefore carry EntryInfo. Remove
// and rename events refer to a path whose original entry is gone, so they must never carry
// entry info: stat-ing the path could otherwise attach a replacement entry that was created
// at the same path before the event was handled.
func opCarriesEntry(op rpc.EventType) bool {
	switch op {
	case rpc.EventType_EVENT_TYPE_CREATE,
		rpc.EventType_EVENT_TYPE_WRITE,
		rpc.EventType_EVENT_TYPE_CHMOD:
		return true
	default:
		return false
	}
}

// eventEntryInfo returns the EntryInfo for the path that triggered a filesystem event.
// It must only be called for events that carry an entry (see opCarriesEntry).
//
// Entry info is best-effort: a nil entry is returned (and the watch keeps running) when it
// cannot be retrieved. A NotFound result is treated as a benign race (the entry was removed
// between the event and the stat) and is not logged; any other failure is logged at warn level.
func eventEntryInfo(logger *zerolog.Logger, path string) *rpc.EntryInfo {
	entry, err := entryInfo(path)
	if err != nil {
		// NotFound is a benign race: the entry was removed before we could stat it.
		if connect.CodeOf(err) != connect.CodeNotFound {
			logger.Warn().Err(err).Str("path", path).Msg("failed to get entry info for filesystem event")
		}

		return nil
	}

	return entry
}

func toTimestamp(time time.Time) *timestamppb.Timestamp {
	if time.IsZero() {
		return nil
	}

	return timestamppb.New(time)
}

// getFileOwnership returns the owner and group names for a file.
// If the lookup fails, it returns the numeric UID and GID as strings.
func getFileOwnership(fileInfo filesystem.EntryInfo) (owner, group string) {
	// Look up username
	owner = fmt.Sprintf("%d", fileInfo.UID)
	if u, err := user.LookupId(owner); err == nil {
		owner = u.Username
	}

	// Look up group name
	group = fmt.Sprintf("%d", fileInfo.GID)
	if g, err := user.LookupGroupId(group); err == nil {
		group = g.Name
	}

	return owner, group
}

// getEntryType determines the type of file entry based on its mode and path.
// If the file is a symlink, it follows the symlink to determine the actual type.
func getEntryType(fileType filesystem.FileType) rpc.FileType {
	switch fileType {
	case filesystem.FileFileType:
		return rpc.FileType_FILE_TYPE_FILE
	case filesystem.DirectoryFileType:
		return rpc.FileType_FILE_TYPE_DIRECTORY
	case filesystem.SymlinkFileType:
		return rpc.FileType_FILE_TYPE_SYMLINK
	default:
		return rpc.FileType_FILE_TYPE_UNSPECIFIED
	}
}
