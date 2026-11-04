package filesystem

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/pkg/xattr"
)

// MetadataXattrPrefix is the xattr namespace used to store user-defined
// metadata that envd surfaces via its file-related APIs. The `user.` prefix
// is required by the Linux VFS for unprivileged xattrs; `e2b.` namespaces
// our keys so they cannot collide with other tooling that writes to
// `user.*` on the same files.
const MetadataXattrPrefix = "user.e2b."

// MaxMetadataKeyLen caps the length of a single metadata key. The Linux VFS
// limits the full xattr name (including the `user.e2b.` prefix) to 255 bytes,
// so this is a hard syscall constraint rather than a policy choice.
const MaxMetadataKeyLen = 255 - len(MetadataXattrPrefix)

// MaxMetadataTotalLen caps the combined size of all metadata stored on a
// single file. ext4 keeps every xattr on an inode (names and values) within a
// single filesystem block — 4 KiB on x86_64 — so a set that exceeds this can't
// be persisted and would otherwise fail late inside WriteMetadata with
// ENOSPC/E2BIG. We validate the budget up front so oversized requests are
// rejected cleanly with HTTP 400 instead. The size is measured as the sum, over
// every entry, of the stored name (`user.e2b.` prefix + key) plus its value;
// this is an approximation of the on-disk cost (it ignores ext4's small
// per-entry header overhead), so it stays a little under the true ceiling.
const MaxMetadataTotalLen = 4096

// ErrMetadataUnsupported reports metadata on a platform whose extended
// attribute backend stores nothing (and reports success). IsXattrUnsupported
// matches it, as it matches a filesystem without extended attributes.
var ErrMetadataUnsupported = fmt.Errorf("file metadata is not supported on this platform: %w", syscall.ENOTSUP)

// CheckMetadataStorable refuses non-empty metadata where nothing would store it.
func CheckMetadataStorable(metadata map[string]string) error {
	if len(metadata) > 0 && !xattr.XATTR_SUPPORTED {
		return ErrMetadataUnsupported
	}

	return nil
}

// ValidateMetadata returns an error if the metadata set would be rejected by
// WriteMetadata. Validation is filesystem-agnostic and catches obvious client
// mistakes before we issue any syscalls.
//
// Both keys and values are required to be printable US-ASCII (0x20-0x7E),
// matching the constraint documented for `X-Metadata-*` request headers.
func ValidateMetadata(metadata map[string]string) error {
	total := 0
	for k, v := range metadata {
		if k == "" {
			return errors.New("metadata key must not be empty")
		}
		if len(k) > MaxMetadataKeyLen {
			return fmt.Errorf("metadata key %q exceeds %d bytes", k, MaxMetadataKeyLen)
		}
		if err := validatePrintableASCII("key", k); err != nil {
			return err
		}
		if err := validatePrintableASCII(fmt.Sprintf("value for key %q", k), v); err != nil {
			return err
		}
		total += len(MetadataXattrPrefix) + len(k) + len(v)
	}

	if total > MaxMetadataTotalLen {
		return fmt.Errorf("total metadata size %d exceeds %d bytes", total, MaxMetadataTotalLen)
	}

	return nil
}

func validatePrintableASCII(label, s string) error {
	for i := range len(s) {
		c := s[i]
		if c < 0x20 || c > 0x7E {
			return fmt.Errorf("metadata %s contains non-printable-ASCII byte 0x%02x at offset %d", label, c, i)
		}
	}

	return nil
}

// ReadMetadata returns user-defined metadata stored in xattrs under the
// MetadataXattrPrefix namespace. Returns nil (not an error) when the
// filesystem does not support xattrs or the file has no metadata set.
func ReadMetadata(path string) (map[string]string, error) {
	names, err := xattr.List(path)
	if err != nil {
		if IsXattrUnsupported(err) {
			return nil, nil
		}

		return nil, err
	}

	var metadata map[string]string
	for _, name := range names {
		if !strings.HasPrefix(name, MetadataXattrPrefix) {
			continue
		}

		value, err := xattr.Get(path, name)
		if err != nil {
			if errors.Is(err, xattr.ENOATTR) {
				continue
			}

			return nil, err
		}

		key := strings.TrimPrefix(name, MetadataXattrPrefix)

		// Metadata can be set directly inside the sandbox (e.g. via setfattr),
		// not just through our upload API, so a value may hold arbitrary bytes.
		// EntryInfo surfaces metadata as a proto map<string, string>, which
		// requires valid UTF-8 — a non-UTF-8 entry would make the whole
		// Stat/ListDir response fail to marshal. Skip such entries rather than
		// poisoning the lookup; well-formed metadata is unaffected.
		if !utf8.ValidString(key) || !utf8.Valid(value) {
			continue
		}

		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[key] = string(value)
	}

	return metadata, nil
}

// WriteMetadata replaces the file's user-defined metadata with the given
// key/value pairs. Any existing xattrs under MetadataXattrPrefix that are
// absent from metadata are removed, and the supplied keys are written via
// xattr.FSet. Passing a nil/empty map therefore clears all metadata, so that
// overwriting a file (which preserves xattrs across O_TRUNC) does not leak
// stale metadata from a previous upload.
//
// It addresses the file through an already-open descriptor, avoiding path races
// with concurrent renames.
func WriteMetadata(file *os.File, metadata map[string]string) error {
	if err := CheckMetadataStorable(metadata); err != nil {
		return err
	}
	if err := ValidateMetadata(metadata); err != nil {
		return err
	}

	existing, err := xattr.FList(file)
	if err != nil {
		return err
	}

	for _, name := range existing {
		if !strings.HasPrefix(name, MetadataXattrPrefix) {
			continue
		}
		key := strings.TrimPrefix(name, MetadataXattrPrefix)
		if _, keep := metadata[key]; keep {
			continue
		}
		if err := xattr.FRemove(file, name); err != nil && !errors.Is(err, xattr.ENOATTR) {
			return err
		}
	}

	for k, v := range metadata {
		if err := xattr.FSet(file, MetadataXattrPrefix+k, []byte(v)); err != nil {
			return err
		}
	}

	return nil
}

// IsXattrUnsupported reports whether err indicates the filesystem does not
// support extended attributes (e.g. virtual filesystems such as /proc and
// /sys). Callers that persist metadata best-effort can use this to log and
// continue instead of failing the request.
func IsXattrUnsupported(err error) bool {
	return errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP)
}
