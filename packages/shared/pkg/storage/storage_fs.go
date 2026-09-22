package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type fsStorage struct {
	basePath  string
	uploadURL string // base URL for local upload endpoint (e.g. "http://localhost:5008")
	hmacKey   []byte // HMAC key for signing upload tokens
}

var _ StorageProvider = (*fsStorage)(nil)

type fsObject struct {
	path    string
	objType SeekableObjectType
}

var (
	_ Seekable    = (*fsObject)(nil)
	_ Blob        = (*fsObject)(nil)
	_ RangeOpener = (*fsObject)(nil)
)

func newFileSystemStorage(basePath, uploadBaseURL string, hmacKey []byte) *fsStorage {
	return &fsStorage{
		basePath:  basePath,
		uploadURL: uploadBaseURL,
		hmacKey:   hmacKey,
	}
}

func (s *fsStorage) DeleteObjectsWithPrefix(_ context.Context, prefix string) error {
	filePath := s.getPath(prefix)

	return os.RemoveAll(filePath)
}

func (s *fsStorage) GetDetails() string {
	return fmt.Sprintf("[Local file storage, base path set to %s]", s.basePath)
}

func (s *fsStorage) UploadSignedURL(_ context.Context, path string, ttl time.Duration) (UploadURL, error) {
	if s.uploadURL == "" || s.hmacKey == nil {
		return UploadURL{}, errors.New("file system storage does not support signed URLs (no local upload endpoint configured)")
	}

	expiresSec := time.Now().Add(ttl).Unix()
	token := ComputeUploadHMAC(s.hmacKey, path, expiresSec)

	u := fmt.Sprintf("%s/upload?path=%s&expires=%d&token=%s",
		s.uploadURL, url.QueryEscape(path), expiresSec, url.QueryEscape(token))

	return UploadURL{URL: u}, nil
}

func (s *fsStorage) OpenSeekable(_ context.Context, path string) (Seekable, error) {
	dir := filepath.Dir(s.getPath(path))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	objType, _ := seekableObjectType(path)

	return &fsObject{
		path:    s.getPath(path),
		objType: objType,
	}, nil
}

func (s *fsStorage) OpenBlob(_ context.Context, path string) (Blob, error) {
	dir := filepath.Dir(s.getPath(path))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &fsObject{
		path: s.getPath(path),
	}, nil
}

func (s *fsStorage) getPath(path string) string {
	return filepath.Join(s.basePath, path)
}

func (o *fsObject) WriteTo(ctx context.Context, dst io.Writer) (n int64, err error) {
	start := time.Now()
	defer func() { RecordReadBlob(ctx, time.Since(start), n, o.path, SourceFS, err) }()

	handle, err := o.getHandle(true)
	if err != nil {
		return 0, err
	}

	defer handle.Close()

	n, err = io.Copy(dst, handle)

	return n, err
}

func (o *fsObject) Put(_ context.Context, data []byte, _ ...PutOption) error {
	_, err := o.replaceFrom(bytes.NewReader(data))

	return err
}

func (o *fsObject) StoreFile(ctx context.Context, path string, opts ...PutOption) (*FullFrameTable, [32]byte, error) {
	putOpts := ApplyPutOptions(opts)
	cfg := CompressConfigFromOpts(putOpts)
	if cfg.IsCompressionEnabled() {
		ft, checksum, err := o.storeFileCompressed(ctx, path, cfg, putOpts.FrameSink)
		if err == nil {
			t := ft.Table()
			logger.L().Debug(ctx, "Stored file to filesystem",
				zap.String("object", o.path),
				zap.String("source", path),
				zap.Int64("size_uncompressed", t.UncompressedSize()),
				zap.Int64("size_compressed", t.CompressedSize()),
				zap.String("compression", cfg.CompressionType().String()),
				zap.Int("frames", t.NumFrames()),
			)
		}

		return ft, checksum, err
	}

	r, err := os.Open(path)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer r.Close()

	srcInfo, err := r.Stat()
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to stat file %s: %w", path, err)
	}
	if dstInfo, statErr := os.Stat(o.path); statErr == nil && os.SameFile(srcInfo, dstInfo) {
		return nil, [32]byte{}, nil
	}

	n, err := o.replaceFrom(r)
	if err != nil {
		return nil, [32]byte{}, err
	}

	logger.L().Debug(ctx, "Stored file to filesystem",
		zap.String("object", o.path),
		zap.String("source", path),
		zap.Int64("size_uncompressed", n),
		zap.String("compression", "none"),
	)

	return nil, [32]byte{}, nil
}

func (o *fsObject) storeFileCompressed(ctx context.Context, localPath string, cfg CompressConfig, sink FrameSink) (*FullFrameTable, [32]byte, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to open local file %s: %w", localPath, err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to stat local file %s: %w", localPath, err)
	}

	uploader := &fsPartUploader{fullPath: o.path}

	const noConcurrencyForMemUploader = 1
	ft, checksum, err := compressStream(ctx, file, cfg, uploader, noConcurrencyForMemUploader, sink)
	if err != nil {
		return nil, [32]byte{}, err
	}

	// Sidecar is written only after compressStream succeeds so a failure (cancel,
	// partial read, compress error) doesn't leave Size() reporting the new size
	// against the unchanged data file.
	sidecarPath := SizeSidecar(o.path)
	if writeErr := os.WriteFile(sidecarPath, []byte(strconv.FormatInt(fi.Size(), 10)), 0o644); writeErr != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to write uncompressed-size sidecar for %s: %w", o.path, writeErr)
	}

	return ft, checksum, nil
}

func (o *fsObject) openRangeReader(_ context.Context, off, length int64) (RangeReader, error) {
	f, err := o.getHandle(true)
	if err != nil {
		return nil, err
	}

	return newSectionReader(f, off, length), nil
}

func (o *fsObject) Exists(_ context.Context) (bool, error) {
	_, err := os.Stat(o.path)
	if os.IsNotExist(err) {
		return false, nil
	}

	return err == nil, err
}

func (o *fsObject) Size(ctx context.Context) (_ int64, err error) {
	start := time.Now()
	defer func() { RecordReadSize(ctx, time.Since(start), o.objType, SourceFS, err) }()

	handle, err := o.getHandle(true)
	if err != nil {
		return 0, err
	}
	defer handle.Close()

	fileInfo, err := handle.Stat()
	if err != nil {
		return 0, err
	}

	// Check for .uncompressed-size sidecar file
	sidecarPath := SizeSidecar(o.path)
	if sidecarData, sidecarErr := os.ReadFile(sidecarPath); sidecarErr == nil {
		if parsed, parseErr := strconv.ParseInt(strings.TrimSpace(string(sidecarData)), 10, 64); parseErr == nil {
			return parsed, nil
		}
	}

	return fileInfo.Size(), nil
}

func (o *fsObject) Delete(_ context.Context) error {
	return os.Remove(o.path)
}

func ComputeUploadHMAC(key []byte, path string, expires int64) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(path))
	mac.Write([]byte{0}) // delimiter to prevent path/expires boundary ambiguity
	mac.Write([]byte(strconv.FormatInt(expires, 10)))

	return hex.EncodeToString(mac.Sum(nil))
}

// ValidateUploadToken validates an HMAC token for a local upload URL.
// Exported so that the upload handler in the orchestrator can use it.
func ValidateUploadToken(key []byte, path string, expires int64, token string) bool {
	if time.Now().Unix() > expires {
		return false
	}

	expected := ComputeUploadHMAC(key, path, expires)

	return hmac.Equal([]byte(expected), []byte(token))
}

func (o *fsObject) getHandle(checkExistence bool) (*os.File, error) {
	if checkExistence {
		info, err := os.Stat(o.path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ErrObjectNotExist
			}

			return nil, err
		}

		if info.IsDir() {
			return nil, fmt.Errorf("path %s is a directory", o.path)
		}
	}

	handle, err := os.OpenFile(o.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	return handle, nil
}

// tmpFileMarker is embedded in the name of the temporary file used for atomic
// replacement. It is shared by replaceFile (which creates such files) and
// SweepStaleTempFiles (which reclaims the ones a crash left behind), so the two
// can never drift out of sync. Temp files are named ".<object>.tmp-<random>".
const tmpFileMarker = ".tmp-"

// envBool reads a boolean environment toggle, returning def when unset or
// unparseable. Accepts the strconv.ParseBool set (1/t/T/TRUE/true/... etc).
func envBool(name string, def bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}

	return parsed
}

// fsyncDurable, when true, makes replaceFile fsync the temp file before the
// rename and fsync the parent directory after it, so a crash cannot leave a
// renamed-but-empty/truncated object. It defaults to false to preserve the
// previous (page-cache-only) performance for the high-frequency cache writes;
// callers that store crash-critical objects (snapshots, headers) can enable it.
// It is a package-level var (not a const) so it can be toggled by config or in
// tests without threading a parameter through every caller.
var fsyncDurable = envBool("STORAGE_FS_FSYNC_DURABLE", false)

// replaceFile atomically replaces path with the contents of r via a temp file
// in the same directory followed by a rename. When durable is true it fsyncs the
// temp file before the rename and the parent directory after, so a crash cannot
// leave a renamed-but-empty object; the cost is two extra fsyncs per write.
func replaceFile(path string, r io.Reader, durable bool) (int64, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+tmpFileMarker+"*")
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	// On any failure before Rename succeeds this removes the temp file; after a
	// successful Rename the temp path no longer exists so Remove is a no-op. A
	// crash between CreateTemp and Rename skips this entirely — SweepStaleTempFiles
	// reclaims such orphans.
	defer os.Remove(tmpPath)

	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()

		return 0, err
	}

	n, err := io.Copy(tmp, r)
	if err != nil {
		tmp.Close()

		return n, err
	}

	// Durability: flush the data to disk before it becomes reachable under the
	// object's name, otherwise the rename can be persisted while the contents are
	// still only in the page cache (crash => renamed-but-empty object).
	if durable {
		if err := tmp.Sync(); err != nil {
			tmp.Close()

			return n, err
		}
	}

	if err := tmp.Close(); err != nil {
		return n, err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return n, err
	}

	// Persist the directory entry created by the rename, so the replacement
	// survives a crash immediately after it.
	if durable {
		if err := fsyncDir(dir); err != nil {
			return n, err
		}
	}

	return n, nil
}

// fsyncDir flushes a directory's metadata so a rename into it is durable.
// A directory that cannot be opened O_RDONLY for sync is treated as a no-op
// rather than a hard error (some filesystems reject directory fsync).
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		// EINVAL / ENOTSUP: filesystem doesn't support directory fsync — the
		// rename itself is still atomic, only the extra durability is missing.
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}

		return err
	}

	return nil
}

// SweepStaleTempFiles removes atomic-replacement temp files (".*.tmp-*") under
// dir that are older than olderThan and are therefore orphans left by a crash
// (OOM kill, power loss) between CreateTemp and Rename. The age gate ensures a
// temp file belonging to a concurrently in-flight write is never removed. It
// returns the number of files reclaimed. Missing dir is not an error.
func SweepStaleTempFiles(dir string, olderThan time.Duration) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, err
	}

	cutoff := time.Now().Add(-olderThan)
	removed := 0
	var errs []error
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Match ".<object>.tmp-<random>": leading dot + contains the marker.
		if !strings.HasPrefix(name, ".") || !strings.Contains(name, tmpFileMarker) {
			continue
		}
		info, statErr := e.Info()
		if statErr != nil {
			continue // vanished concurrently; ignore
		}
		if info.ModTime().After(cutoff) {
			continue // possibly an in-flight write; leave it
		}
		if rmErr := os.Remove(filepath.Join(dir, name)); rmErr != nil && !os.IsNotExist(rmErr) {
			errs = append(errs, rmErr)

			continue
		}
		removed++
	}

	return removed, errors.Join(errs...)
}

func (o *fsObject) replaceFrom(r io.Reader) (int64, error) {
	n, err := replaceFile(o.path, r, fsyncDurable)
	if err != nil {
		return n, err
	}

	return n, o.clearSizeSidecar()
}

func (o *fsObject) clearSizeSidecar() error {
	err := os.Remove(SizeSidecar(o.path))
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	return fmt.Errorf("failed to remove uncompressed-size sidecar for %s: %w", o.path, err)
}

// fsPartUploader implements partUploader for local filesystem.
// Embeds memPartUploader for concurrent-safe part collection,
// then writes atomically on Complete.
type fsPartUploader struct {
	memPartUploader

	fullPath string
}

func (u *fsPartUploader) Complete(_ context.Context) error {
	if err := os.MkdirAll(filepath.Dir(u.fullPath), 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	_, err := replaceFile(u.fullPath, bytes.NewReader(u.Assemble()), fsyncDurable)

	return err
}

func (o *fsObject) OpenRangeReader(ctx context.Context, offsetU int64, length int64, frameTable *FrameTable) (_ RangeReader, _ Source, err error) {
	start := time.Now()
	defer func() { RecordReadOpen(ctx, time.Since(start), o.objType, SourceFS, frameTable.CompressionType(), err) }()

	if frameTable.IsCompressed() {
		r, err := frameTable.LocateCompressed(offsetU)
		if err != nil {
			return nil, SourceFS, fmt.Errorf("get frame for offset %d, FS:%s: %w", offsetU, o.path, err)
		}

		raw, err := o.openRangeReader(ctx, r.Offset, int64(r.Length))
		if err != nil {
			return nil, SourceFS, err
		}

		dec, err := NewDecompressReader(raw, frameTable.CompressionType(), SourceFS, o.objType)
		if err != nil {
			raw.Close(ctx)

			return nil, SourceFS, err
		}

		return dec, SourceFS, nil
	}

	raw, err := o.openRangeReader(ctx, offsetU, length)
	if err != nil {
		return nil, SourceFS, err
	}

	return raw, SourceFS, nil
}
