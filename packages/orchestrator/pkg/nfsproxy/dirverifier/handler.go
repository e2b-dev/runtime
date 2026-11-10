// Package dirverifier keeps the directory listings behind NFSv3 READDIR and
// READDIRPLUS cookie verifiers, so a paginated listing reads the directory
// once instead of once per page.
//
// go-nfs only consults a verifier cache when the outermost handler implements
// nfs.CachingHandler, so this wrapper must be the last one applied.
//
// The cache is shared by every volume the proxy serves. go-nfs's own verifier
// is a hash of the path inside the volume and the entry names, so two volumes
// holding the same tree (a cloned repo, node_modules) share a verifier and one
// would be served the other's attributes, including inode numbers the guest
// kernel then rejects as "fileid changed". This verifier also covers each
// entry's device and inode, which no two volumes share.
package dirverifier

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/fs"
	"syscall"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/willscott/go-nfs"
)

type Handler struct {
	nfs.Handler

	listings *lru.Cache[uint64, listing]
}

type listing struct {
	path     string
	contents []fs.FileInfo
}

var _ nfs.CachingHandler = (*Handler)(nil)

// Wrap keeps the listings of the limit most recently read directories. Every
// directory read stores its full listing, so the limit bounds memory in whole
// listings, not in handles.
func Wrap(h nfs.Handler, limit int) (*Handler, error) {
	listings, err := lru.New[uint64, listing](limit)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory listing cache of size %d: %w", limit, err)
	}

	return &Handler{Handler: h, listings: listings}, nil
}

func (h *Handler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	verifier, cacheable := verifierOf(path, contents)
	if cacheable {
		h.listings.Add(verifier, listing{path: path, contents: contents})
	}

	return verifier
}

func (h *Handler) DataForVerifier(path string, verifier uint64) []fs.FileInfo {
	cached, ok := h.listings.Get(verifier)
	if !ok || cached.path != path {
		return nil
	}

	return cached.contents
}

// verifierOf is deterministic for an unchanged directory, so a listing that
// fell out of the cache is re-read and still matches the client's cookie
// verifier. It is not cacheable when an entry has no inode identity, because
// then nothing distinguishes it from the same names on another volume.
func verifierOf(path string, contents []fs.FileInfo) (uint64, bool) {
	hash := sha256.New()
	buf := make([]byte, 0, 8)
	writeString := func(s string) {
		hash.Write(binary.BigEndian.AppendUint64(buf[:0], uint64(len(s))))
		hash.Write([]byte(s))
	}

	cacheable := true
	writeString(path)
	for _, entry := range contents {
		writeString(entry.Name())

		dev, ino, ok := identity(entry)
		cacheable = cacheable && ok
		hash.Write(binary.BigEndian.AppendUint64(buf[:0], dev))
		hash.Write(binary.BigEndian.AppendUint64(buf[:0], ino))
	}

	verifier := binary.BigEndian.Uint64(hash.Sum(nil)[:8])
	if verifier == 0 {
		// zero tells the client there is no verifier
		verifier = 1
	}

	return verifier, cacheable
}

func identity(info fs.FileInfo) (dev, ino uint64, ok bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}

	return uint64(stat.Dev), stat.Ino, true //nolint:unconvert // Dev is int32 on darwin
}
