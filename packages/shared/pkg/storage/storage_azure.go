package storage

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	azureOperationTimeout = 5 * time.Second
	azureWriteTimeout     = 30 * time.Second
	azureReadTimeout      = 15 * time.Second

	azureUploadBlockSize = 10 * 1024 * 1024 // 10 MB

	// Backdated SAS start time, so clock skew cannot reject a just-issued token.
	azureSASClockSkew = 5 * time.Minute
)

type azureStorage struct {
	client        *azblob.Client
	container     *container.Client
	containerName string
	limiter       *limit.Limiter

	// A shared key signs a SAS locally, a token credential fetches a user delegation key; SAS-only auth can do neither.
	sharedKey   *azblob.SharedKeyCredential
	canDelegate bool
}

var _ StorageProvider = (*azureStorage)(nil)

type azureObject struct {
	container     *container.Client
	containerName string
	path          string
	limiter       *limit.Limiter
}

var (
	_ Seekable       = (*azureObject)(nil)
	_ Blob           = (*azureObject)(nil)
	_ MetadataReader = (*azureObject)(nil)
)

// newAzureStorage builds a Blob Storage backed provider. The "bucket name"
// from the storage spec maps to the blob container name. Authentication
// tries AZURE_STORAGE_CONNECTION_STRING first; otherwise it targets
// https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net with the account
// key (AZURE_STORAGE_ACCOUNT_KEY) when present, falling back to
// azidentity.NewDefaultAzureCredential.
func newAzureStorage(ctx context.Context, containerName string, limiter *limit.Limiter) (*azureStorage, error) {
	var (
		client      *azblob.Client
		sharedKey   *azblob.SharedKeyCredential
		canDelegate bool
	)

	if connectionString := consts.AzureStorageConnectionString(); connectionString != "" {
		var err error
		client, err = azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client from connection string: %w", err)
		}

		// The SDK keeps the credential it parsed private, so signing needs its own copy.
		if accountName, accountKey, ok := parseConnectionStringSharedKey(connectionString); ok {
			sharedKey, err = azblob.NewSharedKeyCredential(accountName, accountKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create Azure shared key credential: %w", err)
			}
		}
	} else {
		accountName := consts.AzureStorageAccountName()
		if accountName == "" {
			return nil, errors.New("AZURE_STORAGE_ACCOUNT_NAME environment variable is not set")
		}

		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

		if accountKey := consts.AzureStorageAccountKey(); accountKey != "" {
			var err error
			sharedKey, err = azblob.NewSharedKeyCredential(accountName, accountKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create Azure shared key credential: %w", err)
			}

			client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, sharedKey, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create Azure client with shared key: %w", err)
			}
		} else {
			credential, err := azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create Azure default credential: %w", err)
			}

			client, err = azblob.NewClient(serviceURL, credential, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create Azure client: %w", err)
			}

			canDelegate = true
		}
	}

	if sharedKey == nil && !canDelegate {
		logger.L().Warn(ctx, "Azure storage cannot sign upload URLs with a SAS-only connection string: template layer-file uploads will fail on a cache miss",
			zap.String("container", containerName))
	}

	return &azureStorage{
		client:        client,
		container:     client.ServiceClient().NewContainerClient(containerName),
		containerName: containerName,
		limiter:       limiter,
		sharedKey:     sharedKey,
		canDelegate:   canDelegate,
	}, nil
}

// ok is false for a connection string that authenticates with a SAS instead of a key.
func parseConnectionStringSharedKey(connectionString string) (accountName, accountKey string, ok bool) {
	for part := range strings.SplitSeq(connectionString, ";") {
		key, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}

		switch strings.TrimSpace(key) {
		case "AccountName":
			accountName = value
		case "AccountKey":
			accountKey = value
		}
	}

	return accountName, accountKey, accountName != "" && accountKey != ""
}

func (s *azureStorage) DeleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	// An empty prefix would match, and delete, every blob in the container.
	if prefix == "" {
		return errors.New("refusing to delete objects with an empty prefix")
	}

	// Deletes are sequential per blob for now. azblob does have Blob Batch
	// (container.Client.NewBatchBuilder/SubmitBatch, 256 deletes per request,
	// supported by Azurite) — switching to it is deferred, so large prefixes
	// pay one round trip per blob where the AWS provider pays one per 1000.
	// The listing pages and deletes inherit the caller's context instead of
	// one short operation timeout.
	deleted := 0
	pager := s.container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("error when listing blobs with prefix: %w", err)
		}

		for _, item := range page.Segment.BlobItems {
			_, err := s.container.NewBlobClient(*item.Name).Delete(ctx, nil)
			if err != nil {
				if !bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
					return fmt.Errorf("error when deleting blob %q: %w", *item.Name, err)
				}

				// Already gone: tolerated, but not counted. Counting it meant a concurrent
				// cleaner removing everything mid-walk suppressed the "no objects found"
				// warning below while this call deleted nothing.
				continue
			}

			deleted++
		}
	}

	if deleted == 0 {
		logger.L().Warn(ctx, "No objects found to delete with the given prefix", zap.String("prefix", prefix), zap.String("container", s.containerName))
	}

	return nil
}

func (s *azureStorage) GetDetails() string {
	return fmt.Sprintf("[Azure Storage, container set to %s]", s.containerName)
}

// Put Blob also requires the "x-ms-blob-type" request header, which a SAS cannot carry, so it travels back in Headers for the external client to send.
func (s *azureStorage) UploadSignedURL(ctx context.Context, path string, ttl time.Duration) (UploadURL, error) {
	blobURL := s.container.NewBlobClient(path).URL()

	now := time.Now().UTC()
	values := sas.BlobSignatureValues{
		Protocol:      sasProtocolFor(blobURL),
		StartTime:     now.Add(-azureSASClockSkew),
		ExpiryTime:    now.Add(ttl),
		Permissions:   (&sas.BlobPermissions{Create: true, Write: true}).String(),
		ContainerName: s.containerName,
		BlobName:      path,
	}

	var (
		params sas.QueryParameters
		err    error
	)

	switch {
	case s.sharedKey != nil:
		params, err = values.SignWithSharedKey(s.sharedKey)
	case s.canDelegate:
		var credential *service.UserDelegationCredential
		credential, err = s.client.ServiceClient().GetUserDelegationCredential(ctx, service.KeyInfo{
			Start:  new(values.StartTime.Format(sas.TimeFormat)),
			Expiry: new(values.ExpiryTime.Format(sas.TimeFormat)),
		}, nil)
		if err != nil {
			return UploadURL{}, fmt.Errorf("failed to get Azure user delegation key for %q: %w", path, err)
		}

		params, err = values.SignWithUserDelegation(credential)
	default:
		return UploadURL{}, fmt.Errorf("%w: Azure (%q) needs AZURE_STORAGE_ACCOUNT_KEY, an AccountKey in AZURE_STORAGE_CONNECTION_STRING, or a credential allowed to fetch a user delegation key", ErrSignedUploadURLUnsupported, path)
	}

	if err != nil {
		return UploadURL{}, fmt.Errorf("failed to sign Azure upload SAS for %q: %w", path, err)
	}

	return UploadURL{
		URL:     blobURL + "?" + params.Encode(),
		Headers: map[string]string{"x-ms-blob-type": string(blob.BlobTypeBlockBlob)},
	}, nil
}

// HTTPS-only against a real account; the storage emulator serves plain HTTP.
func sasProtocolFor(blobURL string) sas.Protocol {
	if strings.HasPrefix(blobURL, "https://") {
		return sas.ProtocolHTTPS
	}

	return sas.ProtocolHTTPSandHTTP
}

func (s *azureStorage) OpenSeekable(_ context.Context, path string) (Seekable, error) {
	return &azureObject{
		container:     s.container,
		containerName: s.containerName,
		path:          path,
		limiter:       s.limiter,
	}, nil
}

func (s *azureStorage) OpenBlob(_ context.Context, path string) (Blob, error) {
	return &azureObject{
		container:     s.container,
		containerName: s.containerName,
		path:          path,
		limiter:       s.limiter,
	}, nil
}

func (o *azureObject) WriteTo(ctx context.Context, dst io.Writer) (n int64, err error) {
	start := time.Now()
	defer func() { RecordReadBlob(ctx, time.Since(start), n, o.path, SourceAzure, err) }()

	ctx, cancel := context.WithTimeout(ctx, azureReadTimeout)
	defer cancel()

	resp, err := o.container.NewBlobClient(o.path).DownloadStream(ctx, nil)
	if err != nil {
		// BlobNotFound only, deliberately not ContainerNotFound. Mapping a missing container
		// to ErrObjectNotExist makes a misconfigured cluster indistinguishable from empty
		// storage: the orchestrator reads "no snapshot" and starts fresh instead of failing,
		// so a typo in the bucket name looks like normal operation. The AWS provider surfaces
		// NoSuchBucket and GCP surfaces ErrBucketNotExist for the same reason. The two Delete
		// paths below still tolerate it, where idempotent-delete semantics make it correct.
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return 0, ErrObjectNotExist
		}

		return 0, err
	}

	defer resp.Body.Close()

	n, err = io.Copy(dst, resp.Body)

	return n, err
}

func (o *azureObject) StoreFile(ctx context.Context, path string, opts ...PutOption) (*FullFrameTable, [32]byte, error) {
	p := ApplyPutOptions(opts)

	release, err := o.limiter.AcquireUploadSlot(ctx)
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer release()

	cfg := CompressConfigFromOpts(p)
	if cfg.IsCompressionEnabled() {
		return storeFileCompressed(ctx, path, cfg, o.limiter.MaxUploadTasks(ctx), p, func(metadata ObjectMetadata) (partUploader, error) {
			return newAzurePartUploader(o.container.NewBlockBlobClient(o.path), metadata)
		})
	}

	// Inherit the caller's context for the block upload. UploadFile stages
	// blocks concurrently (Concurrency=8, BlockSize=10MB) and commits the block
	// list at the end — a tight static timeout here would cancel an in-flight
	// multi-GB snapshot upload. The caller (pkg/server/sandboxes.go) already
	// scopes a per-attempt deadline (uploadTimeout = 20m) with retry budget on
	// top, matching the AWS/GCP paths which also inherit the caller's ctx.
	f, err := os.Open(path)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer f.Close()

	uploadMetadata, err := encodeAzureMetadata(p.Metadata)
	if err != nil {
		return nil, [32]byte{}, err
	}

	_, err = o.container.NewBlockBlobClient(o.path).UploadFile(
		ctx,
		f,
		o.uploadFileOptions(ctx, uploadMetadata),
	)
	if err == nil {
		fi, _ := f.Stat()
		var size int64
		if fi != nil {
			size = fi.Size()
		}

		logger.L().Debug(ctx, "Uploaded file to Azure Blob Storage",
			zap.String("container", o.containerName),
			zap.String("object", o.path),
			zap.String("source", path),
			zap.Int64("size_uncompressed", size),
			zap.String("compression", "none"),
		)
	}

	return nil, [32]byte{}, err
}

func (o *azureObject) Put(ctx context.Context, data []byte, opts ...PutOption) error {
	ctx, cancel := context.WithTimeout(ctx, azureWriteTimeout)
	defer cancel()

	putMetadata, err := encodeAzureMetadata(ApplyPutOptions(opts).Metadata)
	if err != nil {
		return err
	}

	_, err = o.container.NewBlockBlobClient(o.path).UploadBuffer(
		ctx,
		data,
		&blockblob.UploadBufferOptions{
			Metadata: putMetadata,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (o *azureObject) OpenRangeReader(ctx context.Context, off, length int64, frameTable *FrameTable) (_ RangeReader, _ Source, err error) {
	start := time.Now()
	objType, _ := seekableObjectType(o.path)
	defer func() {
		RecordReadOpen(ctx, time.Since(start), objType, SourceAzure, frameTable.CompressionType(), err)
	}()

	if !frameTable.IsCompressed() {
		rc, err := o.openRangeReader(ctx, off, length)
		if err != nil {
			return nil, SourceAzure, err
		}

		return rc, SourceAzure, nil
	}

	r, err := frameTable.LocateCompressed(off)
	if err != nil {
		return nil, SourceAzure, fmt.Errorf("get frame for offset %d, Azure:%s: %w", off, o.path, err)
	}

	raw, err := o.openRangeReader(ctx, r.Offset, int64(r.Length))
	if err != nil {
		return nil, SourceAzure, err
	}

	dec, err := NewDecompressReader(raw, frameTable.CompressionType(), SourceAzure, objType)
	if err != nil {
		raw.Close(ctx)

		return nil, SourceAzure, err
	}

	return dec, SourceAzure, nil
}

func (o *azureObject) openRangeReader(ctx context.Context, off, length int64) (RangeReader, error) {
	// In azblob a Count of 0 means CountToEnd, which would silently invert a
	// zero-length request into a read of the whole blob tail.
	if length <= 0 {
		return nil, fmt.Errorf("invalid range length %d for %q", length, o.path)
	}

	resp, err := o.container.NewBlobClient(o.path).DownloadStream(
		ctx,
		&blob.DownloadStreamOptions{
			Range: blob.HTTPRange{Offset: off, Count: length},
		},
	)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, ErrObjectNotExist
		}

		return nil, fmt.Errorf("failed to create Azure range reader for %q: %w", o.path, err)
	}

	return NewRangeReader(resp.Body), nil
}

func (o *azureObject) Size(ctx context.Context) (_ int64, err error) {
	start := time.Now()
	objType, _ := seekableObjectType(o.path)
	defer func() { RecordReadSize(ctx, time.Since(start), objType, SourceAzure, err) }()

	ctx, cancel := context.WithTimeout(ctx, azureOperationTimeout)
	defer cancel()

	resp, err := o.container.NewBlobClient(o.path).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return 0, ErrObjectNotExist
		}

		return 0, err
	}

	if size, ok := decodeAzureMetadata(resp.Metadata).UncompressedSize(); ok {
		return size, nil
	}

	return *resp.ContentLength, nil
}

// Metadata implements MetadataReader via GetProperties (always hits Azure).
func (o *azureObject) Metadata(ctx context.Context) (ObjectMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, azureOperationTimeout)
	defer cancel()

	resp, err := o.container.NewBlobClient(o.path).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, fmt.Errorf("failed to get Azure blob (%q) properties: %w", o.path, ErrObjectNotExist)
		}

		return nil, fmt.Errorf("failed to get Azure blob (%q) properties: %w", o.path, err)
	}

	return decodeAzureMetadata(resp.Metadata), nil
}

func (o *azureObject) Exists(ctx context.Context) (bool, error) {
	_, err := o.Size(ctx)

	return err == nil, ignoreNotExists(err)
}

func (o *azureObject) Delete(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, azureOperationTimeout)
	defer cancel()

	// Deleting a missing blob succeeds, matching S3 DeleteObject semantics.
	_, err := o.container.NewBlobClient(o.path).Delete(ctx, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
		return err
	}

	return nil
}

// azurePartUploader maps the partUploader contract onto block blobs:
// UploadPart stages a block, Complete commits the block list in part order.
// There is no Abort equivalent — uncommitted blocks are garbage-collected by
// Azure after 7 days, so Close after a failed upload is a no-op.
type azurePartUploader struct {
	client   *blockblob.Client
	metadata ObjectMetadata
	// uploadID isolates this uploader's staged blocks. Azure stages blocks in a
	// per-blob namespace with no S3-style upload identity, so two concurrent
	// uploads of the same path would otherwise stage over each other's
	// deterministic block IDs and one could commit a mix of both bodies.
	uploadID string

	mu     sync.Mutex
	blocks []azureStagedBlock
}

type azureStagedBlock struct {
	partIndex int
	id        string
}

var _ partUploader = (*azurePartUploader)(nil)

// newAzurePartUploader creates an uploader with a random upload identity.
func newAzurePartUploader(client *blockblob.Client, metadata ObjectMetadata) (*azurePartUploader, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("failed to generate azure upload id: %w", err)
	}

	return &azurePartUploader{
		client:   client,
		metadata: metadata,
		uploadID: hex.EncodeToString(nonce[:]),
	}, nil
}

func (m *azurePartUploader) Start(context.Context) error {
	// Block blobs need no initiation call; blocks are staged directly.
	return nil
}

// UploadPart stages one block. Block IDs must be base64-encoded and equal
// length within a blob; the fixed-width upload id + part index satisfies both.
func (m *azurePartUploader) UploadPart(ctx context.Context, partIndex int, data ...[]byte) error {
	reader := newMultiSliceReader(data)

	// Azure rejects a zero-length Put Block, but a block list may legally be
	// empty — so an empty part is dropped rather than staged. (S3 and the GCS
	// XML API instead require at least one part, which is why compressStream
	// emits an empty one for zero-byte input.)
	if reader.Size() == 0 {
		return nil
	}

	body := streaming.NopCloser(reader)

	blockID := m.blockID(partIndex)

	if _, err := m.client.StageBlock(ctx, blockID, body, nil); err != nil {
		return fmt.Errorf("failed to stage block %d: %w", partIndex, err)
	}

	m.mu.Lock()
	m.blocks = append(m.blocks, azureStagedBlock{partIndex: partIndex, id: blockID})
	m.mu.Unlock()

	return nil
}

// blockID names a staged block. It must carry uploadID: Azure stages blocks in a per-blob
// namespace with no S3-style upload identity, so two concurrent uploads of the same path
// would otherwise stage over each other's deterministic IDs and one could commit a mix of
// both bodies. Extracted so that property is assertable without a live client.
//
// Every block ID committed for one blob must also decode to the same length, which holds
// here because uploadID is a fixed-width hex nonce and partIndex is zero-padded.
func (m *azurePartUploader) blockID(partIndex int) string {
	return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s-%08d", m.uploadID, partIndex))
}

func (m *azurePartUploader) Complete(ctx context.Context) error {
	m.mu.Lock()
	blocks := make([]azureStagedBlock, len(m.blocks))
	copy(blocks, m.blocks)
	m.mu.Unlock()

	slices.SortFunc(blocks, func(a, b azureStagedBlock) int {
		return a.partIndex - b.partIndex
	})

	ids := make([]string, len(blocks))
	for i, b := range blocks {
		ids[i] = b.id
	}

	commitMetadata, err := encodeAzureMetadata(m.metadata)
	if err != nil {
		return err
	}

	_, err = m.client.CommitBlockList(ctx, ids, &blockblob.CommitBlockListOptions{
		Metadata: commitMetadata,
	})
	if err != nil {
		return err
	}

	return nil
}

func (m *azurePartUploader) Close() error {
	// Nothing to abort: blocks staged for a never-committed blob expire on
	// their own (see the type comment).
	return nil
}

// azureMetadataKeyUnsafe reports whether a key cannot survive the encoding
// below. "__" would decode back to "-", and a "_" adjacent to a "-" encodes
// into a run of three underscores that decodes ambiguously (a_-b and a-_b
// both become a___b). Uppercase cannot round-trip either: Azure
// case-normalizes stored keys and decode lowercases before decoding, so
// "TeamID" would write fine and read back as a different key ("teamid") —
// exactly the "never set vs could not be stored" ambiguity the encode error
// path exists to prevent.
func azureMetadataKeyUnsafe(key string) bool {
	return strings.Contains(key, "__") || strings.Contains(key, "_-") || strings.Contains(key, "-_") ||
		key != strings.ToLower(key)
}

// uploadFileOptions builds the block-blob upload options. Extracted so the concurrency
// wiring is assertable: it comes from the limiter, as the AWS path does, and a host tuned
// down to bound memory would otherwise still stage a fixed number of 10 MB blocks per
// upload.
func (o *azureObject) uploadFileOptions(ctx context.Context, metadata map[string]*string) *blockblob.UploadFileOptions {
	return &blockblob.UploadFileOptions{
		BlockSize:   azureUploadBlockSize,
		Concurrency: clampAzureUploadConcurrency(o.limiter.MaxUploadTasks(ctx)),
		Metadata:    metadata,
	}
}

// clampAzureUploadConcurrency bounds the operator-settable limiter value to
// what the SDK's uint16 field can express. MaxUploadTasks comes from a feature
// flag an operator can set to any int: a negative value must degrade to the
// SDK default (0), not wrap through the uint16 conversion into 65535-way
// concurrency on the very host being throttled — the AWS path passes the same
// value as a plain int and degrades safely.
func clampAzureUploadConcurrency(tasks int) uint16 {
	return uint16(min(max(tasks, 0), math.MaxUint16))
}

// encodeAzureMetadata converts object metadata to the map[string]*string shape
// the Azure SDK expects. Azure metadata keys must be valid C# identifiers, so
// hyphens (used by several storage-index keys) are encoded as double
// underscores; no existing key trips azureMetadataKeyUnsafe.
func encodeAzureMetadata(metadata ObjectMetadata) (map[string]*string, error) {
	if len(metadata) == 0 {
		return nil, nil
	}

	encoded := make(map[string]*string, len(metadata))
	for k, v := range metadata {
		if azureMetadataKeyUnsafe(k) {
			// An error, not a log line and a skip. Dropping the key completed the write with
			// the metadata silently absent, so a reader could not tell "never set" from
			// "could not be stored" -- and any future key whose value carries meaning (a
			// tombstone, a size) would fail open. No current key trips this, so refusing is
			// free today and safe later.
			return nil, fmt.Errorf("metadata key %q cannot be stored on Azure without corrupting on decode", k)
		}

		encoded[strings.ReplaceAll(k, "-", "__")] = new(v)
	}

	return encoded, nil
}

// decodeAzureMetadata reverses encodeAzureMetadata. Azure returns metadata
// keys case-normalized, so they are lowercased before decoding.
func decodeAzureMetadata(metadata map[string]*string) ObjectMetadata {
	if len(metadata) == 0 {
		return nil
	}

	decoded := make(ObjectMetadata, len(metadata))
	for k, v := range metadata {
		var value string
		if v != nil {
			value = *v
		}

		decoded[strings.ReplaceAll(strings.ToLower(k), "__", "-")] = value
	}

	return decoded
}
