package storage

import (
	"context"
	"encoding/base64"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestAzureMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	// Every metadata key the storage index defines, not a sample: the encoding is
	// key-shape-dependent (hyphens become double underscores), so a key that was never
	// round-tripped here is a key whose encoding is unverified.
	metadata := ObjectMetadata{
		ObjectMetadataTeamID:           "team-1",
		ObjectMetadataTemplateID:       "template-1",
		ObjectMetadataBuildOrigin:      string(ObjectOriginPause),
		ObjectMetadataSoftDeleted:      "reason:action-1",
		ObjectMetadataLogicalSize:      "1024",
		ObjectMetadataUncompressedSize: "2048",
		ObjectMetadataMappedSize:       "4096",
		ObjectMetadataDiffSize:         "512",
	}

	encoded, err := encodeAzureMetadata(metadata)
	require.NoError(t, err)

	// Hyphenated keys must be encoded to valid C# identifiers.
	for key := range encoded {
		assert.NotContains(t, key, "-")
	}

	assert.Equal(t, metadata, decodeAzureMetadata(encoded))
}

func TestAzureMetadataUnsafeKeyIsRejected(t *testing.T) {
	t.Parallel()

	// Unsafe keys used to be logged and dropped, completing the write with the metadata
	// silently missing. Refusing is the only behaviour a reader can reason about.
	for _, key := range []string{"a__b", "a_-b", "a-_b", "TeamID"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			_, err := encodeAzureMetadata(ObjectMetadata{key: "v"})
			require.Error(t, err)
		})
	}
}

func TestAzureMetadataDecodeIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	// Azure returns metadata keys case-normalized (e.g. first letter upper).
	value := "team-1"
	decoded := decodeAzureMetadata(map[string]*string{"Team_id": &value})

	assert.Equal(t, ObjectMetadata{ObjectMetadataTeamID: value}, decoded)
}

func TestAzureMetadataEmpty(t *testing.T) {
	t.Parallel()

	encoded, err := encodeAzureMetadata(nil)
	require.NoError(t, err)
	assert.Nil(t, encoded)
	assert.Nil(t, decodeAzureMetadata(nil))
}

func TestAzureMetadataKeyUnsafe(t *testing.T) {
	t.Parallel()

	// Every key that survives encode+decode unchanged must be accepted, and
	// every key whose encoding is ambiguous must be rejected: "a_-b" and "a-_b"
	// both encode to "a___b", so neither can round-trip; an uppercase key is
	// case-normalized by Azure and lowercased on decode, so it reads back as a
	// different key.
	safe := []string{
		ObjectMetadataTeamID, ObjectMetadataTemplateID, ObjectMetadataBuildOrigin,
		ObjectMetadataSoftDeleted, ObjectMetadataLogicalSize,
		"plain", "with-hyphen", "with_underscore", "a-b_c",
	}
	for _, key := range safe {
		assert.False(t, azureMetadataKeyUnsafe(key), "key %q must be storable", key)

		encoded, err := encodeAzureMetadata(ObjectMetadata{key: "v"})
		require.NoError(t, err)
		assert.Equal(t, ObjectMetadata{key: "v"}, decodeAzureMetadata(encoded), "key %q must round-trip", key)
	}

	for _, key := range []string{"a__b", "a_-b", "a-_b", "TeamID", "Upper-Case"} {
		assert.True(t, azureMetadataKeyUnsafe(key), "key %q must be rejected", key)

		_, err := encodeAzureMetadata(ObjectMetadata{key: "v"})
		require.Error(t, err, "key %q must fail the write, not be skipped", key)
	}
}

func TestAzurePartUploaderBlockIDsAreUploadScoped(t *testing.T) {
	t.Parallel()

	// Azure stages blocks in a per-blob namespace with no upload identity of its own, so
	// two uploaders writing the same path must not generate colliding block IDs -- one
	// commit could otherwise mix both bodies. Assert the ID itself, not just the nonce
	// field: dropping uploadID from blockID is exactly the regression that matters, and a
	// test that only compares uploadIDs stays green through it.
	a, err := newAzurePartUploader(nil, nil)
	require.NoError(t, err)
	b, err := newAzurePartUploader(nil, nil)
	require.NoError(t, err)

	require.NotEqual(t, a.uploadID, b.uploadID)
	require.Len(t, a.uploadID, 16)

	t.Run("same part index, different uploaders, different block IDs", func(t *testing.T) {
		t.Parallel()

		assert.NotEqual(t, a.blockID(1), b.blockID(1))
	})

	t.Run("block ID carries the uploader's nonce", func(t *testing.T) {
		t.Parallel()

		decoded, err := base64.StdEncoding.DecodeString(a.blockID(7))
		require.NoError(t, err)
		assert.Contains(t, string(decoded), a.uploadID)
		assert.NotContains(t, string(decoded), b.uploadID)
	})

	t.Run("part index still distinguishes blocks within one upload", func(t *testing.T) {
		t.Parallel()

		assert.NotEqual(t, a.blockID(1), a.blockID(2))
	})

	t.Run("all block IDs for a blob decode to the same length", func(t *testing.T) {
		t.Parallel()

		// Azure rejects a commit whose block IDs are not equal-length.
		first, err := base64.StdEncoding.DecodeString(a.blockID(1))
		require.NoError(t, err)

		for _, part := range []int{2, 99, 12345678} {
			other, err := base64.StdEncoding.DecodeString(a.blockID(part))
			require.NoError(t, err)
			assert.Len(t, other, len(first), "part %d changed the block-ID length", part)
		}
	})
}

func TestNewAzureStorageAcceptsKeylessConnectionString(t *testing.T) {
	// A SAS-only connection string (no AccountKey) works for everything except minting an upload SAS.
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING",
		"BlobEndpoint=https://myaccount.blob.core.windows.net;SharedAccessSignature=sv=2022-11-02&ss=b&sig=fake")

	s, err := newAzureStorage(t.Context(), "fc-templates", nil)
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestNewAzureStorageAcceptsConnectionStringWithAccountKey(t *testing.T) {
	// The account-key shape stays accepted alongside the SAS one above.
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING",
		"DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=ZmFrZS1hY2NvdW50LWtleS1mb3ItdGVzdHMtb25seS1ub3QtYS1jcmVkZW50aWFs;EndpointSuffix=core.windows.net")

	s, err := newAzureStorage(t.Context(), "fc-templates", nil)
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestAzureUploadConcurrencyComesFromTheLimiter(t *testing.T) {
	t.Parallel()

	// A nil limiter reports the feature flag's fallback, which is deliberately not the
	// same as the 8 the provider once hardcoded: if the wiring is reverted to a constant,
	// this assertion fails. A host tuned down to bound memory must not still stage eight
	// 10 MB blocks per upload.
	o := &azureObject{limiter: nil}
	opts := o.uploadFileOptions(t.Context(), nil)

	require.NotNil(t, opts)
	assert.Equal(t, uint16(featureflags.StorageMaxUploadTasks.Fallback()), opts.Concurrency)
	assert.NotEqual(t, uint16(8), opts.Concurrency,
		"the fallback must differ from the old constant, or this test proves nothing")
	assert.Equal(t, int64(azureUploadBlockSize), opts.BlockSize)
}

func TestClampAzureUploadConcurrency(t *testing.T) {
	t.Parallel()

	// The limiter value is an operator-settable int; the SDK field is a uint16.
	// A negative throttle must degrade to the SDK default (0), not wrap to
	// 65535-way concurrency on the very host being tuned down.
	tests := []struct {
		name  string
		tasks int
		want  uint16
	}{
		{name: "negative degrades to SDK default", tasks: -1, want: 0},
		{name: "zero passes through", tasks: 0, want: 0},
		{name: "in-range passes through", tasks: 16, want: 16},
		{name: "uint16 max passes through", tasks: math.MaxUint16, want: math.MaxUint16},
		{name: "over uint16 max is clamped", tasks: math.MaxUint16 + 1, want: math.MaxUint16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, clampAzureUploadConcurrency(tt.tasks))
		})
	}
}

func TestNewAzureStorageSigningCredential(t *testing.T) {
	t.Run("connection string with an account key can sign locally", func(t *testing.T) {
		t.Setenv("AZURE_STORAGE_CONNECTION_STRING",
			"DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=ZmFrZS1hY2NvdW50LWtleS1mb3ItdGVzdHMtb25seS1ub3QtYS1jcmVkZW50aWFs;EndpointSuffix=core.windows.net")

		s, err := newAzureStorage(t.Context(), "fc-templates", nil)
		require.NoError(t, err)
		assert.NotNil(t, s.sharedKey)
		assert.False(t, s.canDelegate)

		upload, err := s.UploadSignedURL(t.Context(), "templates/abc/layer.tar", 30*time.Minute)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"x-ms-blob-type": "BlockBlob"}, upload.Headers)
		assert.Contains(t, upload.URL, "sp=cw", "the SAS must grant exactly create+write")
	})

	t.Run("SAS-only connection string cannot sign", func(t *testing.T) {
		t.Setenv("AZURE_STORAGE_CONNECTION_STRING",
			"BlobEndpoint=https://myaccount.blob.core.windows.net;SharedAccessSignature=sv=2022-11-02&ss=b&sig=fake")

		s, err := newAzureStorage(t.Context(), "fc-templates", nil)
		require.NoError(t, err)
		assert.Nil(t, s.sharedKey)
		assert.False(t, s.canDelegate)

		_, err = s.UploadSignedURL(t.Context(), "templates/abc/layer.tar", 30*time.Minute)
		require.ErrorIs(t, err, ErrSignedUploadURLUnsupported)
	})

	t.Run("account name and key env pair signs locally", func(t *testing.T) {
		t.Setenv("AZURE_STORAGE_ACCOUNT_NAME", "myaccount")
		t.Setenv("AZURE_STORAGE_ACCOUNT_KEY", "ZmFrZS1hY2NvdW50LWtleS1mb3ItdGVzdHMtb25seS1ub3QtYS1jcmVkZW50aWFs")

		s, err := newAzureStorage(t.Context(), "fc-templates", nil)
		require.NoError(t, err)
		assert.NotNil(t, s.sharedKey)

		upload, err := s.UploadSignedURL(t.Context(), "templates/abc/layer.tar", 30*time.Minute)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(upload.URL, "https://myaccount.blob.core.windows.net/"), upload.URL)
		assert.Contains(t, upload.URL, "spr=https", "a real account must only accept HTTPS")
	})
}

func TestParseConnectionStringSharedKey(t *testing.T) {
	t.Parallel()

	// Base64 keys end in '=' padding, so the value must be split on the first '=' only.
	const key = "ZmFrZS1hY2NvdW50LWtleS1mb3ItdGVzdHMtb25seS1ub3QtYS1jcmVk=="

	for _, tt := range []struct {
		name             string
		connectionString string
		wantName         string
		wantKey          string
		wantOK           bool
	}{
		{
			name:             "account key with base64 padding",
			connectionString: "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=" + key + ";EndpointSuffix=core.windows.net",
			wantName:         "myaccount",
			wantKey:          key,
			wantOK:           true,
		},
		{
			name:             "sas only",
			connectionString: "BlobEndpoint=https://myaccount.blob.core.windows.net;SharedAccessSignature=sv=2022-11-02&ss=b&sig=fake",
			wantOK:           false,
		},
		{
			name:             "account name without a key",
			connectionString: "AccountName=myaccount;SharedAccessSignature=sig=fake",
			wantName:         "myaccount",
			wantOK:           false,
		},
		{
			name:             "empty",
			connectionString: "",
			wantOK:           false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			accountName, accountKey, ok := parseConnectionStringSharedKey(tt.connectionString)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantName, accountName)
			assert.Equal(t, tt.wantKey, accountKey)
		})
	}
}

// staticTokenCredential stands in for a managed identity — the pipeline demands a bearer token before it talks to the fake transport.
type staticTokenCredential struct{}

func (staticTokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "fake-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// userDelegationKeyTransport answers Get User Delegation Key with a canned key and records the request, so its shape asserts without an AAD-backed account.
type userDelegationKeyTransport struct {
	query url.Values
	calls int
}

func (t *userDelegationKeyTransport) Do(req *http.Request) (*http.Response, error) {
	t.calls++
	t.query = req.URL.Query()

	// Azure's own documented example key value; it only ever signs in this test.
	body := `<?xml version="1.0" encoding="utf-8"?>
<UserDelegationKey>
  <SignedOid>11111111-1111-1111-1111-111111111111</SignedOid>
  <SignedTid>22222222-2222-2222-2222-222222222222</SignedTid>
  <SignedStart>2026-09-11T09:00:00Z</SignedStart>
  <SignedExpiry>2026-09-11T09:30:00Z</SignedExpiry>
  <SignedService>b</SignedService>
  <SignedVersion>2026-06-06</SignedVersion>
  <Value>ZmFrZS11c2VyLWRlbGVnYXRpb24ta2V5LWZvci10ZXN0cy1vbmx5</Value>
</UserDelegationKey>`

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/xml"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

// Covers the managed-identity signing path; only the RBAC grant behind the key fetch cannot be exercised here.
func TestAzureUploadSignedURLSignsWithUserDelegation(t *testing.T) {
	t.Parallel()

	transport := &userDelegationKeyTransport{}
	client, err := azblob.NewClient("https://myaccount.blob.core.windows.net/", staticTokenCredential{},
		&azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: transport}})
	require.NoError(t, err)

	provider := &azureStorage{
		client:        client,
		container:     client.ServiceClient().NewContainerClient("fc-templates"),
		containerName: "fc-templates",
		canDelegate:   true,
	}

	upload, err := provider.UploadSignedURL(t.Context(), "templates/abc/layer.tar", 30*time.Minute)
	require.NoError(t, err)

	assert.Equal(t, 1, transport.calls)
	assert.Equal(t, "userdelegationkey", transport.query.Get("comp"))
	assert.Equal(t, "service", transport.query.Get("restype"))

	assert.Equal(t, map[string]string{"x-ms-blob-type": "BlockBlob"}, upload.Headers)

	signed, err := url.Parse(upload.URL)
	require.NoError(t, err)
	assert.Equal(t, "/fc-templates/templates/abc/layer.tar", signed.Path)

	params := signed.Query()
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", params.Get("skoid"), "a user-delegation SAS is identified by skoid/sktid")
	assert.Equal(t, "22222222-2222-2222-2222-222222222222", params.Get("sktid"))
	assert.Equal(t, "cw", params.Get("sp"))
	assert.Equal(t, "b", params.Get("sr"))
	assert.Equal(t, "https", params.Get("spr"))
	assert.NotEmpty(t, params.Get("sig"))
}
