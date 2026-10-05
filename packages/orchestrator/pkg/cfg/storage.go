// Storage role resolution: which destination (provider, bucket/path,
// connection options) the template and build-cache storage use. A role's
// storage URL env is authoritative; when unset, the legacy envs are converted
// to the equivalent URL so both styles share one parsing path:
//
//	TEMPLATE_STORAGE_URL | legacy envs → storage.ParseStorageURL → storage.Spec
//
// Deliberately not build-tagged linux like the rest of this package so the
// resolution logic and its tests run everywhere.

package cfg

import (
	"cmp"
	"fmt"
	"net/url"
	"strings"

	"github.com/caarlos0/env/v11"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// storageEnv is the environment surface for storage role resolution, parsed
// lazily on each resolution because the CLI tools set these from flags after
// startup. Empty values behave as unset.
type storageEnv struct {
	TemplateURL   string `env:"TEMPLATE_STORAGE_URL"`
	BuildCacheURL string `env:"BUILD_CACHE_STORAGE_URL"`

	// Legacy environment style, converted to storage URLs by legacyStorageURL.
	Provider           storage.Provider `env:"STORAGE_PROVIDER"`
	TemplateBucket     string           `env:"TEMPLATE_BUCKET_NAME"`
	TemplateBasePath   string           `env:"LOCAL_TEMPLATE_STORAGE_BASE_PATH"`
	BuildCacheBucket   string           `env:"BUILD_CACHE_BUCKET_NAME"`
	BuildCacheBasePath string           `env:"LOCAL_BUILD_CACHE_STORAGE_BASE_PATH"`
	// Parsed strictly: a malformed value fails resolution loudly (even for
	// URL-configured roles) instead of being silently treated as false.
	S3PathStyle bool `env:"S3_USE_PATH_STYLE"`
	// Server-side encryption requested on every S3 write (ssetype/kmskeyid
	// in URL form); validated by storage.ParseStorageURL.
	S3SSEType     string `env:"S3_SSE_TYPE"`
	S3SSEKMSKeyID string `env:"S3_SSE_KMS_KEY_ID"`
}

// TemplateStorage resolves the template storage destination.
func TemplateStorage() (storage.Spec, error) {
	e, err := env.ParseAs[storageEnv]()
	if err != nil {
		return storage.Spec{}, fmt.Errorf("parse storage environment: %w", err)
	}

	return resolveStorage(e, e.TemplateURL, e.TemplateBucket, e.TemplateBasePath,
		"template", "TEMPLATE_BUCKET_NAME", "/tmp/templates")
}

// BuildCacheStorage resolves the build-cache storage destination.
func BuildCacheStorage() (storage.Spec, error) {
	e, err := env.ParseAs[storageEnv]()
	if err != nil {
		return storage.Spec{}, fmt.Errorf("parse storage environment: %w", err)
	}

	return resolveStorage(e, e.BuildCacheURL, e.BuildCacheBucket, e.BuildCacheBasePath,
		"build cache", "BUILD_CACHE_BUCKET_NAME", "/tmp/build-cache")
}

func resolveStorage(e storageEnv, rawURL, bucket, basePath, name, bucketEnv, defaultBasePath string) (storage.Spec, error) {
	if raw := strings.TrimSpace(rawURL); raw != "" {
		return storage.ParseStorageURL(raw)
	}

	legacy, err := legacyStorageURL(e, bucket, basePath, name, bucketEnv, defaultBasePath)
	if err != nil {
		return storage.Spec{}, err
	}

	return storage.ParseStorageURL(legacy)
}

// legacyStorageURL converts the legacy env style into a storage URL. Delete
// together with the legacy fields of storageEnv once the legacy envs are
// retired.
func legacyStorageURL(e storageEnv, bucket, basePath, name, bucketEnv, defaultBasePath string) (string, error) {
	provider := cmp.Or(e.Provider, storage.DefaultStorageProvider)
	switch provider {
	case storage.LocalStorageProvider:
		basePath = cmp.Or(basePath, defaultBasePath)
		if strings.HasPrefix(basePath, "/") {
			return (&url.URL{Scheme: "file", Path: basePath}).String(), nil
		}

		// The hierarchical file:// form cannot express a relative base
		// path; use the opaque form.
		return (&url.URL{Scheme: "file", Opaque: basePath}).String(), nil
	case storage.GCPStorageProvider, storage.AWSStorageProvider, storage.AzureStorageProvider:
		if bucket == "" {
			return "", fmt.Errorf("%s storage bucket not configured: set %s", name, bucketEnv)
		}

		u := url.URL{Scheme: "gs", Host: bucket}
		switch provider {
		case storage.AWSStorageProvider:
			u.Scheme = "s3"
			query := url.Values{}
			if e.S3PathStyle {
				query.Set("s3ForcePathStyle", "true")
			}
			if e.S3SSEType != "" {
				query.Set("ssetype", e.S3SSEType)
			}
			if e.S3SSEKMSKeyID != "" {
				query.Set("kmskeyid", e.S3SSEKMSKeyID)
			}
			u.RawQuery = query.Encode()
		case storage.AzureStorageProvider:
			u.Scheme = "azblob"
		}

		return u.String(), nil
	default:
		return "", fmt.Errorf("unknown storage provider: %s", e.Provider)
	}
}
