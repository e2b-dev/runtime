// Storage URL parsing: the gocloud.dev blob URL dialect declaring a storage
// destination (provider, bucket/path, connection options) in one string.
// Which destination each storage role uses is resolved by the binaries'
// config layer (e.g. orchestrator/pkg/cfg); this package only parses URLs
// (ParseStorageURL) and constructs providers (NewProvider).

package storage

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type Provider string

const (
	GCPStorageProvider   Provider = "GCPBucket"
	AWSStorageProvider   Provider = "AWSBucket"
	AzureStorageProvider Provider = "AzureBucket"
	LocalStorageProvider Provider = "Local"

	DefaultStorageProvider Provider = GCPStorageProvider
)

// Spec is a fully resolved storage destination, produced from a
// storage URL (see ParseStorageURL).
type Spec struct {
	Provider Provider

	// Bucket for cloud providers (gs://, s3://); the container name for azblob://.
	Bucket string
	// BasePath for the local filesystem provider (file://).
	BasePath string

	// Endpoint overrides the S3 endpoint; empty defers to the AWS SDK (incl. AWS_ENDPOINT_URL).
	Endpoint string
	// UsePathStyle forces S3 path-style addressing (needed by most S3-compatible backends).
	UsePathStyle bool
	// Region overrides the S3 region; empty defers to the AWS SDK (AWS_REGION et al.).
	Region string
}

// ParseStorageURL parses a storage URL into a Spec, following the
// gocloud.dev blob URL dialect:
//
//	gs://bucket                                                    Google Cloud Storage
//	s3://bucket?endpoint=http://host:port/s3&s3ForcePathStyle=true S3 / S3-compatible
//	s3://bucket?region=us-east-1                                   plain AWS S3
//	azblob://container                                             Azure Blob Storage
//	file:///var/lib/storage                                        local filesystem
//	file:relative/path                                             local filesystem (relative)
//
// Unknown query parameters are rejected so typos fail fast. Credentials are
// not accepted in URLs; they come from the provider's usual environment
// (ADC / Workload Identity for gs://, AWS_ACCESS_KEY_ID etc. for s3://,
// AZURE_STORAGE_ACCOUNT_NAME + managed identity / account key for azblob://).
func ParseStorageURL(raw string) (Spec, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		// Do not echo the raw URL: it may carry credentials. url.Error also
		// embeds the URL, so unwrap it and keep only the underlying cause.
		if urlErr, ok := errors.AsType[*url.Error](err); ok {
			err = urlErr.Err
		}

		return Spec{}, fmt.Errorf("invalid storage URL: %w", err)
	}

	switch strings.ToLower(u.Scheme) {
	case "gs":
		return parseBucketURL(u, GCPStorageProvider)
	case "s3":
		return parseBucketURL(u, AWSStorageProvider)
	case "azblob":
		return parseBucketURL(u, AzureStorageProvider)
	case "file":
		return parseFileURL(u)
	default:
		return Spec{}, fmt.Errorf("storage URL %q: unsupported scheme %q (want gs, s3, azblob, or file)", redactedURL(u), u.Scheme)
	}
}

// redactedURL renders a URL for error messages with any userinfo stripped, so
// credentials mistakenly pasted into a storage URL never round-trip into
// errors and from there into logs and observability sinks. Query parameter
// values may themselves be URLs (endpoint=…) carrying mistyped credentials,
// so userinfo is stripped from those too.
func redactedURL(u *url.URL) string {
	c := *u
	c.User = nil

	if c.RawQuery != "" {
		query := c.Query()
		for key, values := range query {
			for i, value := range values {
				if v, err := url.Parse(value); err == nil && v.User != nil {
					v.User = nil
					values[i] = v.String()
				}
			}
			query[key] = values
		}
		c.RawQuery = query.Encode()
	}

	return c.String()
}

// parseBucketURL handles the bucket-addressed schemes (gs://, s3://). Only
// s3:// accepts connection query parameters.
func parseBucketURL(u *url.URL, provider Provider) (Spec, error) {
	if err := validateBucketURL(u); err != nil {
		return Spec{}, err
	}

	spec := Spec{Provider: provider, Bucket: u.Host}

	query := u.Query()
	if provider != AWSStorageProvider && len(query) > 0 {
		return Spec{}, fmt.Errorf("storage URL %q: %s:// does not accept query parameters", redactedURL(u), u.Scheme)
	}

	for key, values := range query {
		value := values[len(values)-1]

		switch key {
		case "endpoint":
			endpoint, err := url.Parse(value)
			if err != nil || endpoint.Scheme != "http" && endpoint.Scheme != "https" || endpoint.Host == "" {
				return Spec{}, fmt.Errorf("storage URL %q: endpoint must be an absolute http(s) URL", redactedURL(u))
			}
			if endpoint.User != nil {
				return Spec{}, fmt.Errorf("storage URL %q: credentials in URLs are not supported", redactedURL(u))
			}
			spec.Endpoint = value
		case "s3ForcePathStyle":
			pathStyle, err := strconv.ParseBool(value)
			if err != nil {
				return Spec{}, fmt.Errorf("storage URL %q: invalid s3ForcePathStyle %q: %w", redactedURL(u), value, err)
			}
			spec.UsePathStyle = pathStyle
		case "region":
			spec.Region = value
		default:
			return Spec{}, fmt.Errorf("storage URL %q: unsupported query parameter %q (want endpoint, s3ForcePathStyle, or region)", redactedURL(u), key)
		}
	}

	return spec, nil
}

func parseFileURL(u *url.URL) (Spec, error) {
	if len(u.Query()) > 0 {
		return Spec{}, fmt.Errorf("storage URL %q: file: does not accept query parameters", redactedURL(u))
	}

	// Non-hierarchical form (file:relative/path) carries a relative base path.
	if u.Opaque != "" {
		return Spec{Provider: LocalStorageProvider, BasePath: u.Opaque}, nil
	}

	// RFC 8089: the authority must be empty or "localhost"; the path carries
	// the absolute directory, e.g. file:///var/lib/storage.
	if u.Host != "" && u.Host != "localhost" {
		return Spec{}, fmt.Errorf("storage URL %q: file:// must not have a host (use file:///abs/path)", redactedURL(u))
	}
	if u.Path == "" {
		return Spec{}, fmt.Errorf("storage URL %q: file:// requires a path", redactedURL(u))
	}

	return Spec{Provider: LocalStorageProvider, BasePath: u.Path}, nil
}

func validateBucketURL(u *url.URL) error {
	if u.Host == "" {
		return fmt.Errorf("storage URL %q: missing bucket name", redactedURL(u))
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Errorf("storage URL %q: key prefixes are not supported (bucket only)", redactedURL(u))
	}
	if u.User != nil {
		return fmt.Errorf("storage URL %q: credentials in URLs are not supported", redactedURL(u))
	}

	return nil
}
