package storage

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/option"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

//nolint:paralleltest // Exercises the existing process-wide service account configuration.
func TestGCPSignedUploadRetainsPrivateKeySigning(t *testing.T) {
	// This test changes process-wide configuration and must run serially.
	previous := consts.GoogleServiceAccountSecret
	t.Cleanup(func() { consts.GoogleServiceAccountSecret = previous })
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	const account = "legacy-uploads@example.iam.gserviceaccount.com"
	credentials, err := json.Marshal(map[string]string{
		"client_email": account,
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
	})
	require.NoError(t, err)
	consts.GoogleServiceAccountSecret = base64.StdEncoding.EncodeToString(credentials)
	client, err := gcs.NewClient(t.Context(), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := &gcpStorage{bucket: client.Bucket("templates")}
	result, err := store.UploadSignedURL(t.Context(), "build/rootfs", time.Minute)
	require.NoError(t, err)
	parsed, err := url.Parse(result.URL)
	require.NoError(t, err)
	require.Equal(t, account, parsed.Query().Get("GoogleAccessId"))
	signature, err := base64.StdEncoding.DecodeString(parsed.Query().Get("Signature"))
	require.NoError(t, err)
	digest := sha256.Sum256([]byte("PUT\n\n\n" + parsed.Query().Get("Expires") + "\n/templates/build/rootfs"))
	require.NoError(t, rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature))

	consts.GoogleServiceAccountSecret = "invalid!"
	_, err = store.UploadSignedURL(t.Context(), "build/rootfs", time.Minute)
	require.ErrorContains(t, err, "failed to parse GCP service account")
}

func TestGCPSignedUploadUsesIAM(t *testing.T) {
	t.Parallel()
	const account = "uploads@example.iam.gserviceaccount.com"
	var payload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/projects/-/serviceAccounts/"+account+":signBlob", r.URL.Path)
		var request iamcredentials.SignBlobRequest
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&request)) {
			return
		}
		raw, err := base64.StdEncoding.DecodeString(request.Payload)
		if !assert.NoError(t, err) {
			return
		}
		payload = string(raw)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"signedBlob":"c2lnbmF0dXJl"}`))
	}))
	t.Cleanup(server.Close)
	client, err := gcs.NewClient(t.Context(), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	signer, err := iamcredentials.NewService(t.Context(), option.WithEndpoint(server.URL), option.WithHTTPClient(server.Client()))
	require.NoError(t, err)
	store := &gcpStorage{bucket: client.Bucket("templates"), signingClient: signer, signingServiceAccount: account}
	result, err := store.UploadSignedURL(t.Context(), "build/rootfs", time.Minute)
	require.NoError(t, err)
	parsed, err := url.Parse(result.URL)
	require.NoError(t, err)
	require.Equal(t, account, parsed.Query().Get("GoogleAccessId"))
	require.Equal(t, "c2lnbmF0dXJl", parsed.Query().Get("Signature"))
	require.Contains(t, payload, "PUT\n")
	require.Contains(t, payload, "/templates/build/rootfs")
}

func TestGCPSignedUploadFailsClosed(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "signing denied", http.StatusForbidden)
	}))
	t.Cleanup(server.Close)
	client, err := gcs.NewClient(t.Context(), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	signer, err := iamcredentials.NewService(t.Context(), option.WithEndpoint(server.URL), option.WithHTTPClient(server.Client()))
	require.NoError(t, err)
	store := &gcpStorage{bucket: client.Bucket("templates"), signingClient: signer, signingServiceAccount: "uploads@example.iam.gserviceaccount.com"}
	result, err := store.UploadSignedURL(t.Context(), "build/rootfs", time.Minute)
	require.ErrorContains(t, err, "403")
	require.Empty(t, result.URL)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = store.UploadSignedURL(ctx, "build/rootfs", time.Minute)
	require.ErrorIs(t, err, context.Canceled)
}
