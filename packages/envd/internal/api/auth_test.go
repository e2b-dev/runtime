package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

func newAuthTestAPI(t *testing.T, token string) *API {
	t.Helper()
	secureToken := &SecureToken{}
	err := secureToken.Set([]byte(token))
	require.NoError(t, err)
	logger := zerolog.Nop()

	return &API{accessToken: secureToken, logger: &logger}
}

func TestKeyGenerationAlgorithmIsStable(t *testing.T) {
	t.Parallel()
	apiToken := "secret-access-token"
	api := newAuthTestAPI(t, apiToken)

	path := "/path/to/demo.txt"
	username := "root"
	operation := "write"
	timestampSec := time.Now().Unix()

	signature, err := api.generateSignature(path, username, operation, &timestampSec)
	require.NoError(t, err)
	assert.NotEmpty(t, signature)

	// locally generated signature
	hasher := keys.NewSHA256Hashing()
	localSignatureTmp := fmt.Sprintf("%s:%s:%s:%s:%s", path, operation, username, apiToken, strconv.FormatInt(timestampSec, 10))
	localSignature := fmt.Sprintf("v1_%s", hasher.HashWithoutPrefix([]byte(localSignatureTmp)))

	assert.Equal(t, localSignature, signature)
}

func TestKeyGenerationAlgorithmWithoutExpirationIsStable(t *testing.T) {
	t.Parallel()
	apiToken := "secret-access-token"
	api := newAuthTestAPI(t, apiToken)

	path := "/path/to/resource.txt"
	username := "user"
	operation := "read"

	signature, err := api.generateSignature(path, username, operation, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, signature)

	// locally generated signature
	hasher := keys.NewSHA256Hashing()
	localSignatureTmp := fmt.Sprintf("%s:%s:%s:%s", path, operation, username, apiToken)
	localSignature := fmt.Sprintf("v1_%s", hasher.HashWithoutPrefix([]byte(localSignatureTmp)))

	assert.Equal(t, localSignature, signature)
}

func TestValidateSigningAcceptsCorrectSignature(t *testing.T) {
	t.Parallel()
	api := newAuthTestAPI(t, "test-token")

	path := "/files"
	username := "user1"
	operation := SigningWriteOperation
	expSec := time.Now().Add(time.Hour).Unix()

	sig, err := api.generateSignature(path, username, operation, &expSec)
	require.NoError(t, err)

	expInt := int(expSec)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files", nil)
	err = api.validateSigning(r, &sig, &expInt, &username, path, operation)
	assert.NoError(t, err)
}

func TestValidateSigningRejectsWrongSignature(t *testing.T) {
	t.Parallel()
	api := newAuthTestAPI(t, "test-token")

	wrong := "v1_wrong_signature"
	exp := int(time.Now().Add(time.Hour).Unix())
	username := "user1"

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files", nil)
	err := api.validateSigning(r, &wrong, &exp, &username, "/files", SigningWriteOperation)
	assert.EqualError(t, err, "invalid signature")
}

func TestValidateSigningRejectsExpiredSignature(t *testing.T) {
	t.Parallel()
	api := newAuthTestAPI(t, "test-token")

	path := "/files"
	username := "user1"
	operation := SigningReadOperation
	// Expired 1 hour ago
	expSec := time.Now().Add(-time.Hour).Unix()

	sig, err := api.generateSignature(path, username, operation, &expSec)
	require.NoError(t, err)

	expInt := int(expSec)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files", nil)
	err = api.validateSigning(r, &sig, &expInt, &username, path, operation)
	assert.EqualError(t, err, "signature is already expired")
}

func TestValidateSigningRejectsMissingSignature(t *testing.T) {
	t.Parallel()
	api := newAuthTestAPI(t, "test-token")

	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files", nil)
	err := api.validateSigning(r, nil, nil, nil, "/files", SigningReadOperation)
	assert.EqualError(t, err, "missing signature query parameter")
}

func TestValidateSigningAcceptsValidAccessTokenHeader(t *testing.T) {
	t.Parallel()
	api := newAuthTestAPI(t, "test-token")

	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files", nil)
	r.Header.Set(accessTokenHeader, "test-token")
	err := api.validateSigning(r, nil, nil, nil, "/files", SigningReadOperation)
	assert.NoError(t, err)
}

func TestValidateSigningRejectsInvalidAccessTokenHeader(t *testing.T) {
	t.Parallel()
	api := newAuthTestAPI(t, "test-token")

	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files", nil)
	r.Header.Set(accessTokenHeader, "wrong-token")
	err := api.validateSigning(r, nil, nil, nil, "/files", SigningReadOperation)
	assert.EqualError(t, err, "access token present in header but does not match")
}

// A user name with a colon is refused, even with a signature made for it.
func TestValidateSigningRejectsAColonInTheUserName(t *testing.T) {
	t.Parallel()
	api := newAuthTestAPI(t, "test-token")

	username := "a:b"
	sig, err := api.generateSignature("/files", username, SigningWriteOperation, nil)
	require.NoError(t, err)

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files", nil)
	assert.Error(t, api.validateSigning(r, &sig, nil, &username, "/files", SigningWriteOperation))
}
