//go:build linux

package oci

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/oci/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func createFileTar(t *testing.T, fileName string) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Add a file to the tarball
	content := []byte("layer text")
	err := tw.WriteHeader(&tar.Header{
		Name: fileName + ".txt",
		Mode: 0o600,
		Size: int64(len(content)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	tw.Close()

	return &buf
}

func TestCreateExportLayersOrder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	logger := logger.NewNopLogger()

	// Create a dummy image with some layers
	img := empty.Image
	layer1, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(createFileTar(t, "layer0").Bytes())), nil
	})
	require.NoError(t, err)
	img, err = mutate.AppendLayers(img, layer1)
	require.NoError(t, err)

	layer2, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(createFileTar(t, "layer1").Bytes())), nil
	})
	require.NoError(t, err)
	img, err = mutate.AppendLayers(img, layer2)
	require.NoError(t, err)

	layer3, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(createFileTar(t, "layer2").Bytes())), nil
	})
	require.NoError(t, err)
	img, err = mutate.AppendLayers(img, layer3)
	require.NoError(t, err)

	// Export the layers
	dir := t.TempDir()
	layers, err := createExport(ctx, logger, img, dir)
	require.NoError(t, err)
	require.NotNil(t, layers)

	// Layers should be in reverse order
	assert.Len(t, layers, 3)
	assert.Regexp(t, "/layer-2.*", strings.TrimPrefix(layers[0], dir))
	assert.FileExists(t, filepath.Join(layers[0], "layer2.txt"))
	assert.Regexp(t, "/layer-1.*", strings.TrimPrefix(layers[1], dir))
	assert.FileExists(t, filepath.Join(layers[1], "layer1.txt"))
	assert.Regexp(t, "/layer-0.*", strings.TrimPrefix(layers[2], dir))
	assert.FileExists(t, filepath.Join(layers[2], "layer0.txt"))
}

func createLayerWithWhiteout(t *testing.T, whiteoutPath string) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := tw.WriteHeader(&tar.Header{
		Name: ".wh." + whiteoutPath,
		Mode: 0o644,
		Size: 0,
	})
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	return &buf
}

func TestCreateExportHandlesOCIWhiteout(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	logger := logger.NewNopLogger()

	layer0Content := []byte("old-version")
	layer0Buf := &bytes.Buffer{}
	tw0 := tar.NewWriter(layer0Buf)
	require.NoError(t, tw0.WriteHeader(&tar.Header{Name: "stale-file.txt", Mode: 0o644, Size: int64(len(layer0Content))}))
	_, err := tw0.Write(layer0Content)
	require.NoError(t, err)
	require.NoError(t, tw0.Close())

	layer1Buf := createLayerWithWhiteout(t, "stale-file.txt")

	img := empty.Image
	layer0, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(layer0Buf.Bytes())), nil
	})
	require.NoError(t, err)
	img, err = mutate.AppendLayers(img, layer0)
	require.NoError(t, err)

	layer1, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(layer1Buf.Bytes())), nil
	})
	require.NoError(t, err)
	img, err = mutate.AppendLayers(img, layer1)
	require.NoError(t, err)

	dir := t.TempDir()
	layerPaths, err := createExport(ctx, logger, img, dir)
	require.NoError(t, err)
	require.Len(t, layerPaths, 2)
	upperLayerDir := layerPaths[0]
	lowerLayerDir := layerPaths[1]

	lowerFile := filepath.Join(lowerLayerDir, "stale-file.txt")
	assert.FileExists(t, lowerFile)

	whPath := filepath.Join(upperLayerDir, ".wh.stale-file.txt")
	_, err = os.Stat(whPath)
	assert.True(t, os.IsNotExist(err), "whiteout entry .wh.stale-file.txt must not remain as a regular file; it should be processed by Untar with WhiteoutFormat")

	upperWhiteoutPath := filepath.Join(upperLayerDir, "stale-file.txt")
	info, err := os.Stat(upperWhiteoutPath)
	require.NoError(t, err, "upper layer must contain whiteout device at stale-file.txt so overlay merge hides lower layer's file")
	assert.NotEqual(t, os.FileMode(0), info.Mode()&os.ModeCharDevice, "stale-file.txt in upper layer must be a character device (OCI whiteout), got %s", info.Mode())
}

// TestCreateExportPreservesEscapingRelativeSymlink reproduces layers produced
// by `nix store optimise`: /nix/store/.links contains symlinks whose relative
// targets (e.g. ../../../../../etc/environment) lexically escape the layer
// directory but are clamped at / by the kernel at runtime
func TestCreateExportPreservesEscapingRelativeSymlink(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	logger := logger.NewNopLogger()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, dir := range []string{"nix", "nix/store", "nix/store/.links", "etc"} {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name:     dir + "/",
			Typeflag: tar.TypeDir,
			Mode:     0o755,
		}))
	}

	fileContent := []byte("PATH=/usr/bin\n")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "etc/environment",
		Typeflag: tar.TypeReg,
		Mode:     0o644,
		Size:     int64(len(fileContent)),
	}))
	_, err := tw.Write(fileContent)
	require.NoError(t, err)

	// The problematic entry: 5 levels up from a directory only 3 levels deep.
	escapingTarget := "../../../../../etc/environment"
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "nix/store/.links/abc",
		Typeflag: tar.TypeSymlink,
		Linkname: escapingTarget,
		Mode:     0o777,
	}))

	// A regular relative symlink that stays inside the layer.
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "nix/store/link-within",
		Typeflag: tar.TypeSymlink,
		Linkname: "../../etc/environment",
		Mode:     0o777,
	}))

	require.NoError(t, tw.Close())

	img := empty.Image
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
	})
	require.NoError(t, err)
	img, err = mutate.AppendLayers(img, layer)
	require.NoError(t, err)

	layerPaths, err := createExport(ctx, logger, img, t.TempDir())
	require.NoError(t, err)
	require.Len(t, layerPaths, 1)
	layerPath := layerPaths[0]

	target, err := os.Readlink(filepath.Join(layerPath, "nix/store/.links/abc"))
	require.NoError(t, err)
	assert.Equal(t, escapingTarget, target, "symlink target must be preserved verbatim")

	target, err = os.Readlink(filepath.Join(layerPath, "nix/store/link-within"))
	require.NoError(t, err)
	assert.Equal(t, "../../etc/environment", target)

	content, err := os.ReadFile(filepath.Join(layerPath, "etc/environment"))
	require.NoError(t, err)
	assert.Equal(t, fileContent, content)
}

// authHandler wraps a registry handler with basic authentication
func authHandler(handler http.Handler, username, password string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow certain endpoints without auth (like /v2/)
		if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
			handler.ServeHTTP(w, r)

			return
		}

		// Check for Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			// Send WWW-Authenticate challenge
			w.Header().Set("WWW-Authenticate", `Basic realm="Registry"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		// Parse and validate credentials
		user, pass, ok := r.BasicAuth()
		if !ok || user != username || pass != password {
			w.Header().Set("WWW-Authenticate", `Basic realm="Registry"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		// Credentials are valid, proceed to the handler
		handler.ServeHTTP(w, r)
	})
}

func TestGetPublicImageWithGeneralAuth(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Create a test image
	testImage := empty.Image
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(createFileTar(t, "test-layer").Bytes())), nil
	})
	require.NoError(t, err)

	testImage, err = mutate.AppendLayers(testImage, layer)
	require.NoError(t, err)

	// Set the config to include the proper platform
	configFile, err := testImage.ConfigFile()
	require.NoError(t, err)
	configFile.Architecture = utils.TargetArch()
	configFile.OS = "linux"
	testImage, err = mutate.ConfigFile(testImage, configFile)
	require.NoError(t, err)

	// Test credentials
	testUsername := "testuser"
	testPassword := "testpass"

	testRepository := "test/image"
	testImageRef := testRepository + ":latest"

	dockerhubRepository := dockerhub.NewNoopRemoteRepository()
	t.Cleanup(func() {
		err := dockerhubRepository.Close()
		if err != nil {
			t.Errorf("error closing dockerhub repository: %v", err)
		}
	})

	t.Run("successful auth and pull", func(t *testing.T) {
		t.Parallel()
		reg := registry.New()

		// Wrap the registry with authentication handler
		authReg := authHandler(reg, testUsername, testPassword)

		// Start the test server with auth handler
		server := httptest.NewServer(authReg)
		defer server.Close()

		// Parse server URL to get registry host
		host := strings.TrimPrefix(server.URL, "http://")

		// Push test image to the mock registry first
		imageRef := path.Join(host, testImageRef)
		ref, err := name.ParseReference(imageRef, name.Insecure)
		require.NoError(t, err)

		// Push image to registry
		err = remote.Write(ref, testImage, remote.WithAuth(
			&authn.Basic{
				Username: testUsername,
				Password: testPassword,
			},
		))
		require.NoError(t, err)

		// Create general auth provider with test credentials
		generalRegistry := &templatemanager.GeneralRegistry{
			Username: testUsername,
			Password: testPassword,
		}
		authProvider := auth.NewGeneralAuthProvider(generalRegistry)

		// Test that auth provider creates correct auth option
		authOption, err := authProvider.GetAuthOption(ctx)
		require.NoError(t, err)
		require.NotNil(t, authOption)

		// Now test GetPublicImage
		img, err := GetPublicImage(ctx, dockerhubRepository, imageRef, authProvider)
		require.NoError(t, err)
		require.NotNil(t, img)

		// Verify we got the right image
		layers, err := img.Layers()
		require.NoError(t, err)
		assert.Len(t, layers, 1)
	})

	t.Run("incorrect auth", func(t *testing.T) {
		t.Parallel()
		reg := registry.New()

		// Wrap the registry with authentication handler
		authReg := authHandler(reg, testUsername, testPassword)

		// Start the test server with auth handler
		server := httptest.NewServer(authReg)
		defer server.Close()

		// Parse server URL to get registry host
		host := strings.TrimPrefix(server.URL, "http://")

		// Push test image to the mock registry first
		imageRef := path.Join(host, testImageRef)
		ref, err := name.ParseReference(imageRef, name.Insecure)
		require.NoError(t, err)

		// Push image to registry
		err = remote.Write(ref, testImage, remote.WithAuth(
			&authn.Basic{
				Username: testUsername,
				Password: testPassword,
			},
		))
		require.NoError(t, err)

		// Create general auth provider with test credentials
		generalRegistry := &templatemanager.GeneralRegistry{
			Username: "incorrect",
			Password: "incorrect",
		}
		authProvider := auth.NewGeneralAuthProvider(generalRegistry)

		// Test that auth provider creates correct auth option
		authOption, err := authProvider.GetAuthOption(ctx)
		require.NoError(t, err)
		require.NotNil(t, authOption)

		// Now test GetPublicImage
		img, err := GetPublicImage(ctx, dockerhubRepository, imageRef, authProvider)
		require.Error(t, err)
		require.Nil(t, img)
	})

	t.Run("works without auth for public registry", func(t *testing.T) {
		t.Parallel()
		// Create a mock registry without authentication (public)
		reg := registry.New()

		// Start the test server
		server := httptest.NewServer(reg)
		defer server.Close()

		// Parse server URL to get registry host
		host := strings.TrimPrefix(server.URL, "http://")

		// Push test image to the mock registry (no auth needed)
		imageRef := path.Join(host, testImageRef)
		ref, err := name.ParseReference(imageRef, name.Insecure)
		require.NoError(t, err)

		err = remote.Write(ref, testImage)
		require.NoError(t, err)

		// Get image without auth provider (nil)
		img, err := GetPublicImage(ctx, dockerhubRepository, imageRef, nil)
		require.NoError(t, err)
		require.NotNil(t, img)

		// Verify we got the right image
		layers, err := img.Layers()
		require.NoError(t, err)
		assert.Len(t, layers, 1)
	})
}

func TestWrapImagePullErrorSanitizesOriginalError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	imageRef := "registry.example.com/test/image:latest"

	remoteErr := &transport.Error{
		StatusCode: http.StatusTeapot,
		Errors: []transport.Diagnostic{
			{
				Code:    transport.UnknownErrorCode,
				Message: "registry-controlled message",
				Detail:  "registry-controlled detail",
			},
		},
	}

	err := wrapImagePullError(ctx, remoteErr, imageRef)
	require.EqualError(t, err, "failed to pull image 'registry.example.com/test/image:latest': registry returned status code 418")
	assert.NotContains(t, err.Error(), "registry-controlled message")
	assert.NotContains(t, err.Error(), "registry-controlled detail")
	assert.NotErrorIs(t, err, remoteErr)
}

func TestWrapImagePullErrorUsesPredefinedRegistryCodeMessage(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	imageRef := "registry.example.com/test/image:latest"

	remoteErr := &transport.Error{
		StatusCode: http.StatusUnauthorized,
		Errors: []transport.Diagnostic{
			{
				Code:    transport.UnauthorizedErrorCode,
				Message: "registry-controlled message",
			},
		},
	}

	err := wrapImagePullError(ctx, remoteErr, imageRef)
	require.EqualError(t, err, "access denied to 'registry.example.com/test/image:latest': authentication required or insufficient permissions")
	assert.NotContains(t, err.Error(), "registry-controlled message")
	assert.NotErrorIs(t, err, remoteErr)
}

func TestWrapImagePullErrorKeepsProxyClassificationOnly(t *testing.T) {
	t.Parallel()
	original := &transport.Error{StatusCode: http.StatusForbidden, Errors: []transport.Diagnostic{{Code: transport.DeniedErrorCode, Message: "private registry details"}}}
	err := wrapImagePullError(t.Context(), fmt.Errorf("%w: %w", dockerhub.ErrRepositoryAccessDenied, original), "ubuntu:24.04")
	require.ErrorIs(t, err, dockerhub.ErrRepositoryAccessDenied)
	require.NotErrorIs(t, err, original)
	assert.NotContains(t, err.Error(), "private registry details")
	assert.NotContains(t, err.Error(), "you don't have permission")
}

//nolint:paralleltest // replaces the global logger to capture the trusted field
func TestWrapImagePullErrorLogsTrustedProxyClassification(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))
	spoofed := &transport.Error{StatusCode: http.StatusForbidden, Errors: []transport.Diagnostic{{
		Code:    transport.DeniedErrorCode,
		Message: dockerhub.ErrRepositoryAccessDenied.Error() + ` {"dockerhub_proxy_access_denied":true}`,
	}}}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{spoofed, false},
		{fmt.Errorf("%w: %w", dockerhub.ErrRepositoryAccessDenied, spoofed), true},
	} {
		err := wrapImagePullError(t.Context(), tc.err, "registry.example.com/test/image:latest")
		require.Error(t, err)
		entries := logs.TakeAll()
		require.Len(t, entries, 1)
		assert.Equal(t, tc.want, entries[0].ContextMap()["dockerhub_proxy_access_denied"])
	}
}
