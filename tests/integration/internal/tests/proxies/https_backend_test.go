package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func generateHTTPSBackendCertificate(t *testing.T) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	var certificatePEM bytes.Buffer
	require.NoError(t, pem.Encode(&certificatePEM, &pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}))
	var privateKeyPEM bytes.Buffer
	require.NoError(t, pem.Encode(&privateKeyPEM, &pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER}))

	return certificatePEM.String(), privateKeyPEM.String()
}

func startHTTPSBackendInSandbox(t *testing.T, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, port int) {
	t.Helper()

	certificate, privateKey := generateHTTPSBackendCertificate(t)
	utils.UploadFile(t, ctx, sbx, envdClient, "/tmp/https-backend-cert.pem", certificate)
	utils.UploadFile(t, ctx, sbx, envdClient, "/tmp/https-backend-key.pem", privateKey)

	serverScript := `
import http.server
import ssl
import sys

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"https backend")

    def log_message(self, format, *args):
        pass

server = http.server.ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/tmp/https-backend-cert.pem", "/tmp/https-backend-key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
`

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverReq := connect.NewRequest(&process.StartRequest{
		Process: &process.ProcessConfig{
			Cmd:  "python3",
			Args: []string{"-c", serverScript, strconv.Itoa(port)},
		},
	})
	setup.SetSandboxHeader(t, serverReq.Header(), sbx)
	setup.SetUserHeader(t, serverReq.Header(), "user")
	serverStream, err := envdClient.ProcessClient.Start(serverCtx, serverReq)
	require.NoError(t, err)

	t.Cleanup(func() {
		serverCancel()
		if streamErr := serverStream.Close(); streamErr != nil {
			t.Logf("Error closing HTTPS server stream: %v", streamErr)
		}
	})
}

func TestSandboxProxyHTTPSBackend(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := setup.GetAPIClient()
	port := 8443
	httpsPorts := []uint32{uint32(port)}
	sbx := utils.SetupSandboxWithCleanup(
		t,
		client,
		utils.WithTimeout(120),
		utils.WithNetwork(&api.SandboxNetworkConfig{HttpsPorts: &httpsPorts}),
	)
	envdClient := setup.GetEnvdClient(t, ctx)
	startHTTPSBackendInSandbox(t, ctx, sbx, envdClient, port)

	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)
	response := utils.WaitForStatus(t, &http.Client{Timeout: 10 * time.Second}, sbx, proxyURL, port, nil, http.StatusOK)
	require.NotNil(t, response)
	t.Cleanup(func() { require.NoError(t, response.Body.Close()) })

	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	assert.Equal(t, "https backend", string(body))
}
