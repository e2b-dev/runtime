package utils

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
)

func NewRequest(sbx *api.Sandbox, url *url.URL, port int, extraHeaders *http.Header) *http.Request {
	var host string

	if url.Hostname() == "localhost" {
		host = fmt.Sprintf("%d-%s-%s.%s", port, sbx.SandboxID, sbx.ClientID, "localhost")
	} else {
		proxyHost := url.Hostname()
		labels := strings.Split(proxyHost, ".")
		// Expected proxy host shape is something like: "<proxy>.<routing-domain>".
		routingDomain := strings.Join(labels[1:], ".")

		// Just in case it's something crazy
		if routingDomain == "" {
			routingDomain = proxyHost
		}

		portSuffix := ""
		if proxyPort := url.Port(); proxyPort != "" {
			portSuffix = ":" + proxyPort
		}
		host = fmt.Sprintf("%d-%s-%s.%s%s", port, sbx.SandboxID, sbx.ClientID, routingDomain, portSuffix)
	}

	header := http.Header{
		"Host": []string{host},
	}

	if sbx.TrafficAccessToken != nil {
		header.Set("e2b-traffic-access-token", *sbx.TrafficAccessToken)
	}

	if extraHeaders != nil {
		maps.Copy(header, *extraHeaders)
	}

	return &http.Request{
		Method: http.MethodGet,
		URL:    url,
		Host:   host,
		Header: header,
	}
}

func WaitForStatus(tb testing.TB, client *http.Client, sbx *api.Sandbox, url *url.URL, port int, headers *http.Header, expectedStatus int) *http.Response {
	tb.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req := NewRequest(sbx, url, port, headers)
		resp, err := client.Do(req)
		if err != nil {
			tb.Logf("Error: %v", err)
			time.Sleep(500 * time.Millisecond)

			continue
		}

		if resp.StatusCode == expectedStatus {
			return resp
		}

		x, err := io.ReadAll(resp.Body)
		if err != nil {
			tb.Logf("[Status code: %d] Error reading response body: %v", resp.StatusCode, err)
		} else {
			tb.Logf("[Status code: %d] Response body: %s", resp.StatusCode, string(x))
		}
		require.NoError(tb, resp.Body.Close())

		time.Sleep(500 * time.Millisecond)
	}

	assert.Fail(tb, "the sandbox did not return the expected status within 60s", "expected status %d", expectedStatus)

	return nil
}
