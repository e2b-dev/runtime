//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// The cap must not change what a message keeps: every body yields the text a full read gave.
func TestEnvdErrorBodyMatchesAFullRead(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"empty":                   "",
		"short":                   "envd: bad request",
		"exactly 100 ascii runes": strings.Repeat("a", 100),
		"long ascii":              strings.Repeat("a", 10_000),
		// 400 bytes hold exactly these 100 runes, so a 400-byte cap would hide that more followed.
		"100 four-byte runes, then more": strings.Repeat("😀", 100) + "x",
		"exactly 100 four-byte runes":    strings.Repeat("😀", 100),
		"invalid utf-8":                  strings.Repeat("\xff", 1_000),
		"two-byte runes":                 strings.Repeat("é", 1_000),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, utils.Truncate(body, 100), envdErrorBody(strings.NewReader(body)))
		})
	}
}

// oversizedBody is far past the socket buffers between the test server and the client, so
// the server finishes writing it only when the client reads it.
const oversizedBody = 64 << 20

// oversizedEnvd answers every request with status and a body of prefix followed by
// oversizedBody bytes of 'e'. delivered reports whether the whole body was written.
func oversizedEnvd(t *testing.T, status int, prefix string) (string, <-chan bool) {
	t.Helper()

	delivered := make(chan bool, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(status)

		_, err := io.WriteString(w, prefix)
		chunk := bytes.Repeat([]byte{'e'}, 32<<10)
		for sent := 0; err == nil && sent < oversizedBody; sent += len(chunk) {
			_, err = w.Write(chunk)
		}
		delivered <- err == nil
	}))
	t.Cleanup(srv.Close)

	return srv.URL, delivered
}

func TestEnvdCallsReadABoundedBody(t *testing.T) {
	t.Parallel()

	const timeout = 10 * time.Second
	truncated := strings.Repeat("e", 97) + "..."

	upgradeBin := filepath.Join(t.TempDir(), "envd")
	require.NoError(t, os.WriteFile(upgradeBin, []byte("binary"), 0o600))

	for name, tc := range map[string]struct {
		status int
		prefix string
		// call runs the envd call against s and returns the text the body reached.
		call func(ctx context.Context, t *testing.T, s *Sandbox, id string) string
		want string
	}{
		"freeze error": {
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, t *testing.T, s *Sandbox, _ string) string {
				t.Helper()
				_, _, err := s.callEnvdFreeze(ctx, timeout, false, 0)
				require.Error(t, err)

				return err.Error()
			},
			want: "freeze returned 500: " + truncated,
		},
		"collapse error": {
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, t *testing.T, s *Sandbox, _ string) string {
				t.Helper()
				_, err := s.callEnvdCollapse(ctx, timeout)
				require.Error(t, err)

				return err.Error()
			},
			want: "collapse returned 500: " + truncated,
		},
		"post error": {
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, t *testing.T, s *Sandbox, _ string) string {
				t.Helper()
				err := s.postEnvd(ctx, timeout, "fsfreeze")
				require.Error(t, err)

				return err.Error()
			},
			want: "fsfreeze returned 500: " + truncated,
		},
		"upgrade error": {
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, t *testing.T, s *Sandbox, _ string) string {
				t.Helper()
				_, err := s.CallEnvdUpgrade(ctx, upgradeBin, "/usr/bin/envd", timeout)
				require.Error(t, err)

				return err.Error()
			},
			want: "upgrade returned 500: " + truncated,
		},
		"init error": {
			status: http.StatusInternalServerError,
			call: func(ctx context.Context, t *testing.T, s *Sandbox, id string) string {
				t.Helper()
				err := s.initEnvd(ctx, StartTypeResume, false)
				require.EqualError(t, err, "unexpected status code: 500")

				return initFailureBody(t, "sbx-"+id)
			},
			want: truncated,
		},
		"freeze result": {
			status: http.StatusOK,
			prefix: `{"frozen":1,"pad":"`,
			call: func(ctx context.Context, t *testing.T, s *Sandbox, _ string) string {
				t.Helper()
				_, _, err := s.callEnvdFreeze(ctx, timeout, false, 0)
				require.Error(t, err)

				return err.Error()
			},
			want: "decode freeze result: unexpected EOF",
		},
		"collapse result": {
			status: http.StatusOK,
			prefix: `{"chunks":1,"pad":"`,
			call: func(ctx context.Context, t *testing.T, s *Sandbox, _ string) string {
				t.Helper()
				_, err := s.callEnvdCollapse(ctx, timeout)
				require.Error(t, err)

				return err.Error()
			},
			want: "decode collapse result: unexpected EOF",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			url, delivered := oversizedEnvd(t, tc.status, tc.prefix)
			s, id := newMemoryTestSandbox(t, url)

			assert.Equal(t, tc.want, tc.call(t.Context(), t, s, id))
			assert.False(t, <-delivered, "the client read the whole %d MiB body", oversizedBody>>20)
		})
	}
}

// The JSON cap leaves room for a real result: one within it still decodes.
func TestEnvdResultWithinCapDecodes(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"frozen":3,"requested":3,"pad":"`+strings.Repeat("e", 64<<10)+`"}`)
	}))
	t.Cleanup(srv.Close)
	s, _ := newMemoryTestSandbox(t, srv.URL)

	res, ok, err := s.callEnvdFreeze(t.Context(), 10*time.Second, false, 0)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 3, res.Frozen)
}

// initFailureBody returns the response_body logged for one sandbox's failed /init.
func initFailureBody(t *testing.T, sandboxID string) string {
	t.Helper()

	var bodies []string
	for _, e := range testLogObserver.FilterMessage("envd init request failed").All() {
		for _, f := range e.Context {
			if f.String == sandboxID {
				bodies = append(bodies, e.ContextMap()["response_body"].(string))
			}
		}
	}
	require.Len(t, bodies, 1, "expected exactly one init failure logged for %s", sandboxID)

	return bodies[0]
}
