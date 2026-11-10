package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// gatedBody hands its content to the handler only once release is closed, and closes reading
// at the handler's first body read: by then the handler has read its freeze generation.
type gatedBody struct {
	r       io.Reader
	once    sync.Once
	reading chan struct{}
	release chan struct{}
}

func newGatedBody(raw []byte) *gatedBody {
	return &gatedBody{r: bytes.NewReader(raw), reading: make(chan struct{}), release: make(chan struct{})}
}

func (g *gatedBody) Read(p []byte) (int, error) {
	g.once.Do(func() { close(g.reading) })
	<-g.release

	return g.r.Read(p)
}

// servedOnce serves the envd router behind WithAuthorization, as main.go mounts it but without
// CORS and the Connect authn middleware, and closes done when the one request it is given has
// been handled.
func servedOnce(api *API) (http.Handler, <-chan struct{}) {
	done := make(chan struct{})
	h := api.WithAuthorization(HandlerFromMux(api, chi.NewRouter()))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		h.ServeHTTP(w, r)
	}), done
}

// sendInitHeaders opens a connection to srv and writes a /init request's headers with
// Expect: 100-continue and the body withheld, as an orchestrator that holds the body until
// the 100 Continue would, then waits for the server's 100 Continue: Go writes it at the handler's first body read.
func sendInitHeaders(t *testing.T, srv *httptest.Server, bodyLen int) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", srv.Listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	_, err = fmt.Fprintf(conn, "POST /init HTTP/1.1\r\nHost: envd\r\nContent-Type: application/json\r\nContent-Length: %d\r\nExpect: 100-continue\r\n\r\n", bodyLen)
	require.NoError(t, err)

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "HTTP/1.1 100 Continue", strings.TrimSpace(status))
	blank, err := br.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "\r\n", blank)

	return conn, br
}

// withheldThawLog returns the fields of the line a handler logs when it withholds its thaw.
func withheldThawLog(t *testing.T, out string) map[string]any {
	t.Helper()

	for line := range strings.SplitSeq(out, "\n") {
		if !strings.Contains(line, "not thawing the workload") {
			continue
		}
		var fields map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &fields))

		return fields
	}
	t.Fatalf("no withheld-thaw line in the log:\n%s", out)

	return nil
}

// A handler that a freeze overtook -- it entered, then a pre-pause freeze ran while it waited
// for its body -- answers 204 but thaws nothing, and logs the generation it entered with
// beside the one it found.
func TestPostInit_FreezeAfterEntryWithholdsTheThaw(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	var logs bytes.Buffer
	api := newAPIWithCgroupManagerLogging(mgr, &logs)
	api.isNotFC = true
	entered := api.workloadFreezer.FreezeGeneration(t.Context())

	body := newGatedBody([]byte(`{}`))
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", body)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		api.PostInit(rec, req)
	}()

	<-body.reading
	freezeAPI(t, api)
	close(body.release)
	<-done

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, mgr.unfreezeAttempts, "the handler must not undo a freeze that came after it entered")
	fields := withheldThawLog(t, logs.String())
	assert.InDelta(t, float64(entered), fields["entry_freeze_generation"], 0)
	assert.InDelta(t, float64(entered+1), fields["current_freeze_generation"], 0)
}

// Under Expect: 100-continue the generation is read before the server's 100 Continue goes
// out: a freeze that lands after the 100 and before the body is sent has overtaken the
// handler, which then thaws nothing.
func TestPostInit_FreezeBetweenContinueAndBodyWithholdsTheThaw(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true
	h, done := servedOnce(api)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	raw := []byte(`{}`)
	conn, br := sendInitHeaders(t, srv, len(raw))
	freezeAPI(t, api)
	_, err := conn.Write(raw)
	require.NoError(t, err)

	resp, err := http.ReadResponse(br, nil)
	require.NoError(t, err)
	resp.Body.Close()
	<-done

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, mgr.unfreezeAttempts, "the freeze landed after the handler read its generation")
}

// A held request that a client gives up on after the 100 Continue, sending no body, fails its
// body read before auth, where the thaw is installed, so it thaws nothing even though it
// entered after the freeze.
func TestPostInit_ClientGoneBeforeTheBodyThawsNothing(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true
	h, done := servedOnce(api)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	freezeAPI(t, api)
	conn, _ := sendInitHeaders(t, srv, len(`{}`))
	require.NoError(t, conn.Close())
	<-done

	assert.Empty(t, mgr.unfreezeAttempts, "a handler with no body never reaches the thaw")
}

// A handler that enters after a freeze holds the new generation and thaws it: the resume's
// own /init.
func TestPostInit_EntryAfterTheFreezeThaws(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true
	freezeAPI(t, api)

	rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{})

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.unfrozen)
}

// A handler that entered while another held the /init lock, with no freeze between their
// entries, holds the same generation and still thaws after the first has thawed (finding
// nothing left): another handler's thaw never withholds a later one.
func TestPostInit_EntryBeforeAnotherThawStillThaws(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	freezeAPI(t, api)

	inLock := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	api.mmdsClient = &mockMMDSClient{onGet: func() {
		once.Do(func() {
			close(inLock)
			<-releaseFirst
		})
	}}

	firstReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", strings.NewReader(`{}`))
	require.NoError(t, err)
	first := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		api.PostInit(rec, firstReq)
		first <- rec.Code
	}()
	<-inLock

	body := newGatedBody([]byte(`{}`))
	close(body.release)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", body)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	second := make(chan struct{})
	go func() {
		defer close(second)
		api.PostInit(rec, req)
	}()
	<-body.reading
	close(releaseFirst)

	require.Equal(t, http.StatusNoContent, <-first)
	<-second
	require.Equal(t, http.StatusNoContent, rec.Code)

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	assert.Len(t, mgr.unfreezeAttempts, 2*len(cgroups.WorkloadProcessTypes), "both handlers ran their thaw")
}

// A process that froze nothing itself -- an envd restarted with the kernel's freeze still in
// place -- thaws on its first /init: its generation is its own, and no freeze of its own has
// moved it.
func TestPostInit_FirstInitOfANewProcessThaws(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{frozen: slices.Clone(cgroups.WorkloadProcessTypes)}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true

	rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{})

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.unfrozen)
}
