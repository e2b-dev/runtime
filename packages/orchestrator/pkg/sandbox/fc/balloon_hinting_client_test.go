//go:build linux

package fc

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	hintStubBlock    = -1
	hintStopAckGrace = 100 * time.Millisecond
)

// hintStub serves the FC balloon hinting API on a unix socket. Each status
// field selects the HTTP status a route answers (0 = success; hintStubBlock =
// hold the request until the client gives up). balloon defaults to one with
// hinting; noBalloon means no balloon device (FC answers 400 on GET /balloon).
// guestDone > 0 plays a guest that completes the cycle that long after a successful
// start; startDelay and configDelay hold the respective answers; a negative
// guestDone completes the cycle inside the start itself.
type hintStub struct {
	start, status, stop, config int
	failStatusFirst             int32
	failConfigFirst             int32
	configCalls                 atomic.Int32
	noBalloon                   bool
	balloon                     map[string]any
	guestDone                   time.Duration
	startDelay, configDelay     time.Duration
	hostCmd                     atomic.Int64
	guestCmd                    atomic.Int64
	statusCalls                 atomic.Int32
	stopCalls                   atomic.Int32
}

func (h *hintStub) answer(w http.ResponseWriter, r *http.Request, status int, body any) {
	switch {
	case status == hintStubBlock:
		<-r.Context().Done()
	case status != 0:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"fault_message":"stub"}`))
	case body == nil:
		w.WriteHeader(http.StatusNoContent)
	default:
		data, err := json.Marshal(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}
}

func newHintProcess(t *testing.T, h *hintStub) *Process {
	t.Helper()
	if h.balloon == nil && !h.noBalloon {
		h.balloon = balloonWithHinting(true)
	}
	dir, err := os.MkdirTemp("", "fc") //nolint:usetesting // t.TempDir embeds the subtest name and overflows the unix sun_path limit
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := dir + "/fc.sock"
	ln, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /balloon/hinting/start", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(h.startDelay)
		if h.start == 0 {
			h.hostCmd.Store(freePageHintFirstCmd)
			switch {
			case h.guestDone < 0:
				// The guest finishes before the start even answers.
				h.guestCmd.Store(freePageHintStop)
				h.hostCmd.Store(freePageHintDone)
			case h.guestDone > 0:
				time.AfterFunc(h.guestDone, func() {
					h.guestCmd.Store(freePageHintStop)
					h.hostCmd.Store(freePageHintDone)
				})
			}
		}
		h.answer(w, r, h.start, nil)
	})
	mux.HandleFunc("PATCH /balloon/hinting/stop", func(w http.ResponseWriter, r *http.Request) {
		h.stopCalls.Add(1)
		if h.stop == 0 {
			h.hostCmd.Store(freePageHintDone)
		}
		h.answer(w, r, h.stop, nil)
	})
	mux.HandleFunc("GET /balloon/hinting/status", func(w http.ResponseWriter, r *http.Request) {
		if h.statusCalls.Add(1) <= h.failStatusFirst {
			h.answer(w, r, http.StatusInternalServerError, nil)

			return
		}
		h.answer(w, r, h.status, map[string]int64{"host_cmd": h.hostCmd.Load(), "guest_cmd": h.guestCmd.Load()})
	})
	mux.HandleFunc("GET /balloon", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(h.configDelay)
		if h.configCalls.Add(1) <= h.failConfigFirst {
			h.answer(w, r, http.StatusInternalServerError, nil)

			return
		}
		if h.noBalloon && h.config == 0 {
			h.answer(w, r, http.StatusBadRequest, nil)

			return
		}
		h.answer(w, r, h.config, h.balloon)
	})

	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	return &Process{
		Versions: Config{FirecrackerVersion: "v1.14.0"},
		Exit:     utils.NewErrorOnce(),
		client:   newApiClient(socket),
	}
}

func balloonWithHinting(hinting bool) map[string]any {
	return map[string]any{
		"amount_mib": 0, "deflate_on_oom": true, "stats_polling_interval_s": 0,
		"free_page_reporting": false, "free_page_hinting": hinting,
	}
}

func shortCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	t.Cleanup(cancel)

	return ctx
}

// graceCtx outlives the stop grace by far: tests on the grace path assert that
// the stop returned within a loose multiple of the grace, not that the
// context expired.
func graceCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)

	return ctx
}

const graceBound = 5 * hintStopAckGrace

// TestDrainBalloon pins the error classification the periodic hinter and the
// pre-pause drain act on, against real FC-shaped responses through the
// go-swagger client.
func TestDrainBalloon(t *testing.T) {
	t.Parallel()

	t.Run("completed cycle", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{guestDone: 20 * time.Millisecond}
		// Steady state after a prior cycle: host_cmd already reads DONE.
		h.hostCmd.Store(freePageHintDone)
		p := newHintProcess(t, h)
		require.NoError(t, p.DrainBalloon(shortCtx(t)))
		assert.Equal(t, freePageHintDone, h.hostCmd.Load())
		assert.GreaterOrEqual(t, h.statusCalls.Load(), int32(2), "the new cycle was polled to completion, not the stale DONE")
	})

	t.Run("balloon without hinting is not configured, settled from the config", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{start: http.StatusBadRequest, balloon: balloonWithHinting(false)}
		p := newHintProcess(t, h)
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingNotConfigured)
		require.NotErrorIs(t, err, ErrHintingInFlight)
		assert.Zero(t, h.statusCalls.Load(), "no hinting request is made on a balloon that cannot hint")
		require.ErrorIs(t, p.DrainBalloon(shortCtx(t)), ErrHintingNotConfigured)
		assert.Equal(t, int32(1), h.configCalls.Load(), "the config is read once per process")
	})

	t.Run("no balloon device is not configured", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{start: http.StatusBadRequest, noBalloon: true})
		require.ErrorIs(t, p.DrainBalloon(shortCtx(t)), ErrHintingNotConfigured)
	})

	t.Run("balloon without hinting via the 400 path when the config is unreadable first", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{start: http.StatusBadRequest, balloon: balloonWithHinting(false), failConfigFirst: 1}
		p := newHintProcess(t, h)
		require.ErrorIs(t, p.DrainBalloon(shortCtx(t)), ErrHintingNotConfigured)
	})

	t.Run("refusal on a hinting balloon is transient", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{start: http.StatusBadRequest, balloon: balloonWithHinting(true)})
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingStartRefused)
		require.NotErrorIs(t, err, ErrHintingNotConfigured)
		require.NotErrorIs(t, err, ErrHintingInFlight)
	})

	t.Run("refusal with unreadable config did not start, and says so", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{start: http.StatusBadRequest, config: http.StatusInternalServerError})
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingStartRefused)
		require.ErrorIs(t, err, ErrHintingConfigUnknown)
		require.NotErrorIs(t, err, ErrHintingInFlight)
	})

	t.Run("refusal classified even when the drain deadline is nearly spent", func(t *testing.T) {
		t.Parallel()
		// The up-front config read fails (200 ms), the start answers 400 at
		// ~500 ms, and the classifying config read needs 200 ms with ~100 ms
		// of drain budget left: it runs on its own budget, so the answer is
		// still "not configured" rather than "unknown".
		h := &hintStub{start: http.StatusBadRequest, balloon: balloonWithHinting(false), startDelay: 300 * time.Millisecond, configDelay: 200 * time.Millisecond, failConfigFirst: 1}
		p := newHintProcess(t, h)
		ctx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, p.DrainBalloon(ctx), ErrHintingNotConfigured)
	})

	t.Run("start past the deadline is in flight", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{start: hintStubBlock})
		p.hintCmd.Store(5)
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingInFlight)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, ErrHintingGuestSilent)
		assert.Zero(t, p.hintCmd.Load(), "the previous cycle's id does not key this stop")
	})

	t.Run("start cancelled is in flight", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{start: hintStubBlock})
		ctx, cancel := context.WithCancel(t.Context())
		go func() { time.Sleep(20 * time.Millisecond); cancel() }()
		err := p.DrainBalloon(ctx)
		require.ErrorIs(t, err, ErrHintingInFlight)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("start failure is a plain error", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{start: http.StatusInternalServerError})
		err := p.DrainBalloon(shortCtx(t))
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrHintingInFlight)
		require.NotErrorIs(t, err, ErrHintingStartRefused)
		require.NotErrorIs(t, err, ErrHintingNotConfigured)
	})

	t.Run("fast cycle after a prior done", func(t *testing.T) {
		t.Parallel()
		// The guest finishes before the first status read: DONE is seen at once.
		h := &hintStub{guestDone: -1}
		h.hostCmd.Store(freePageHintDone)
		p := newHintProcess(t, h)
		p.hintCmd.Store(9)
		require.NoError(t, p.DrainBalloon(shortCtx(t)))
		assert.Zero(t, p.hintCmd.Load(), "a reserved id is not the cycle's command")
	})

	t.Run("budget spent before the first status read is not silence", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{status: hintStubBlock}
		p := newHintProcess(t, h)
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingInFlight)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, ErrHintingGuestSilent)
	})

	t.Run("guest that never engages is silent", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		p := newHintProcess(t, h)
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingInFlight)
		require.ErrorIs(t, err, ErrHintingGuestSilent)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Greater(t, h.statusCalls.Load(), int32(1), "the status was polled")
		assert.Equal(t, freePageHintFirstCmd, p.hintCmd.Load(), "the cycle's command id was captured")
	})

	t.Run("guest that engaged but is slow is not silent", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		p := newHintProcess(t, h)
		time.AfterFunc(20*time.Millisecond, func() { h.guestCmd.Store(freePageHintFirstCmd) })
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingInFlight)
		require.NotErrorIs(t, err, ErrHintingGuestSilent)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("status failure after start is in flight", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{status: http.StatusInternalServerError})
		err := p.DrainBalloon(shortCtx(t))
		require.ErrorIs(t, err, ErrHintingInFlight)
		require.NotErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, context.Canceled)
	})

	t.Run("old firecracker is not configured, without a request", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{start: http.StatusInternalServerError}
		p := newHintProcess(t, h)
		p.Versions.FirecrackerVersion = "v1.12.0"
		require.ErrorIs(t, p.DrainBalloon(shortCtx(t)), ErrHintingNotConfigured)
		require.NoError(t, p.StopBalloonHinting(shortCtx(t), hintStopAckGrace))
		assert.Zero(t, h.configCalls.Load())
		assert.Zero(t, h.stopCalls.Load())
	})
}

func TestBalloonCaps(t *testing.T) {
	t.Parallel()
	caps, err := newHintProcess(t, &hintStub{balloon: balloonWithHinting(true)}).BalloonCaps(shortCtx(t))
	require.NoError(t, err)
	assert.Equal(t, BalloonCaps{Hinting: true}, caps)

	caps, err = newHintProcess(t, &hintStub{noBalloon: true}).BalloonCaps(shortCtx(t))
	require.NoError(t, err, "no balloon device is not an error")
	assert.Equal(t, BalloonCaps{}, caps)

	_, err = newHintProcess(t, &hintStub{config: http.StatusInternalServerError}).BalloonCaps(shortCtx(t))
	require.Error(t, err)
}

func TestStopBalloonHinting(t *testing.T) {
	t.Parallel()

	t.Run("guest that never read the command is given the grace and no more", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		p := newHintProcess(t, h)
		p.hintCmd.Store(freePageHintFirstCmd)
		start := time.Now()
		require.NoError(t, p.StopBalloonHinting(graceCtx(t), hintStopAckGrace))
		took := time.Since(start)
		assert.GreaterOrEqual(t, took, hintStopAckGrace, "a sticky STOP from the previous cycle is not this cycle's ack")
		assert.Less(t, took, graceBound)
		assert.Equal(t, int32(1), h.stopCalls.Load())
		assert.Equal(t, freePageHintDone, h.hostCmd.Load())
	})

	t.Run("guest that echoed the command is waited for", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		h.guestCmd.Store(freePageHintFirstCmd)
		p := newHintProcess(t, h)
		p.hintCmd.Store(freePageHintFirstCmd)
		// Later than the grace: only an engaged guest is waited for this long.
		time.AfterFunc(hintStopAckGrace+20*time.Millisecond, func() { h.guestCmd.Store(freePageHintStop) })
		ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
		defer cancel()
		require.NoError(t, p.StopBalloonHinting(ctx, hintStopAckGrace))
		assert.Equal(t, freePageHintStop, h.guestCmd.Load())
	})

	t.Run("guest that engages during the grace is waited for", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		p := newHintProcess(t, h)
		p.hintCmd.Store(freePageHintFirstCmd)
		time.AfterFunc(30*time.Millisecond, func() { h.guestCmd.Store(freePageHintFirstCmd) })
		time.AfterFunc(hintStopAckGrace+40*time.Millisecond, func() { h.guestCmd.Store(freePageHintStop) })
		ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
		defer cancel()
		require.NoError(t, p.StopBalloonHinting(ctx, hintStopAckGrace))
	})

	t.Run("engaged guest that never acknowledges is reported as such", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		h.guestCmd.Store(freePageHintFirstCmd)
		p := newHintProcess(t, h)
		p.hintCmd.Store(freePageHintFirstCmd)
		err := p.StopBalloonHinting(shortCtx(t), hintStopAckGrace)
		require.ErrorIs(t, err, ErrHintingStopUnacked)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, int32(1), h.stopCalls.Load(), "the stop itself landed")
	})

	t.Run("unknown command counts a guest command not seen before as this cycle", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		h.guestCmd.Store(9)
		p := newHintProcess(t, h)
		p.hintSeenInit.Store(true) // baseline read saw guest_cmd=0
		err := p.StopBalloonHinting(shortCtx(t), hintStopAckGrace)
		require.ErrorIs(t, err, ErrHintingStopUnacked)
	})

	t.Run("guest command persisted from an earlier life is not engagement", func(t *testing.T) {
		t.Parallel()
		// The device came out of a snapshot with guest_cmd=7 and this process's
		// first start times out at the HTTP layer.
		h := &hintStub{start: hintStubBlock}
		h.guestCmd.Store(7)
		p := newHintProcess(t, h)
		require.ErrorIs(t, p.DrainBalloon(shortCtx(t)), ErrHintingInFlight)
		start := time.Now()
		require.NoError(t, p.StopBalloonHinting(graceCtx(t), hintStopAckGrace))
		assert.Less(t, time.Since(start), graceBound, "the persisted id was read before the start and is not this cycle's")
	})

	t.Run("persisted guest command read is retried after a failed first read", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{start: hintStubBlock, failStatusFirst: 1}
		h.guestCmd.Store(7)
		p := newHintProcess(t, h)
		require.ErrorIs(t, p.DrainBalloon(shortCtx(t)), ErrHintingInFlight)
		assert.False(t, p.hintSeenInit.Load(), "a failed read leaves the init pending")
		start := time.Now()
		require.NoError(t, p.StopBalloonHinting(graceCtx(t), hintStopAckGrace), "without a baseline a live-looking guest_cmd is not engagement")
		assert.Less(t, time.Since(start), graceBound)
		require.ErrorIs(t, p.DrainBalloon(shortCtx(t)), ErrHintingInFlight)
		assert.True(t, p.hintSeenInit.Load())
		start = time.Now()
		require.NoError(t, p.StopBalloonHinting(graceCtx(t), hintStopAckGrace))
		assert.Less(t, time.Since(start), graceBound)
	})

	t.Run("unknown command ignores a stale guest command", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{}
		h.guestCmd.Store(9)
		p := newHintProcess(t, h)
		p.hintGuestSeen.Store(9)
		p.hintSeenInit.Store(true)
		start := time.Now()
		require.NoError(t, p.StopBalloonHinting(graceCtx(t), hintStopAckGrace))
		assert.Less(t, time.Since(start), graceBound, "a sticky id from an earlier cycle is not engagement")
	})

	t.Run("status failure during the ack wait is an API error, not the guest's", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{status: http.StatusInternalServerError}
		p := newHintProcess(t, h)
		err := p.StopBalloonHinting(shortCtx(t), hintStopAckGrace)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrHintingStopUnacked)
		assert.Equal(t, int32(1), h.stopCalls.Load())
	})

	t.Run("stop refused means nothing was running", func(t *testing.T) {
		t.Parallel()
		h := &hintStub{stop: http.StatusBadRequest}
		p := newHintProcess(t, h)
		require.NoError(t, p.StopBalloonHinting(shortCtx(t), hintStopAckGrace))
		assert.Zero(t, h.statusCalls.Load(), "no acknowledgement to wait for")
	})

	t.Run("stop failure is an error", func(t *testing.T) {
		t.Parallel()
		p := newHintProcess(t, &hintStub{stop: http.StatusInternalServerError})
		err := p.StopBalloonHinting(shortCtx(t), hintStopAckGrace)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrHintingStopUnacked)
	})
}
