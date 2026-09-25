package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

func TestSimpleCases(t *testing.T) {
	t.Parallel()
	testCases := map[string]func(string) string{
		"both newlines":               func(s string) string { return s },
		"no newline prefix":           func(s string) string { return strings.TrimPrefix(s, "\n") },
		"no newline suffix":           func(s string) string { return strings.TrimSuffix(s, "\n") },
		"no newline prefix or suffix": strings.TrimSpace,
	}

	for name, preprocessor := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tempDir := t.TempDir()

			value := `
# comment
127.0.0.1        one.host
127.0.0.2        two.host
`
			value = preprocessor(value)
			inputPath := filepath.Join(tempDir, "hosts")
			err := os.WriteFile(inputPath, []byte(value), 0o644)
			require.NoError(t, err)

			err = rewriteHostsFile("127.0.0.3", inputPath)
			require.NoError(t, err)

			data, err := os.ReadFile(inputPath)
			require.NoError(t, err)

			assert.Equal(t, `# comment
127.0.0.1        one.host
127.0.0.2        two.host
127.0.0.3        events.e2b.local`, strings.TrimSpace(string(data)))
		})
	}
}

func TestShouldSetSystemTime(t *testing.T) {
	t.Parallel()
	sandboxTime := time.Now()

	tests := []struct {
		name     string
		hostTime time.Time
		want     bool
	}{
		{
			name:     "sandbox time far ahead of host time (should set)",
			hostTime: sandboxTime.Add(-10 * time.Second),
			want:     true,
		},
		{
			name:     "sandbox time at maxTimeInPast boundary ahead of host time (should not set)",
			hostTime: sandboxTime.Add(-50 * time.Millisecond),
			want:     false,
		},
		{
			name:     "sandbox time just within maxTimeInPast ahead of host time (should not set)",
			hostTime: sandboxTime.Add(-40 * time.Millisecond),
			want:     false,
		},
		{
			name:     "sandbox time slightly ahead of host time (should not set)",
			hostTime: sandboxTime.Add(-10 * time.Millisecond),
			want:     false,
		},
		{
			name:     "sandbox time equals host time (should not set)",
			hostTime: sandboxTime,
			want:     false,
		},
		{
			name:     "sandbox time slightly behind host time (should not set)",
			hostTime: sandboxTime.Add(1 * time.Second),
			want:     false,
		},
		{
			name:     "sandbox time just within maxTimeInFuture behind host time (should not set)",
			hostTime: sandboxTime.Add(4 * time.Second),
			want:     false,
		},
		{
			name:     "sandbox time at maxTimeInFuture boundary behind host time (should not set)",
			hostTime: sandboxTime.Add(5 * time.Second),
			want:     false,
		},
		{
			name:     "sandbox time far behind host time (should set)",
			hostTime: sandboxTime.Add(1 * time.Minute),
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := shouldSetSystemTime(tt.hostTime, sandboxTime)
			assert.Equal(t, tt.want, got)
		})
	}
}

func secureTokenPtr(s string) *SecureToken {
	token := &SecureToken{}
	_ = token.Set([]byte(s))

	return token
}

type mockMMDSClient struct {
	hash string
	err  error
}

func (m *mockMMDSClient) GetAccessTokenHash(_ context.Context) (string, error) {
	return m.hash, m.err
}

func newTestAPI(accessToken *SecureToken, mmdsClient MMDSClient) *API {
	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil)
	if accessToken != nil {
		api.accessToken.TakeFrom(accessToken)
	}
	api.mmdsClient = mmdsClient

	return api
}

func TestValidateInitAccessToken(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	tests := []struct {
		name         string
		accessToken  *SecureToken
		requestToken *SecureToken
		mmdsHash     string
		mmdsErr      error
		wantErr      error
	}{
		{
			name:         "fast path: token matches existing",
			accessToken:  secureTokenPtr("secret-token"),
			requestToken: secureTokenPtr("secret-token"),
			mmdsHash:     "",
			mmdsErr:      nil,
			wantErr:      nil,
		},
		{
			name:         "MMDS match: token hash matches MMDS hash",
			accessToken:  secureTokenPtr("old-token"),
			requestToken: secureTokenPtr("new-token"),
			mmdsHash:     keys.HashAccessToken("new-token"),
			mmdsErr:      nil,
			wantErr:      nil,
		},
		{
			name:         "first-time setup: no existing token, MMDS error",
			accessToken:  nil,
			requestToken: secureTokenPtr("new-token"),
			mmdsHash:     "",
			mmdsErr:      assert.AnError,
			wantErr:      nil,
		},
		{
			name:         "first-time setup: no existing token, empty MMDS hash",
			accessToken:  nil,
			requestToken: secureTokenPtr("new-token"),
			mmdsHash:     "",
			mmdsErr:      nil,
			wantErr:      nil,
		},
		{
			name:         "first-time setup: both tokens nil, no MMDS",
			accessToken:  nil,
			requestToken: nil,
			mmdsHash:     "",
			mmdsErr:      assert.AnError,
			wantErr:      nil,
		},
		{
			name:         "mismatch: existing token differs from request, no MMDS",
			accessToken:  secureTokenPtr("existing-token"),
			requestToken: secureTokenPtr("wrong-token"),
			mmdsHash:     "",
			mmdsErr:      assert.AnError,
			wantErr:      ErrAccessTokenMismatch,
		},
		{
			name:         "mismatch: existing token differs from request, MMDS hash mismatch",
			accessToken:  secureTokenPtr("existing-token"),
			requestToken: secureTokenPtr("wrong-token"),
			mmdsHash:     keys.HashAccessToken("different-token"),
			mmdsErr:      nil,
			wantErr:      ErrAccessTokenMismatch,
		},
		{
			name:         "conflict: existing token, nil request, MMDS exists",
			accessToken:  secureTokenPtr("existing-token"),
			requestToken: nil,
			mmdsHash:     keys.HashAccessToken("some-token"),
			mmdsErr:      nil,
			wantErr:      ErrAccessTokenResetNotAuthorized,
		},
		{
			name:         "conflict: existing token, nil request, no MMDS",
			accessToken:  secureTokenPtr("existing-token"),
			requestToken: nil,
			mmdsHash:     "",
			mmdsErr:      assert.AnError,
			wantErr:      ErrAccessTokenResetNotAuthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mmdsClient := &mockMMDSClient{hash: tt.mmdsHash, err: tt.mmdsErr}
			api := newTestAPI(tt.accessToken, mmdsClient)

			err := api.validateInitAccessToken(ctx, tt.requestToken)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckMMDSHash(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	t.Run("returns match when token hash equals MMDS hash", func(t *testing.T) {
		t.Parallel()
		token := "my-secret-token"
		mmdsClient := &mockMMDSClient{hash: keys.HashAccessToken(token), err: nil}
		api := newTestAPI(nil, mmdsClient)

		matches, exists := api.checkMMDSHash(ctx, secureTokenPtr(token))

		assert.True(t, matches)
		assert.True(t, exists)
	})

	t.Run("returns no match when token hash differs from MMDS hash", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: keys.HashAccessToken("different-token"), err: nil}
		api := newTestAPI(nil, mmdsClient)

		matches, exists := api.checkMMDSHash(ctx, secureTokenPtr("my-token"))

		assert.False(t, matches)
		assert.True(t, exists)
	})

	t.Run("returns exists but no match when request token is nil", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: keys.HashAccessToken("some-token"), err: nil}
		api := newTestAPI(nil, mmdsClient)

		matches, exists := api.checkMMDSHash(ctx, nil)

		assert.False(t, matches)
		assert.True(t, exists)
	})

	t.Run("returns false, false when MMDS returns error", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: assert.AnError}
		api := newTestAPI(nil, mmdsClient)

		matches, exists := api.checkMMDSHash(ctx, secureTokenPtr("any-token"))

		assert.False(t, matches)
		assert.False(t, exists)
	})

	t.Run("returns false, false when MMDS returns empty hash with non-nil request", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: nil}
		api := newTestAPI(nil, mmdsClient)

		matches, exists := api.checkMMDSHash(ctx, secureTokenPtr("any-token"))

		assert.False(t, matches)
		assert.False(t, exists)
	})

	t.Run("returns false, false when MMDS returns empty hash with nil request", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: nil}
		api := newTestAPI(nil, mmdsClient)

		matches, exists := api.checkMMDSHash(ctx, nil)

		assert.False(t, matches)
		assert.False(t, exists)
	})

	t.Run("returns true, true when MMDS returns hash of empty string with nil request (explicit reset)", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: keys.HashAccessToken(""), err: nil}
		api := newTestAPI(nil, mmdsClient)

		matches, exists := api.checkMMDSHash(ctx, nil)

		assert.True(t, matches)
		assert.True(t, exists)
	})
}

func TestSetData(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	logger := zerolog.Nop()

	t.Run("access token updates", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name           string
			existingToken  *SecureToken
			requestToken   *SecureToken
			wantFinalToken *SecureToken
		}{
			{
				name:           "first-time setup: sets initial token",
				existingToken:  nil,
				requestToken:   secureTokenPtr("initial-token"),
				wantFinalToken: secureTokenPtr("initial-token"),
			},
			{
				name:           "first-time setup: nil request token leaves token unset",
				existingToken:  nil,
				requestToken:   nil,
				wantFinalToken: nil,
			},
			{
				name:           "re-init with same token: token unchanged",
				existingToken:  secureTokenPtr("same-token"),
				requestToken:   secureTokenPtr("same-token"),
				wantFinalToken: secureTokenPtr("same-token"),
			},
			{
				name:           "updates token when request has new token",
				existingToken:  secureTokenPtr("old-token"),
				requestToken:   secureTokenPtr("new-token"),
				wantFinalToken: secureTokenPtr("new-token"),
			},
			{
				name:           "clears token when request is nil and existing token is set",
				existingToken:  secureTokenPtr("existing-token"),
				requestToken:   nil,
				wantFinalToken: nil,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				mmdsClient := &mockMMDSClient{}
				api := newTestAPI(tt.existingToken, mmdsClient)

				data := PostInitJSONBody{
					AccessToken: tt.requestToken,
				}

				err := api.SetData(ctx, logger, data)
				require.NoError(t, err)

				if tt.wantFinalToken == nil {
					assert.False(t, api.accessToken.IsSet(), "expected token to not be set")
				} else {
					require.True(t, api.accessToken.IsSet(), "expected token to be set")
					assert.True(t, api.accessToken.EqualsSecure(tt.wantFinalToken), "expected token to match")
				}
			})
		}
	})

	t.Run("sets environment variables", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: assert.AnError}
		api := newTestAPI(nil, mmdsClient)

		envVars := EnvVars{"FOO": "bar", "BAZ": "qux"}
		data := PostInitJSONBody{
			EnvVars: &envVars,
		}

		err := api.SetData(ctx, logger, data)

		require.NoError(t, err)
		val, ok := api.defaults.EnvVars.Load("FOO")
		assert.True(t, ok)
		assert.Equal(t, "bar", val)
		val, ok = api.defaults.EnvVars.Load("BAZ")
		assert.True(t, ok)
		assert.Equal(t, "qux", val)
	})

	t.Run("sets default user", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: assert.AnError}
		api := newTestAPI(nil, mmdsClient)

		data := PostInitJSONBody{
			DefaultUser: new("testuser"),
		}

		err := api.SetData(ctx, logger, data)

		require.NoError(t, err)
		assert.Equal(t, "testuser", api.defaults.User)
	})

	t.Run("does not set default user when empty", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: assert.AnError}
		api := newTestAPI(nil, mmdsClient)
		api.defaults.User = "original"

		data := PostInitJSONBody{
			DefaultUser: new(""),
		}

		err := api.SetData(ctx, logger, data)

		require.NoError(t, err)
		assert.Equal(t, "original", api.defaults.User)
	})

	t.Run("sets default workdir", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: assert.AnError}
		api := newTestAPI(nil, mmdsClient)

		data := PostInitJSONBody{
			DefaultWorkdir: new("/home/user"),
		}

		err := api.SetData(ctx, logger, data)

		require.NoError(t, err)
		require.NotNil(t, api.defaults.Workdir)
		assert.Equal(t, "/home/user", *api.defaults.Workdir)
	})

	t.Run("does not set default workdir when empty", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: assert.AnError}
		api := newTestAPI(nil, mmdsClient)
		originalWorkdir := "/original"
		api.defaults.Workdir = &originalWorkdir

		data := PostInitJSONBody{
			DefaultWorkdir: new(""),
		}

		err := api.SetData(ctx, logger, data)

		require.NoError(t, err)
		require.NotNil(t, api.defaults.Workdir)
		assert.Equal(t, "/original", *api.defaults.Workdir)
	})

	t.Run("sets multiple fields at once", func(t *testing.T) {
		t.Parallel()
		mmdsClient := &mockMMDSClient{hash: "", err: assert.AnError}
		api := newTestAPI(nil, mmdsClient)

		envVars := EnvVars{"KEY": "value"}
		data := PostInitJSONBody{
			AccessToken:    secureTokenPtr("token"),
			DefaultUser:    new("user"),
			DefaultWorkdir: new("/workdir"),
			EnvVars:        &envVars,
		}

		err := api.SetData(ctx, logger, data)

		require.NoError(t, err)
		assert.True(t, api.accessToken.Equals("token"), "expected token to match")
		assert.Equal(t, "user", api.defaults.User)
		assert.Equal(t, "/workdir", *api.defaults.Workdir)
		val, ok := api.defaults.EnvVars.Load("KEY")
		assert.True(t, ok)
		assert.Equal(t, "value", val)
	})
}

func TestNFSMountOptions(t *testing.T) {
	t.Parallel()

	t.Run("async options contain expected flags", func(t *testing.T) {
		t.Parallel()
		assert.Contains(t, nfsAsyncOptions, "async")
		assert.Contains(t, nfsAsyncOptions, "noatime")
		assert.Contains(t, nfsAsyncOptions, "nodiratime")
		assert.Contains(t, nfsAsyncOptions, "actimeo=3600")
		assert.NotContains(t, nfsAsyncOptions, ",sync,")       // must not have sync write mode
		assert.NotContains(t, nfsAsyncOptions, "lookupcache=") // must not have lookup cache control
	})

	t.Run("sync options contain expected flags", func(t *testing.T) {
		t.Parallel()
		assert.Contains(t, nfsSyncOptions, ",sync,")
		assert.Contains(t, nfsSyncOptions, "noac")
		assert.Contains(t, nfsSyncOptions, "lookupcache=none")
		assert.NotContains(t, nfsSyncOptions, "async")
		assert.NotContains(t, nfsSyncOptions, "actimeo")
	})

	t.Run("both profiles share base options", func(t *testing.T) {
		t.Parallel()
		for _, opt := range nfsBaseOptions {
			assert.Contains(t, nfsAsyncOptions, opt, "async missing base option %q", opt)
			assert.Contains(t, nfsSyncOptions, opt, "sync missing base option %q", opt)
		}
	})
}

func TestSyncDefaultValue(t *testing.T) {
	t.Parallel()

	t.Run("nil sync defaults to sync mount (true)", func(t *testing.T) {
		t.Parallel()
		volume := VolumeMount{NfsTarget: "host:/vol", Path: "/mnt"}
		// volume.Sync is nil
		syncMount := volume.Sync == nil || *volume.Sync
		assert.True(t, syncMount, "nil Sync should default to sync (true)")
	})

	t.Run("explicit true uses sync mount", func(t *testing.T) {
		t.Parallel()
		tr := true
		volume := VolumeMount{NfsTarget: "host:/vol", Path: "/mnt", Sync: &tr}
		syncMount := volume.Sync == nil || *volume.Sync
		assert.True(t, syncMount)
	})

	t.Run("explicit false uses async mount", func(t *testing.T) {
		t.Parallel()
		f := false
		volume := VolumeMount{NfsTarget: "host:/vol", Path: "/mnt", Sync: &f}
		syncMount := volume.Sync == nil || *volume.Sync
		assert.False(t, syncMount)
	})
}

func TestShouldRemountNFS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		isMounted          bool
		mountedLifecycleID string
		requestLifecycleID string
		wantRemount        bool
	}{
		{
			name:               "not mounted: should mount",
			isMounted:          false,
			mountedLifecycleID: "",
			requestLifecycleID: "",
			wantRemount:        true,
		},
		{
			name:               "not mounted with request lifecycle: should mount",
			isMounted:          false,
			mountedLifecycleID: "",
			requestLifecycleID: "abc",
			wantRemount:        true,
		},
		{
			name:               "mounted empty + request empty: no remount (would cause infinite loop)",
			isMounted:          true,
			mountedLifecycleID: "",
			requestLifecycleID: "",
			wantRemount:        false,
		},
		{
			name:               "mounted with lifecycle + request empty: remount (lifecycle cleared)",
			isMounted:          true,
			mountedLifecycleID: "abc",
			requestLifecycleID: "",
			wantRemount:        true,
		},
		{
			name:               "mounted empty + request with lifecycle: remount (new lifecycle)",
			isMounted:          true,
			mountedLifecycleID: "",
			requestLifecycleID: "abc",
			wantRemount:        true,
		},
		{
			name:               "mounted + request same lifecycle: no remount",
			isMounted:          true,
			mountedLifecycleID: "abc",
			requestLifecycleID: "abc",
			wantRemount:        false,
		},
		{
			name:               "mounted + request different lifecycle: remount",
			isMounted:          true,
			mountedLifecycleID: "abc",
			requestLifecycleID: "xyz",
			wantRemount:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := shouldRemountNFS(tt.isMounted, tt.mountedLifecycleID, tt.requestLifecycleID)

			assert.Equal(t, tt.wantRemount, got)
		})
	}
}

type fakeCgroupManager struct {
	mu               sync.Mutex
	frozen           []cgroups.ProcessType
	freezeErr        error
	freezeDelay      time.Duration
	freezeDelayOnce  sync.Once
	unfrozen         []cgroups.ProcessType
	unfreezeAttempts []cgroups.ProcessType
	unfreezeErr      error
	// frozenErr and neverFreezes drive the state-reading path: a cgroup whose tasks
	// never reach a signal-delivery point reports frozen=false until the deadline.
	frozenErr    error
	neverFreezes bool
	// frozenUnobservable models a guest with no cgroup manager: the write is accepted
	// but freeze state can never be read back.
	frozenUnobservable bool
}

type fakeLogFlusher struct {
	mu             sync.Mutex
	calls          int
	sendAttempts   int
	purged         bool
	err            error
	waitForContext bool
	contextErr     error
	deadline       time.Time
	hasDeadline    bool
	onCall         func()
}

func (f *fakeLogFlusher) FlushAndPurge(ctx context.Context) error {
	f.mu.Lock()
	f.calls++
	f.deadline, f.hasDeadline = ctx.Deadline()
	f.mu.Unlock()

	if f.onCall != nil {
		f.onCall()
	}
	defer func() {
		f.mu.Lock()
		f.purged = true
		f.mu.Unlock()
	}()

	if err := ctx.Err(); err != nil {
		f.mu.Lock()
		f.contextErr = err
		f.mu.Unlock()

		return err
	}

	f.mu.Lock()
	f.sendAttempts++
	f.mu.Unlock()
	if f.waitForContext {
		<-ctx.Done()

		f.mu.Lock()
		f.contextErr = ctx.Err()
		f.mu.Unlock()

		return ctx.Err()
	}

	return f.err
}

type fakeLogFlushResult struct {
	calls        int
	sendAttempts int
	purged       bool
	contextErr   error
	deadline     time.Time
	hasDeadline  bool
}

func (f *fakeLogFlusher) result() fakeLogFlushResult {
	f.mu.Lock()
	defer f.mu.Unlock()

	return fakeLogFlushResult{
		calls:        f.calls,
		sendAttempts: f.sendAttempts,
		purged:       f.purged,
		contextErr:   f.contextErr,
		deadline:     f.deadline,
		hasDeadline:  f.hasDeadline,
	}
}

func (f *fakeCgroupManager) GetFileDescriptor(cgroups.ProcessType) (int, bool) {
	return 0, false
}

func (f *fakeCgroupManager) Freeze(pt cgroups.ProcessType) error {
	f.freezeDelayOnce.Do(func() { time.Sleep(f.freezeDelay) })

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.freezeErr != nil {
		return f.freezeErr
	}
	f.frozen = append(f.frozen, pt)

	return nil
}

func (f *fakeCgroupManager) Frozen(pt cgroups.ProcessType) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.frozenUnobservable {
		return false, cgroups.ErrFrozenUnobservable
	}
	if f.frozenErr != nil {
		return false, f.frozenErr
	}
	if f.neverFreezes {
		return false, nil
	}

	return slices.Contains(f.frozen, pt), nil
}

func (f *fakeCgroupManager) Unfreeze(pt cgroups.ProcessType) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unfreezeAttempts = append(f.unfreezeAttempts, pt)
	if f.unfreezeErr != nil {
		return f.unfreezeErr
	}
	f.unfrozen = append(f.unfrozen, pt)

	return nil
}

func (f *fakeCgroupManager) Close() error { return nil }

func newAPIWithCgroupManager(mgr cgroups.Manager) *API {
	return newAPIWithCgroupManagerAndLogFlusher(mgr, nil)
}

func newAPIWithCgroupManagerAndLogFlusher(mgr cgroups.Manager, logFlusher LogFlusher) *API {
	logger := zerolog.Nop()

	return New(&logger, &execcontext.Defaults{EnvVars: utils.NewEnvVars()}, nil, false, cgroups.NewWorkloadFreezer(mgr), logFlusher)
}

// newAPIWithCgroupManagerLogging is newAPIWithCgroupManager with the log output captured,
// for assertions about what the handler does and does not warn about.
func newAPIWithCgroupManagerLogging(mgr cgroups.Manager, out io.Writer) *API {
	logger := zerolog.New(out)

	return New(&logger, &execcontext.Defaults{EnvVars: utils.NewEnvVars()}, nil, false, cgroups.NewWorkloadFreezer(mgr), nil)
}

func TestPostFreeze(t *testing.T) {
	t.Parallel()

	t.Run("flushes logs after freezing the workload", func(t *testing.T) {
		t.Parallel()

		mgr := &fakeCgroupManager{}
		var frozenBeforeFlush bool
		flusher := &fakeLogFlusher{onCall: func() {
			mgr.mu.Lock()
			defer mgr.mu.Unlock()
			frozenBeforeFlush = slices.Equal(mgr.frozen, cgroups.WorkloadProcessTypes)
		}}
		api := newAPIWithCgroupManagerAndLogFlusher(mgr, flusher)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(1000)
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})

		require.Equal(t, http.StatusOK, rec.Code)
		result := flusher.result()
		assert.Equal(t, 1, result.calls)
		assert.Equal(t, 1, result.sendAttempts)
		assert.True(t, result.purged)
		assert.True(t, frozenBeforeFlush)
	})

	t.Run("still answers 200 when flushing fails", func(t *testing.T) {
		t.Parallel()

		flusher := &fakeLogFlusher{err: errors.New("collector unavailable")}
		api := newAPIWithCgroupManagerAndLogFlusher(&fakeCgroupManager{}, flusher)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(1000)
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})

		require.Equal(t, http.StatusOK, rec.Code)
		result := flusher.result()
		assert.Equal(t, 1, result.calls)
		assert.True(t, result.purged)
	})

	t.Run("bounds a stuck flush by maxWaitMs and purges", func(t *testing.T) {
		t.Parallel()

		const maxWait = 100 * time.Millisecond
		flusher := &fakeLogFlusher{waitForContext: true}
		api := newAPIWithCgroupManagerAndLogFlusher(&fakeCgroupManager{}, flusher)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(maxWait / time.Millisecond)

		start := time.Now()
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})
		elapsed := time.Since(start)

		require.Equal(t, http.StatusOK, rec.Code)
		result := flusher.result()
		assert.Equal(t, 1, result.calls)
		assert.Equal(t, 1, result.sendAttempts)
		assert.True(t, result.purged)
		require.ErrorIs(t, result.contextErr, context.DeadlineExceeded)
		assert.GreaterOrEqual(t, elapsed, maxWait/2)
		assert.Less(t, elapsed, maxWait+100*time.Millisecond)
	})

	t.Run("gives the flusher only the budget left after freezing", func(t *testing.T) {
		t.Parallel()

		const (
			maxWait    = time.Second
			freezeTime = 200 * time.Millisecond
		)
		flusher := &fakeLogFlusher{}
		api := newAPIWithCgroupManagerAndLogFlusher(&fakeCgroupManager{freezeDelay: freezeTime}, flusher)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(maxWait / time.Millisecond)

		start := time.Now()
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})

		require.Equal(t, http.StatusOK, rec.Code)
		result := flusher.result()
		require.True(t, result.hasDeadline)
		assert.LessOrEqual(t, result.deadline.Sub(start), maxWait+50*time.Millisecond,
			"the flush deadline must stay anchored to the whole handler budget")
		assert.Equal(t, 1, result.sendAttempts)
		assert.True(t, result.purged)
	})

	t.Run("without maxWaitMs purges without sending", func(t *testing.T) {
		t.Parallel()

		flusher := &fakeLogFlusher{}
		api := newAPIWithCgroupManagerAndLogFlusher(&fakeCgroupManager{}, flusher)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		api.PostFreeze(rec, req, PostFreezeParams{})

		require.Equal(t, http.StatusNoContent, rec.Code)
		result := flusher.result()
		assert.Equal(t, 1, result.calls)
		assert.Zero(t, result.sendAttempts)
		assert.True(t, result.purged)
		require.ErrorIs(t, result.contextErr, context.DeadlineExceeded)
	})

	t.Run("uses a noop flusher outside Firecracker", func(t *testing.T) {
		t.Parallel()

		api := newAPIWithCgroupManagerAndLogFlusher(&fakeCgroupManager{}, noopLogFlusher{})
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()

		api.PostFreeze(rec, req, PostFreezeParams{})
		assert.Equal(t, http.StatusNoContent, rec.Code)
	})

	t.Run("freezes all user cgroups", func(t *testing.T) {
		t.Parallel()
		mgr := &fakeCgroupManager{}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(1000)
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.frozen)

		var body FreezeResult
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
		require.NotNil(t, body.Requested)
		require.NotNil(t, body.Frozen)
		require.NotNil(t, body.NotFrozen)
		assert.Equal(t, len(cgroups.WorkloadProcessTypes), *body.Requested)
		assert.Equal(t, len(cgroups.WorkloadProcessTypes), *body.Frozen,
			"a fake that reports frozen immediately must read back every cgroup frozen")
		assert.Zero(t, *body.NotFrozen)
	})

	t.Run("reports notFrozen when the workload never stops", func(t *testing.T) {
		t.Parallel()
		// The freeze write succeeds but cgroup.events never reports frozen -- a task
		// stuck in an uninterruptible wait. The call must still succeed, because an
		// unfreezable customer task must never fail their pause, and must say so.
		mgr := &fakeCgroupManager{neverFreezes: true}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(20)
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})

		require.Equal(t, http.StatusOK, rec.Code)
		var body FreezeResult
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
		require.NotNil(t, body.NotFrozen)
		assert.Equal(t, len(cgroups.WorkloadProcessTypes), *body.NotFrozen,
			"every cgroup should be reported notFrozen")
		assert.Zero(t, *body.Frozen)
	})

	// A cgroup that is still there and rejects the write is expected. Answering 500 would
	// hide the failed count that exists to make it visible, and would lose the whole
	// result with it. A WALK-DISCOVERED cgroup that was removed mid-sweep does not reach
	// this arm -- it is counted vanished and raises nothing; one of envd's own static
	// cgroups that goes away does reach it, by design.
	t.Run("reports failed cgroups in the body rather than erroring", func(t *testing.T) {
		t.Parallel()
		mgr := &fakeCgroupManager{freezeErr: errors.New("write cgroup.freeze: io error")}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(20)
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, mgr.frozen)

		var body FreezeResult
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
		require.NotNil(t, body.Failed)
		assert.Equal(t, len(cgroups.WorkloadProcessTypes), *body.Failed)
		assert.Zero(t, *body.Requested)
	})

	// The write lands but the state cannot be read back -- a cgroup removed mid-sweep
	// reports ENOENT. That is a failed cgroup, not one refusing to stop: reporting it as
	// notFrozen would tell the pause path a live workload is about to be snapshotted, and
	// answering 500 would throw away the whole result over one unreadable cgroup.
	t.Run("reports a cgroup whose freeze state cannot be read as failed", func(t *testing.T) {
		t.Parallel()
		mgr := &fakeCgroupManager{frozenErr: errors.New("read cgroup.events: no such file or directory")}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(10_000)

		start := time.Now()
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})
		elapsed := time.Since(start)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.frozen, "the writes still landed")

		var body FreezeResult
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
		require.NotNil(t, body.Failed)
		assert.Equal(t, len(cgroups.WorkloadProcessTypes), *body.Failed)
		assert.Zero(t, *body.NotFrozen, "unreadable is not the same as refusing to stop")
		assert.Zero(t, *body.Frozen)
		assert.Less(t, elapsed, time.Second,
			"a state that cannot be read must not be polled for the whole budget (10s)")
	})

	// An orchestrator predating maxWaitMs treats any status but 204 as a failed freeze,
	// so omitting the parameter must reproduce the original contract exactly: freeze
	// issued, nothing awaited, bare 204.
	t.Run("answers 204 without maxWaitMs, for callers predating the structured result", func(t *testing.T) {
		t.Parallel()
		mgr := &fakeCgroupManager{neverFreezes: true}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()

		start := time.Now()
		api.PostFreeze(rec, req, PostFreezeParams{})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, rec.Body.String(), "a 204 must carry no body")
		assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.frozen,
			"the freeze itself must still be issued")
		assert.Less(t, time.Since(start), time.Second,
			"without maxWaitMs the call must not wait at all")
	})

	// A zero budget means the state was never read, which leaves every written cgroup
	// counted as notFrozen. Warning on that would claim a running workload on the strength
	// of a check we deliberately skipped -- once per pause, for every orchestrator
	// predating maxWaitMs, throughout the rollout.
	t.Run("does not warn about a workload it never checked", func(t *testing.T) {
		t.Parallel()
		var logs bytes.Buffer
		mgr := &fakeCgroupManager{neverFreezes: true}
		api := newAPIWithCgroupManagerLogging(mgr, &logs)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		api.PostFreeze(rec, req, PostFreezeParams{})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.frozen, "the freeze is still issued")
		assert.NotContains(t, logs.String(), "did not stop the whole workload",
			"the legacy path skips the state read, so it must not claim the workload kept running")
	})

	// A guest with no cgroup manager cannot report freeze state. That is neither a
	// frozen workload nor one refusing to stop, and conflating it with the
	// latter would burn the whole budget on every pause.
	t.Run("reports unreadable freeze state as unobservable, without waiting", func(t *testing.T) {
		t.Parallel()
		mgr := &fakeCgroupManager{frozenUnobservable: true}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/freeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		maxWaitMs := int64(10_000)

		start := time.Now()
		api.PostFreeze(rec, req, PostFreezeParams{MaxWaitMs: &maxWaitMs})
		elapsed := time.Since(start)

		require.Equal(t, http.StatusOK, rec.Code)
		var body FreezeResult
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
		require.NotNil(t, body.Unobservable)
		assert.Equal(t, len(cgroups.WorkloadProcessTypes), *body.Unobservable)
		assert.Zero(t, *body.NotFrozen, "unobservable is not the same as refusing to stop")
		assert.Zero(t, *body.Failed)
		assert.Less(t, elapsed, time.Second,
			"must not poll for a state that can never appear (budget was 10s)")
	})
}

func TestPostUnfreeze(t *testing.T) {
	t.Parallel()

	t.Run("unfreezes all user cgroups", func(t *testing.T) {
		t.Parallel()
		mgr := &fakeCgroupManager{}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/unfreeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		api.PostUnfreeze(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.unfrozen)
	})

	t.Run("returns 500 but attempts every cgroup on unfreeze error", func(t *testing.T) {
		t.Parallel()
		mgr := &fakeCgroupManager{unfreezeErr: errors.New("write cgroup.freeze: io error")}
		api := newAPIWithCgroupManager(mgr)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/unfreeze", http.NoBody)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		api.PostUnfreeze(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Empty(t, mgr.unfrozen)
		assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.unfreezeAttempts)
	})
}

// Stale /init (Timestamp older than lastSetTime) must still thaw user cgroups
// even though SetData is skipped.
func TestPostInit_UnfreezeOnStaleTimestamp(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true

	now := time.Now()
	require.True(t, api.lastSetTime.SetToGreater(now.UnixNano()))

	stale := now.Add(-1 * time.Minute)
	body, err := json.Marshal(PostInitJSONBody{
		Timestamp: &stale,
		EnvVars:   &EnvVars{"SHOULD_NOT_BE_SET": "x"},
	})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", bytes.NewReader(body))
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	api.PostInit(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	_, ok := api.defaults.EnvVars.Load("SHOULD_NOT_BE_SET")
	assert.False(t, ok, "stale /init should not apply EnvVars")
	assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.unfrozen, "stale /init must still unfreeze")
}

// Unauthorized /init must NOT thaw cgroups.
func TestPostInit_UnauthorizedDoesNotUnfreeze(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true
	api.accessToken.TakeFrom(secureTokenPtr("real-token"))

	body := []byte(`{"accessToken":"wrong-token"}`)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/init", bytes.NewReader(body))
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	api.PostInit(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Empty(t, mgr.unfreezeAttempts, "unauthorized /init must not attempt unfreeze")
}
