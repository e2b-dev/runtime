package mtls

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// fakeFlags is a FlagReader whose values the test sets; a missing key
// returns the fallback, as the feature-flag client does.
type fakeFlags struct {
	mu     sync.Mutex
	values map[string]string
}

func newFakeFlags() *fakeFlags {
	return &fakeFlags{values: map[string]string{}}
}

func (f *fakeFlags) String(_ context.Context, key, fallback string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if v, ok := f.values[key]; ok {
		return v
	}

	return fallback
}

func (f *fakeFlags) set(key, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.values[key] = value
}

func (f *fakeFlags) unset(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.values, key)
}

func TestParseMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value string
		want  Mode
		fails bool
	}{
		{value: "off", want: ModeOff},
		{value: "permissive", want: ModePermissive},
		{value: "required", want: ModeRequired},
		{value: " Required ", want: ModeRequired},
		{value: "REQUIRED", want: ModeRequired},
		{value: "", fails: true},
		{value: "on", fails: true},
		{value: "strict", fails: true},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			t.Parallel()

			got, err := ParseMode(tt.value)
			if tt.fails {
				require.ErrorIs(t, err, ErrInvalidMode)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	assert.Equal(t, "off", ModeOff.String())
	assert.Equal(t, "permissive", ModePermissive.String())
	assert.Equal(t, "required", ModeRequired.String())
}

func TestParseClientMode(t *testing.T) {
	t.Parallel()

	got, err := ParseClientMode(" ON")
	require.NoError(t, err)
	assert.Equal(t, ClientOn, got)

	got, err = ParseClientMode("off")
	require.NoError(t, err)
	assert.Equal(t, ClientOff, got)

	_, err = ParseClientMode("true")
	require.ErrorIs(t, err, ErrInvalidMode)
	_, err = ParseClientMode("permissive")
	require.ErrorIs(t, err, ErrInvalidMode)

	assert.Equal(t, "on", ClientOn.String())
	assert.Equal(t, "off", ClientOff.String())
}

//nolint:paralleltest // t.Setenv cannot be used from a parallel test
func TestModeFromEnv(t *testing.T) {
	const name = "MTLS_TEST_LISTENER_MODE"

	t.Setenv(name, "")
	got, err := ModeFromEnv(name)
	require.NoError(t, err)
	assert.Equal(t, ModeOff, got, "unset means off")

	t.Setenv(name, "required")
	got, err = ModeFromEnv(name)
	require.NoError(t, err)
	assert.Equal(t, ModeRequired, got)

	t.Setenv(name, "bogus")
	_, err = ModeFromEnv(name)
	require.ErrorIs(t, err, ErrInvalidMode, "a typo must not silently read as off")
}

//nolint:paralleltest // t.Setenv cannot be used from a parallel test
func TestClientModeFromEnv(t *testing.T) {
	const name = "MTLS_TEST_CLIENT_MODE"

	t.Setenv(name, "")
	got, err := ClientModeFromEnv(name)
	require.NoError(t, err)
	assert.Equal(t, ClientOff, got)

	t.Setenv(name, "on")
	got, err = ClientModeFromEnv(name)
	require.NoError(t, err)
	assert.Equal(t, ClientOn, got)

	t.Setenv(name, "yes")
	_, err = ClientModeFromEnv(name)
	require.ErrorIs(t, err, ErrInvalidMode)
}

func TestStaticSourcesNeverChange(t *testing.T) {
	t.Parallel()

	var mode ModeSource = StaticMode(ModePermissive)
	assert.Equal(t, ModePermissive, mode.Mode(t.Context()))
	assert.Equal(t, SourceFallback, sourceOf(mode))

	var client ClientModeSource = StaticClientMode(ClientOn)
	assert.Equal(t, ClientOn, client.ClientMode(t.Context()))
	assert.Equal(t, SourceFallback, sourceOf(client))
}

func TestFlagModeSourceStartsOnTheFallbackAndFollowsTheFlag(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	allow, err := ParseAllowList([]string{clientID})
	require.NoError(t, err)
	log, logs := testLogger(t)
	src := NewFlagModeSource(flags, "listener-mode", ModeOff, allow, WithModeLogger(log))

	assert.Equal(t, ModeOff, src.Mode(t.Context()))
	assert.Equal(t, SourceFallback, src.Source())
	assert.Equal(t, SourceFallback, sourceOf(src))

	flags.set("listener-mode", "permissive")
	assert.Equal(t, ModePermissive, src.Mode(t.Context()))
	assert.Equal(t, SourceFlag, src.Source())

	flags.set("listener-mode", "required")
	assert.Equal(t, ModeRequired, src.Mode(t.Context()))

	flags.unset("listener-mode")
	assert.Equal(t, ModeRequired, src.Mode(t.Context()), "a missing flag keeps the last accepted value")
	assert.Equal(t, SourceFlag, src.Source())

	flags.set("listener-mode", "garbage")
	assert.Equal(t, ModeRequired, src.Mode(t.Context()), "a malformed flag keeps the last accepted value")

	changes := 0
	for _, entry := range logs.All() {
		if entry.Message == "mtls: listener mode changed" {
			changes++
		}
	}
	assert.Equal(t, 2, changes, "off to permissive, permissive to required")
}

func TestFlagModeSourceRefusesRequiredWithAnEmptyAllowList(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	allow, err := ParseAllowList(nil)
	require.NoError(t, err)
	log, logs := testLogger(t)
	src := NewFlagModeSource(flags, "listener-mode", ModePermissive, allow, WithModeLogger(log))

	flags.set("listener-mode", "required")
	assert.Equal(t, ModePermissive, src.Mode(t.Context()), "required is refused while the list is empty")
	assert.Equal(t, SourceFallback, src.Source())
	assert.Contains(t, logMessages(logs), "mtls: mode flag asks for required with an empty allow-list; staying")

	require.NoError(t, allow.Replace([]string{clientID}))
	assert.Equal(t, ModeRequired, src.Mode(t.Context()), "once the list has a name the flip is accepted")
	assert.Equal(t, SourceFlag, src.Source())
}

func TestFlagModeSourceAcceptsHumanSpelling(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	allow, err := ParseAllowList([]string{clientID})
	require.NoError(t, err)
	log, _ := testLogger(t)
	src := NewFlagModeSource(flags, "listener-mode", ModeOff, allow, WithModeLogger(log))

	flags.set("listener-mode", " Required")
	assert.Equal(t, ModeRequired, src.Mode(t.Context()))

	flags.set("listener-mode", "PERMISSIVE")
	assert.Equal(t, ModePermissive, src.Mode(t.Context()))
}

func TestFlagModeSourceLogsAnUnknownValueOnce(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	allow, err := ParseAllowList([]string{clientID})
	require.NoError(t, err)
	log, logs := testLogger(t)
	src := NewFlagModeSource(flags, "listener-mode", ModePermissive, allow, WithModeLogger(log))

	flags.set("listener-mode", "strict")
	for range 5 {
		assert.Equal(t, ModePermissive, src.Mode(t.Context()))
	}
	flags.set("listener-mode", "stricter")
	assert.Equal(t, ModePermissive, src.Mode(t.Context()))

	warnings := logs.FilterLevelExact(zapcore.WarnLevel).FilterMessage("mtls: mode flag value is not a mode; staying").All()
	require.Len(t, warnings, 2, "one warning per distinct bad value, not per handshake")
	assert.Equal(t, "strict", warnings[0].ContextMap()["value"])
	assert.Equal(t, "stricter", warnings[1].ContextMap()["value"])
}

func TestFlagClientModeSource(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	log, logs := testLogger(t)
	src := NewFlagClientModeSource(flags, "hop-mode", ClientOff, WithModeLogger(log))

	assert.Equal(t, ClientOff, src.ClientMode(t.Context()))
	assert.Equal(t, SourceFallback, src.Source())

	flags.set("hop-mode", "on")
	assert.Equal(t, ClientOn, src.ClientMode(t.Context()))
	assert.Equal(t, SourceFlag, src.Source())
	assert.Equal(t, SourceFlag, sourceOf(src))

	flags.set("hop-mode", "bogus")
	assert.Equal(t, ClientOn, src.ClientMode(t.Context()))

	flags.unset("hop-mode")
	assert.Equal(t, ClientOn, src.ClientMode(t.Context()))

	assert.Contains(t, logMessages(logs), "mtls: client hop mode changed")
	assert.Contains(t, logMessages(logs), "mtls: mode flag value is not a mode; staying")
}

// slowFirstFlag answers the first evaluation slowly with one value and every
// later one at once with another, then reports the flag unset. Two concurrent
// reads therefore finish evaluating in the opposite order from the one they
// started in, unless the source serialises them.
type slowFirstFlag struct {
	calls atomic.Int32
	done  atomic.Bool
	first string
	then  string
}

func (f *slowFirstFlag) String(_ context.Context, _, fallback string) string {
	if f.done.Load() {
		return fallback
	}
	if f.calls.Add(1) == 1 {
		time.Sleep(50 * time.Millisecond)

		return f.first
	}

	return f.then
}

func TestFlagModeSourceAppliesEvaluationsInTheOrderTheyWereMade(t *testing.T) {
	t.Parallel()

	allow, err := ParseAllowList([]string{clientID})
	require.NoError(t, err)
	flags := &slowFirstFlag{first: "permissive", then: "required"}
	src := NewFlagModeSource(flags, "listener-mode", ModeOff, allow)

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { src.Mode(t.Context()) })
	}
	wg.Wait()

	flags.done.Store(true)
	assert.Equal(t, ModeRequired, src.Mode(t.Context()), "the later evaluation is the one kept once the flag is unreadable")
}

func TestModeAndSourceAreReadTogether(t *testing.T) {
	t.Parallel()

	flags := newFakeFlags()
	allow, err := ParseAllowList([]string{clientID})
	require.NoError(t, err)
	src := NewFlagModeSource(flags, "listener-mode", ModePermissive, allow)

	mode, source := modeWithSource(t.Context(), src)
	assert.Equal(t, ModePermissive, mode)
	assert.Equal(t, SourceFallback, source)

	flags.set("listener-mode", "required")
	mode, source = modeWithSource(t.Context(), src)
	assert.Equal(t, ModeRequired, mode)
	assert.Equal(t, SourceFlag, source)

	mode, source = modeWithSource(t.Context(), StaticMode(ModeOff))
	assert.Equal(t, ModeOff, mode)
	assert.Equal(t, SourceFallback, source, "a static source has no flag behind it")

	hop := NewFlagClientModeSource(flags, "hop-mode", ClientOff)
	flags.set("hop-mode", "on")
	clientMode, source := clientModeWithSource(t.Context(), hop)
	assert.Equal(t, ClientOn, clientMode)
	assert.Equal(t, SourceFlag, source)
}
