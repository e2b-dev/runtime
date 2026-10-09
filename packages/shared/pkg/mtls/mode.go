package mtls

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// Mode is what a listener accepts.
type Mode int

const (
	// ModeOff accepts plaintext only and checks nothing: today's behaviour.
	ModeOff Mode = iota
	// ModePermissive accepts TLS and plaintext, runs every check, counts what
	// it would refuse, and enforces nothing.
	ModePermissive
	// ModeRequired accepts TLS from an allow-listed name; plaintext reaches
	// only the health paths and the gRPC health service.
	ModeRequired
)

func (m Mode) String() string {
	switch m {
	case ModeOff:
		return "off"
	case ModePermissive:
		return "permissive"
	case ModeRequired:
		return "required"
	default:
		return fmt.Sprintf("mode(%d)", int(m))
	}
}

// ParseMode reads off, permissive or required, ignoring case and surrounding space.
func ParseMode(value string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "off":
		return ModeOff, nil
	case "permissive":
		return ModePermissive, nil
	case "required":
		return ModeRequired, nil
	default:
		return ModeOff, fmt.Errorf("%w: %q", ErrInvalidMode, value)
	}
}

// ModeFromEnv parses the listener mode named by the environment variable. An
// unset or blank variable is ModeOff; any other value that is not a mode is an
// error, so a typo cannot silently read as off.
func ModeFromEnv(name string) (Mode, error) {
	value := os.Getenv(name)
	if strings.TrimSpace(value) == "" {
		return ModeOff, nil
	}

	return ParseMode(value)
}

// ClientMode is whether a client hop dials TLS.
type ClientMode int

const (
	// ClientOff dials plaintext.
	ClientOff ClientMode = iota
	// ClientOn dials TLS with the client certificate and verifies the server.
	ClientOn
)

func (m ClientMode) String() string {
	switch m {
	case ClientOff:
		return "off"
	case ClientOn:
		return "on"
	default:
		return fmt.Sprintf("client_mode(%d)", int(m))
	}
}

// ParseClientMode reads on or off, ignoring case and surrounding space.
func ParseClientMode(value string) (ClientMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "off":
		return ClientOff, nil
	case "on":
		return ClientOn, nil
	default:
		return ClientOff, fmt.Errorf("%w: %q", ErrInvalidMode, value)
	}
}

// ClientModeFromEnv parses the client hop mode named by the environment
// variable; unset or blank is ClientOff.
func ClientModeFromEnv(name string) (ClientMode, error) {
	value := os.Getenv(name)
	if strings.TrimSpace(value) == "" {
		return ClientOff, nil
	}

	return ParseClientMode(value)
}

// ModeSource yields the mode a listener applies. It is read on every
// handshake, request and RPC, so a flip needs no restart.
type ModeSource interface {
	Mode(ctx context.Context) Mode
}

// ClientModeSource yields whether a client hop dials TLS; read on every
// handshake, because clients keep connections for a long time.
type ClientModeSource interface {
	ClientMode(ctx context.Context) ClientMode
}

// StaticMode is a ModeSource that never changes, for a service whose mode
// comes from the environment alone.
type StaticMode Mode

// Mode implements ModeSource.
func (m StaticMode) Mode(context.Context) Mode { return Mode(m) }

// StaticClientMode is a ClientModeSource that never changes.
type StaticClientMode ClientMode

// ClientMode implements ClientModeSource.
func (m StaticClientMode) ClientMode(context.Context) ClientMode { return ClientMode(m) }

// Values of the source attribute on the mode gauges.
const (
	// SourceFallback means the mode in force is the environment value.
	SourceFallback = "fallback"
	// SourceFlag means a flag value has been accepted.
	SourceFlag = "flag"
)

// FlagReader is the one method the package needs from a feature-flag client.
// It returns fallback when the flag is missing, so an adapter over any
// client is a few lines and the package imports none.
type FlagReader interface {
	String(ctx context.Context, key, fallback string) string
}

// flagUnset is the fallback handed to the reader: a missing flag comes back
// as it, which keeps the last accepted value.
const flagUnset = ""

// ModeOption configures a flag-backed source.
type ModeOption func(*modeOptions)

type modeOptions struct {
	log logger.Logger
}

// WithModeLogger replaces logger.L().
func WithModeLogger(log logger.Logger) ModeOption {
	return func(o *modeOptions) { o.log = log }
}

func applyModeOptions(opts []ModeOption) modeOptions {
	options := modeOptions{log: logger.L()}
	for _, o := range opts {
		o(&options)
	}

	return options
}

// flagMode is a mode a flag can carry: a listener's Mode or a hop's ClientMode.
type flagMode interface {
	comparable
	fmt.Stringer
}

// flagSource reads a mode from a flag, starting from the environment
// fallback. It keeps the last accepted value when the flag is missing or
// malformed, logs a bad value once per distinct value rather than once per
// handshake, and runs an optional check before accepting a value.
type flagSource[M flagMode] struct {
	reader FlagReader
	key    string
	// kind names what the mode governs in the change log line.
	kind  string
	parse func(string) (M, error)
	// accept refuses a parsed value with the error to log, or is nil.
	accept func(M) error
	// refused is the log message for a value accept turned down.
	refused string
	log     logger.Logger

	mu           sync.Mutex
	current      M
	source       string
	lastRejected string
}

func newFlagSource[M flagMode](reader FlagReader, key, kind string, fallback M, parse func(string) (M, error), accept func(M) error, refused string, opts []ModeOption) *flagSource[M] {
	options := applyModeOptions(opts)

	return &flagSource[M]{reader: reader, key: key, kind: kind, parse: parse, accept: accept, refused: refused, log: options.log, current: fallback, source: SourceFallback}
}

// read consults the flag and returns the mode in force.
func (s *flagSource[M]) read(ctx context.Context) M {
	mode, _ := s.readWithSource(ctx)

	return mode
}

// readWithSource is read with where the mode came from, under one lock, so
// a caller recording both cannot pair a mode with the source of a later
// evaluation.
func (s *flagSource[M]) readWithSource(ctx context.Context) (M, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Evaluated under the lock so evaluations are accepted in the order they
	// were made: a slow one cannot land after, and undo, a later one. The
	// feature-flag client answers from memory, so the lock is held briefly.
	raw := s.reader.String(ctx, s.key, flagUnset)
	if raw == flagUnset {
		return s.current, s.source
	}

	mode, err := s.parse(raw)
	if err != nil {
		s.reject(ctx, raw, "mtls: mode flag value is not a mode; staying", err)

		return s.current, s.source
	}
	if s.accept != nil {
		if err := s.accept(mode); err != nil {
			s.reject(ctx, raw, s.refused, err)

			return s.current, s.source
		}
	}

	if mode != s.current {
		s.log.Info(ctx, "mtls: "+s.kind+" mode changed",
			zap.String("flag", s.key), zap.Stringer("from", s.current), zap.Stringer("to", mode))
	}
	s.current = mode
	s.source = SourceFlag
	s.lastRejected = ""

	return mode, SourceFlag
}

// Source reports where the mode in force came from.
func (s *flagSource[M]) Source() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.source
}

// reject logs a bad flag value once per distinct value, not per handshake.
func (s *flagSource[M]) reject(ctx context.Context, raw, message string, err error) {
	if s.lastRejected == raw {
		return
	}
	s.lastRejected = raw
	s.log.Warn(ctx, message,
		zap.String("flag", s.key), zap.String("value", raw), zap.Stringer("keeping", s.current), zap.Error(err))
}

// FlagModeSource reads a listener's mode from a flag, starting from the
// environment fallback. It keeps the last accepted value when the flag is
// missing or malformed, and refuses a flip to required while the allow-list
// is empty.
type FlagModeSource struct {
	*flagSource[Mode]
}

// NewFlagModeSource starts at fallback with SourceFallback.
func NewFlagModeSource(reader FlagReader, key string, fallback Mode, allow *AllowList, opts ...ModeOption) *FlagModeSource {
	accept := func(mode Mode) error {
		if mode == ModeRequired && allow.Len() == 0 {
			return ErrInvalidAllowList
		}

		return nil
	}

	return &FlagModeSource{newFlagSource(reader, key, "listener", fallback, ParseMode, accept,
		"mtls: mode flag asks for required with an empty allow-list; staying", opts)}
}

// Mode implements ModeSource.
func (s *FlagModeSource) Mode(ctx context.Context) Mode { return s.read(ctx) }

// ModeWithSource returns the mode in force and its source, read together.
func (s *FlagModeSource) ModeWithSource(ctx context.Context) (Mode, string) {
	return s.readWithSource(ctx)
}

// FlagClientModeSource reads a client hop's mode from a flag, starting from
// the environment fallback, and keeps the last accepted value when the flag
// is missing or malformed.
type FlagClientModeSource struct {
	*flagSource[ClientMode]
}

// NewFlagClientModeSource starts at fallback with SourceFallback.
func NewFlagClientModeSource(reader FlagReader, key string, fallback ClientMode, opts ...ModeOption) *FlagClientModeSource {
	return &FlagClientModeSource{newFlagSource(reader, key, "client hop", fallback, ParseClientMode, nil, "", opts)}
}

// ClientMode implements ClientModeSource.
func (s *FlagClientModeSource) ClientMode(ctx context.Context) ClientMode { return s.read(ctx) }

// ClientModeWithSource returns the mode in force and its source, read together.
func (s *FlagClientModeSource) ClientModeWithSource(ctx context.Context) (ClientMode, string) {
	return s.readWithSource(ctx)
}

// modeSourcer is implemented by the flag-backed sources.
type modeSourcer interface {
	Source() string
}

// sourceOf labels a mode source for the gauges: SourceFlag once a flag value
// has been accepted, SourceFallback for static sources and before.
func sourceOf(src any) string {
	if s, ok := src.(modeSourcer); ok {
		return s.Source()
	}

	return SourceFallback
}

// modeWithSource reads a listener's mode and its source together when the
// source can report both under one lock, as the flag-backed ones do, so a
// gauge sample cannot pair a mode with the source of a later evaluation.
func modeWithSource(ctx context.Context, src ModeSource) (Mode, string) {
	if both, ok := src.(interface {
		ModeWithSource(ctx context.Context) (Mode, string)
	}); ok {
		return both.ModeWithSource(ctx)
	}

	return src.Mode(ctx), sourceOf(src)
}

// clientModeWithSource is modeWithSource for a client hop.
func clientModeWithSource(ctx context.Context, src ClientModeSource) (ClientMode, string) {
	if both, ok := src.(interface {
		ClientModeWithSource(ctx context.Context) (ClientMode, string)
	}); ok {
		return both.ClientModeWithSource(ctx)
	}

	return src.ClientMode(ctx), sourceOf(src)
}
