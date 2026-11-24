package logs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
)

type OperationID string

const (
	OperationIDKey    OperationID = "operation_id"
	DefaultHTTPMethod string      = "POST"
)

var operationID = atomic.Int32{}

func AssignOperationID() string {
	id := operationID.Add(1)

	return strconv.Itoa(int(id))
}

func AddRequestIDToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, OperationIDKey, AssignOperationID())
}

func formatMethod(method string) string {
	parts := strings.Split(method, ".")
	if len(parts) < 2 {
		return method
	}

	split := strings.Split(parts[1], "/")
	if len(split) < 2 {
		return method
	}

	servicePart := split[0]
	servicePart = strings.ToUpper(servicePart[:1]) + servicePart[1:]

	methodPart := split[1]
	methodPart = strings.ToLower(methodPart[:1]) + methodPart[1:]

	return fmt.Sprintf("%s %s", servicePart, methodPart)
}

// Option adjusts how a wrapper logs a call.
type Option func(*options)

type options struct {
	redact func(msg any) any
}

// WithRedact sets the function that turns a message into the value to log.
func WithRedact(redact func(msg any) any) Option {
	return func(o *options) { o.redact = redact }
}

func newOptions(opts []Option) options {
	o := options{redact: func(msg any) any { return msg }}
	for _, opt := range opts {
		opt(&o)
	}

	return o
}

func NewUnaryLogInterceptor(logger *zerolog.Logger, opts ...Option) connect.UnaryInterceptorFunc {
	o := newOptions(opts)

	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(
			ctx context.Context,
			req connect.AnyRequest,
		) (connect.AnyResponse, error) {
			ctx = AddRequestIDToContext(ctx)

			res, err := next(ctx, req)

			l := endLogEvent(ctx, logger, zerolog.InfoLevel, err).
				Str("method", DefaultHTTPMethod+" "+req.Spec().Procedure).
				Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

			if err != nil {
				l = l.Int("error_code", int(connect.CodeOf(err)))
			}

			if req != nil {
				l = l.Interface("request", o.redact(req.Any()))
			}

			if res != nil && err == nil {
				l = l.Interface("response", o.redact(res.Any()))
			}

			if res == nil && err == nil {
				l = l.Interface("response", nil)
			}

			l.Msg(formatMethod(req.Spec().Procedure))

			return res, err
		})
	}

	return connect.UnaryInterceptorFunc(interceptor)
}

func LogServerStreamWithoutEvents[T any, R any](
	ctx context.Context,
	logger *zerolog.Logger,
	req *connect.Request[R],
	stream *connect.ServerStream[T],
	handler func(ctx context.Context, req *connect.Request[R], stream *connect.ServerStream[T]) error,
	opts ...Option,
) error {
	o := newOptions(opts)
	ctx = AddRequestIDToContext(ctx)

	l := logger.Debug().
		Str("method", DefaultHTTPMethod+" "+req.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

	if req != nil {
		l = l.Interface("request", o.redact(req.Any()))
	}

	l.Msg(fmt.Sprintf("%s (server stream start)", formatMethod(req.Spec().Procedure)))

	err := handler(ctx, req, stream)

	logEvent := endLogEvent(ctx, logger, zerolog.DebugLevel, err).
		Str("method", DefaultHTTPMethod+" "+req.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

	if err != nil {
		logEvent = logEvent.Int("error_code", int(connect.CodeOf(err)))
	} else {
		logEvent = logEvent.Interface("response", nil)
	}

	logEvent.Msg(fmt.Sprintf("%s (server stream end)", formatMethod(req.Spec().Procedure)))

	return err
}

func LogClientStreamWithoutEvents[T any, R any](
	ctx context.Context,
	logger *zerolog.Logger,
	stream *connect.ClientStream[T],
	handler func(ctx context.Context, stream *connect.ClientStream[T]) (*connect.Response[R], error),
	opts ...Option,
) (*connect.Response[R], error) {
	o := newOptions(opts)
	ctx = AddRequestIDToContext(ctx)

	logger.Debug().
		Str("method", DefaultHTTPMethod+" "+stream.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string)).
		Msg(fmt.Sprintf("%s (client stream start)", formatMethod(stream.Spec().Procedure)))

	res, err := handler(ctx, stream)

	logEvent := endLogEvent(ctx, logger, zerolog.DebugLevel, err).
		Str("method", DefaultHTTPMethod+" "+stream.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

	if err != nil {
		logEvent = logEvent.Int("error_code", int(connect.CodeOf(err)))
	}

	if res != nil && err == nil {
		logEvent = logEvent.Interface("response", o.redact(res.Any()))
	}

	if res == nil && err == nil {
		logEvent = logEvent.Interface("response", nil)
	}

	logEvent.Msg(fmt.Sprintf("%s (client stream end)", formatMethod(stream.Spec().Procedure)))

	return res, err
}

// endLogEvent returns the event for a call's final log line: the success level
// when err is nil, debug when the client cancelled the call, and error
// otherwise. A cancelled call can fail in any way, for example a stream write
// to the closed connection, so the request context decides, not the error.
func endLogEvent(ctx context.Context, logger *zerolog.Logger, success zerolog.Level, err error) *zerolog.Event {
	switch {
	case err == nil:
		return logger.WithLevel(success) //nolint:zerologlint // the caller adds fields and sends it
	case errors.Is(ctx.Err(), context.Canceled):
		return logger.Debug().Err(err) //nolint:zerologlint // the caller adds fields and sends it
	default:
		return logger.Error().Err(err) //nolint:zerologlint // the caller adds fields and sends it
	}
}
