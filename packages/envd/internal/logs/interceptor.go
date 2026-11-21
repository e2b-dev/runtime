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

func NewUnaryLogInterceptor(logger *zerolog.Logger) connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(
			ctx context.Context,
			req connect.AnyRequest,
		) (connect.AnyResponse, error) {
			ctx = AddRequestIDToContext(ctx)

			res, err := next(ctx, req)

			l := logger.
				Err(err).
				Str("method", DefaultHTTPMethod+" "+req.Spec().Procedure).
				Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

			if err != nil {
				l = l.Int("error_code", int(codeOf(err)))
			}

			if req != nil {
				l = l.Interface("request", req.Any())
			}

			if res != nil && err == nil {
				l = l.Interface("response", res.Any())
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
) error {
	ctx = AddRequestIDToContext(ctx)

	l := logger.Debug().
		Str("method", DefaultHTTPMethod+" "+req.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

	if req != nil {
		l = l.Interface("request", req.Any())
	}

	l.Msg(fmt.Sprintf("%s (server stream start)", formatMethod(req.Spec().Procedure)))

	err := handler(ctx, req, stream)

	logEvent := getErrDebugLogEvent(logger, err).
		Str("method", DefaultHTTPMethod+" "+req.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

	if err != nil {
		logEvent = logEvent.Int("error_code", int(codeOf(err)))
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
) (*connect.Response[R], error) {
	ctx = AddRequestIDToContext(ctx)

	logger.Debug().
		Str("method", DefaultHTTPMethod+" "+stream.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string)).
		Msg(fmt.Sprintf("%s (client stream start)", formatMethod(stream.Spec().Procedure)))

	res, err := handler(ctx, stream)

	logEvent := getErrDebugLogEvent(logger, err).
		Str("method", DefaultHTTPMethod+" "+stream.Spec().Procedure).
		Str(string(OperationIDKey), ctx.Value(OperationIDKey).(string))

	if err != nil {
		logEvent = logEvent.Int("error_code", int(codeOf(err)))
	}

	if res != nil && err == nil {
		logEvent = logEvent.Interface("response", res.Any())
	}

	if res == nil && err == nil {
		logEvent = logEvent.Interface("response", nil)
	}

	logEvent.Msg(fmt.Sprintf("%s (client stream end)", formatMethod(stream.Spec().Procedure)))

	return res, err
}

// getErrDebugLogEvent picks the log level by the nature of err, so a routine
// client-side cancellation of a streaming call (the client stopped reading the
// stream — it timed out, was cancelled by a sibling task, or the caller went
// away) is not logged at ERROR next to genuine failures.
//
//   - nil                      -> Debug (normal completion)
//   - context.Canceled         -> Info  (client went away; expected, not a fault)
//   - context.DeadlineExceeded -> Warn  (the call outran its deadline)
//   - anything else            -> Error (a real failure)
func getErrDebugLogEvent(logger *zerolog.Logger, err error) *zerolog.Event {
	switch {
	case err == nil:
		return logger.Debug() //nolint:zerologlint // this builds an event, it is not expected to return it
	case errors.Is(err, context.Canceled):
		return logger.Info().Err(err) //nolint:zerologlint // this builds an event, it is not expected to return it
	case errors.Is(err, context.DeadlineExceeded):
		return logger.Warn().Err(err) //nolint:zerologlint // this builds an event, it is not expected to return it
	default:
		return logger.Error().Err(err) //nolint:zerologlint // this builds an event, it is not expected to return it
	}
}

// codeOf maps an error to a connect code, recognizing the standard context
// sentinels that connect.CodeOf otherwise reports as CodeUnknown. This keeps
// error_code meaningful for downstream aggregation: a client cancellation is
// CodeCanceled and a deadline is CodeDeadlineExceeded, not Unknown.
func codeOf(err error) connect.Code {
	switch {
	case errors.Is(err, context.Canceled):
		return connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return connect.CodeDeadlineExceeded
	default:
		return connect.CodeOf(err)
	}
}
