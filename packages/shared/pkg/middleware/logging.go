package middleware

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// Based on https://github.com/gin-contrib/zap

// Fn is a function to get zap fields from gin.Context
type Fn func(c *gin.Context) []zapcore.Field

// Skipper is a function to skip logs based on provided Context
type Skipper func(c *gin.Context) bool

type Config struct {
	TimeFormat      string
	UTC             bool
	SkipPaths       []string
	SkipPathRegexps []*regexp.Regexp
	Context         Fn
	DefaultLevel    zapcore.Level
	// Skipper indicates which logs should not be written. Optional.
	Skipper Skipper
}

func LoggingMiddleware(logger logger.Logger, conf Config) gin.HandlerFunc {
	skipPaths := make(map[string]bool, len(conf.SkipPaths))
	for _, path := range conf.SkipPaths {
		skipPaths[path] = true
	}

	return func(c *gin.Context) {
		ctx := c.Request.Context()
		start := time.Now()

		// Preserve this if any middleware modifies these values
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()
		track := true

		if _, ok := skipPaths[path]; ok || (conf.Skipper != nil && conf.Skipper(c)) {
			track = false
		}

		if track && len(conf.SkipPathRegexps) > 0 {
			for _, reg := range conf.SkipPathRegexps {
				if !reg.MatchString(path) {
					continue
				}

				track = false

				break
			}
		}

		if track {
			end := time.Now()
			latency := end.Sub(start)
			if conf.UTC {
				end = end.UTC()
			}

			fields := []zapcore.Field{
				zap.String("method", c.Request.Method),
				zap.String("path", path),
				zap.String("query", query),
				zap.String("user-agent", c.Request.UserAgent()),
				zap.Duration("latency", latency),
			}

			status := c.Writer.Status()
			cause := CancelCause(c)
			if errors.Is(cause, ErrRequestTimeout) {
				fields = append(fields, zap.Bool("request_timeout", true))
			} else if errors.Is(cause, context.Canceled) {
				status = StatusClientClosedRequest // 499
				fields = append(fields, zap.Bool("client_canceled", true))
			}

			fields = append(fields, zap.Int("status", status))
			if conf.TimeFormat != "" {
				fields = append(fields, zap.String("time", end.Format(conf.TimeFormat)))
			}

			if conf.Context != nil {
				fields = append(fields, conf.Context(c)...)
			}

			// Log errors if present
			if len(c.Errors) > 0 {
				errs := make([]error, 0, len(c.Errors))
				for _, e := range c.Errors {
					errs = append(errs, e.Err)
				}
				fields = append(fields, zap.Error(errors.Join(errs...)))
			}

			level := conf.DefaultLevel
			switch {
			case status >= http.StatusInternalServerError:
				level = zapcore.ErrorLevel
			case status == http.StatusNotFound:
				level = zapcore.InfoLevel
			case status >= http.StatusBadRequest:
				level = zapcore.WarnLevel
			}

			logger.Log(ctx, level, path, fields...)
		}
	}
}
