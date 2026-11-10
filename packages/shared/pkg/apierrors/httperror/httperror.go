// Package httperror holds the API's error type and its HTTP response, with no
// web framework dependency, so any net/http server can answer in the API's
// error shape.
package httperror

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// ContentType is the Content-Type of every error response.
const ContentType = "application/json; charset=utf-8"

// ShouldRetryHeader carries an error's explicit retry classification.
const ShouldRetryHeader = "Should-Retry"

var _ error = (*APIError)(nil)

// Retryability states whether the condition behind an error may clear, so
// that retrying the request could succeed. It says nothing about idempotency
// keys. Error responses carry it as ShouldRetryHeader; the zero value leaves the header unchanged.
type Retryability int

const (
	UndefinedRetry Retryability = iota
	CanRetry
	NoRetry
)

// APIError represents a structured error with an HTTP status code and client-facing message.
type APIError struct {
	Err       error
	ClientMsg string
	Code      int
	// ErrorCode is an optional machine-readable semantic code (e.g.
	// "sandbox_capacity_unavailable") rendered as error_code in the body.
	ErrorCode string
	Retryable Retryability
}

func (e *APIError) Error() string {
	return e.Err.Error()
}

// body is the JSON body of an error response: the client-facing message and
// codes, never Err.
type body struct {
	Code      int32  `json:"code"`
	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message"`
}

func encode(apiErr *APIError) ([]byte, error) {
	encoded, err := json.Marshal(body{Code: int32(apiErr.Code), ErrorCode: apiErr.ErrorCode, Message: apiErr.ClientMsg})
	if err != nil {
		return nil, fmt.Errorf("encode error body: %w", err)
	}

	return encoded, nil
}

func setRetryHeader(header http.Header, retryable Retryability) {
	switch retryable {
	case CanRetry:
		header.Set(ShouldRetryHeader, "true")
	case NoRetry:
		header.Set(ShouldRetryHeader, "false")
	}
}

// Write answers w with the error. If the body cannot be encoded, w still gets
// the status and Should-Retry, with no body, and Write returns the encoding error.
func Write(w http.ResponseWriter, apiErr *APIError) error {
	setRetryHeader(w.Header(), apiErr.Retryable)
	encoded, err := encode(apiErr)
	if err != nil {
		w.WriteHeader(apiErr.Code)

		return err
	}

	w.Header().Set("Content-Type", ContentType)
	w.WriteHeader(apiErr.Code)
	if _, err := w.Write(encoded); err != nil {
		return fmt.Errorf("write error body: %w", err)
	}

	return nil
}

// Response is the error as an *http.Response, for code that must return one
// rather than write it, such as an http.RoundTripper. It sets only the status,
// headers and body.
func Response(apiErr *APIError) (*http.Response, error) {
	encoded, err := encode(apiErr)
	if err != nil {
		return nil, err
	}

	header := http.Header{"Content-Type": {ContentType}}
	setRetryHeader(header, apiErr.Retryable)

	return &http.Response{
		StatusCode: apiErr.Code,
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(encoded)),
	}, nil
}
