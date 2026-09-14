package templatecache

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

var (
	ErrTemplateNotFound = errors.New("template not found")
	ErrAccessDenied     = errors.New("access denied")
	ErrClusterMismatch  = errors.New("template not available in cluster")
)

type templateNotFoundError struct {
	Identifier string
}

func (e templateNotFoundError) Error() string {
	return fmt.Sprintf("%s: %s", ErrTemplateNotFound, e.Identifier)
}

func (e templateNotFoundError) Unwrap() error {
	return ErrTemplateNotFound
}

type templateTagNotFoundError struct {
	Tag string
}

func (e templateTagNotFoundError) Error() string {
	return fmt.Sprintf("%s: tag %s", ErrTemplateNotFound, e.Tag)
}

func (e templateTagNotFoundError) Unwrap() error {
	return ErrTemplateNotFound
}

type TemplateRef struct {
	Identifier string
	Visible    bool
}

func ErrorToAPIError(err error, fallbackIdentifier string) *api.APIError {
	return ToAPIError(err, fallbackIdentifier)
}

func ToAPIError(err error, identifier string) *api.APIError {
	var tagErr templateTagNotFoundError
	switch {
	case errors.As(err, &tagErr):
		return &api.APIError{
			Code:      http.StatusNotFound,
			ClientMsg: fmt.Sprintf("tag '%s' does not exist for template '%s'", tagErr.Tag, identifier),
			Err:       err,
		}
	case errors.Is(err, ErrTemplateNotFound):
		if notFoundErr, ok := errors.AsType[templateNotFoundError](err); ok {
			identifier = notFoundErr.Identifier
		}

		return &api.APIError{
			Code:      http.StatusNotFound,
			ClientMsg: fmt.Sprintf("template '%s' not found", identifier),
			Err:       err,
		}
	case errors.Is(err, ErrAccessDenied):
		return &api.APIError{
			Code:      http.StatusForbidden,
			ClientMsg: fmt.Sprintf("you don't have access to template '%s'", identifier),
			Err:       err,
		}
	case errors.Is(err, ErrClusterMismatch):
		return &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("template '%s' is not available in the requested cluster", identifier),
			Err:       err,
		}
	}

	return &api.APIError{
		Code:      http.StatusInternalServerError,
		ClientMsg: fmt.Sprintf("error resolving template '%s'", identifier),
		Err:       err,
	}
}

func (r TemplateRef) APIError(err error) *api.APIError {
	if !r.Visible && (errors.Is(err, ErrAccessDenied) || errors.Is(err, ErrTemplateNotFound)) {
		return &api.APIError{
			Code:      http.StatusNotFound,
			ClientMsg: fmt.Sprintf("template '%s' not found", r.Identifier),
			Err:       err,
		}
	}

	var tagErr templateTagNotFoundError
	if !errors.As(err, &tagErr) {
		return ToAPIError(err, r.Identifier)
	}

	clientMsg := fmt.Sprintf("tag '%s' does not exist for template '%s'", tagErr.Tag, r.Identifier)

	return &api.APIError{
		Code:      http.StatusNotFound,
		ClientMsg: clientMsg,
		Err:       err,
	}
}
