//go:build linux

package phases

import (
	"errors"
)

type PhaseBuildError struct {
	Phase string
	Step  string
	Err   error
}

func (e *PhaseBuildError) Error() string {
	return e.Err.Error()
}

func (e *PhaseBuildError) Unwrap() error {
	return e.Err
}

func NewPhaseBuildError(phaseMetadata PhaseMeta, err error) *PhaseBuildError {
	return &PhaseBuildError{
		Phase: string(phaseMetadata.Phase),
		Step:  stepString(phaseMetadata),
		Err:   err,
	}
}

func UnwrapPhaseBuildError(err error) *PhaseBuildError {
	if phaseBuildError, ok := errors.AsType[*PhaseBuildError](err); ok {
		return phaseBuildError
	}

	return nil
}
