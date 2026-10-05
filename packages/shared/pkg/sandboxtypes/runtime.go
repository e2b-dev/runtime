// Package sandboxtypes provides execution metadata without worker resource ownership.
package sandboxtypes

import (
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// SandboxType distinguishes build sandboxes from regular sandboxes.
type SandboxType string

const (
	SandboxTypeSandbox SandboxType = "sandbox"
	SandboxTypeBuild   SandboxType = "build"
)

// String returns the sandbox type as a string, defaulting to "sandbox" if empty.
func (t SandboxType) String() string {
	if t == "" {
		return string(SandboxTypeSandbox)
	}

	return string(t)
}

// EgressClass selects the configured egress DSCP for a sandbox.
type EgressClass uint8

const (
	// EgressClassSandbox is a regular, customer-facing sandbox.
	EgressClassSandbox EgressClass = iota
	// EgressClassBuild is a template-build sandbox.
	EgressClassBuild
)

func (c EgressClass) String() string {
	if c == EgressClassBuild {
		return "build"
	}

	return "sandbox"
}

// EgressClass maps build sandboxes to build traffic and defaults to regular sandbox traffic.
func (t SandboxType) EgressClass() EgressClass {
	if t == SandboxTypeBuild {
		return EgressClassBuild
	}

	return EgressClassSandbox
}

type RuntimeMetadata struct {
	TemplateID  string
	SandboxID   string
	ExecutionID string

	// TeamID is best-effort metadata; not always populated so do not use for
	// decisions or feature-flag targeting.
	TeamID string

	BuildID     string
	SandboxType SandboxType
}

// LogFields returns the identity fields for every line logged on this
// sandbox's behalf. Ids that are not populated are omitted rather than emitted
// blank.
func (r RuntimeMetadata) LogFields() []zap.Field {
	fields := make([]zap.Field, 0, 5)

	for _, f := range []struct {
		value string
		field func(string) zap.Field
	}{
		{r.SandboxID, logger.WithSandboxID},
		{r.TemplateID, logger.WithTemplateID},
		{r.TeamID, logger.WithTeamID},
		{r.BuildID, logger.WithBuildID},
		{r.ExecutionID, logger.WithExecutionID},
	} {
		if f.value != "" {
			fields = append(fields, f.field(f.value))
		}
	}

	return fields
}

// Logger returns the process logger tagged with LogFields.
func (r RuntimeMetadata) Logger() logger.Logger {
	return logger.L().With(r.LogFields()...)
}
