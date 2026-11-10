//go:build linux

package build

import (
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/builderrors"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
)

// ClassifyBuildResult maps the outcome of Builder.Build to the value used both
// as the `result` label on template_build_result_total and as the build.result
// span attribute. It is the single place the user-vs-internal decision is made.
//
//   - success: no error and a result was produced.
//   - user_error: the error chain contains a PhaseBuildError (a failed user
//     command, script, image pull, or a build-level cancellation/timeout).
//   - internal_error: anything else.
func ClassifyBuildResult(r *Result, err error) metrics.BuildResultType {
	if err == nil && r != nil {
		return metrics.BuildResultSuccess
	}

	if builderrors.IsUserError(err) {
		return metrics.BuildResultUserError
	}

	return metrics.BuildResultInternalError
}

// builtVcpuSizing is the `built` label on template_build_result_total: resizable
// when the final VM has more vCPUs than are online, not_resizable otherwise, and
// none when the build failed, as it leaves no template.
func builtVcpuSizing(r *Result, err error, vcpus int64) string {
	switch {
	case err != nil || r == nil:
		return "none"
	case r.VcpuCount > vcpus:
		return "resizable"
	default:
		return "not_resizable"
	}
}
