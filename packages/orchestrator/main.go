//go:build linux

package main

import (
	"context"
	"os"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/factories"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/tcpfirewall"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/version"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

var commitSHA string

func main() {
	applyTestFlagOverrides()

	factories.Run(factories.Options{
		Version:       version.Version,
		CommitSHA:     commitSHA,
		EgressFactory: defaultEgressFactory,
	})
}

func applyTestFlagOverrides() {
	// "default" (the harness input default) means the flag's own fallback —
	// dedup disabled — not an override that quietly enables it modeless.
	if mode := os.Getenv("TESTS_MEMFILE_DIFF_DEDUP_MODE"); mode != "" && mode != "default" {
		// direct_io also engages a representative fetch-defrag budget so the
		// promotion/defrag path executes in CI; production tunes the numbers
		// in the flag, the shape is what matters here.
		defrag := 0
		if mode == "direct_io" {
			defrag = 1
		}
		featureflags.OverrideJSONFlag(featureflags.MemfileDiffDedupFlag, ldvalue.FromJSONMarshal(map[string]any{
			"enabled":                        true,
			"bestEffort":                     mode == "best_effort",
			"directIO":                       mode == "direct_io",
			"maxFetchWindowsPerBlock":        2 * defrag,
			"maxPromotedParentPagesPerBlock": 64 * defrag,
			"maxPagesPerPromotedFrame":       8 * defrag,
		}))
	}
	if os.Getenv("TESTS_DISABLE_MEMFD") == "true" {
		featureflags.OverrideBoolFlag(featureflags.UseMemFdFlag, false)
	}
}

func defaultEgressFactory(_ context.Context, deps *factories.Deps) (*factories.EgressSetup, error) {
	fw := tcpfirewall.New(
		deps.Logger,
		deps.Config.NetworkConfig,
		deps.Sandboxes,
		deps.MeterProvider,
		deps.FeatureFlags,
	)

	return &factories.EgressSetup{
		Proxy: fw,
		Start: fw.Start,
		Close: fw.Close,
	}, nil
}
