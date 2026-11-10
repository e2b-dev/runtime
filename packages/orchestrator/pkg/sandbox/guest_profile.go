package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// GuestProfile returns the profile of the sandbox's guest OS. A sandbox
// without a config (no template metadata) has the default profile.
func (c *Config) GuestProfile() guest.Profile {
	if c == nil {
		return guest.Default()
	}

	return c.FirecrackerConfig.GuestProfile()
}

// ErrEnvdTokenNotReplaceable refuses the start of a sandbox whose envd would
// answer /init with 401: the snapshot has no MMDS store to authorize a new token.
var ErrEnvdTokenNotReplaceable = errors.New("the snapshot has no MMDS store, so its envd accepts only the access token it already holds")

// refuseEnvdTokenReplacement refuses a restore under another access token than
// the one the snapshot's envd holds, when the snapshot has no MMDS store to
// authorize the replacement. An envd that holds none takes any token, an
// unrecorded holder is left to envd, and a restore that sends no /init
// (SkipEnvdWait) gives envd no token to refuse.
func refuseEnvdTokenReplacement(meta metadata.Template, token *string, initializesEnvd bool) error {
	held := meta.EnvdAccessTokenHash
	if !initializesEnvd || !meta.NoMmdsStore || held == "" {
		return nil
	}
	if held == keys.HashAccessToken("") || held == keys.HashAccessToken(utils.DerefOrDefault(token, "")) {
		return nil
	}

	return fmt.Errorf("%w: a sandbox starts from it only with the token of the sandbox it was taken from", ErrEnvdTokenNotReplaceable)
}

// ErrColdBootVcpus refuses a cold boot whose VM is larger than its vCPU
// target when the guest onlines every vCPU of its VM: the online cap is the
// maxcpus= kernel argument, which a firmware boot does not take.
var ErrColdBootVcpus = errors.New("the guest onlines every vCPU of its VM on a cold boot")

// refuseColdBootVcpus refuses such a cold boot sized above its target,
// whether by an explicit maximum or by the size a reboot inherits from its lineage.
func refuseColdBootVcpus(config *Config) error {
	if !config.GuestProfile().Boot().RefuseVcpusAboveTarget || config.ConfiguredVmVcpus <= config.Vcpu {
		return nil
	}

	return fmt.Errorf("%w: the VM would have %d vCPUs for a target of %d", ErrColdBootVcpus, config.ConfiguredVmVcpus, config.Vcpu)
}

// envdTokenAtPause is what a pause records about its envd's token: nothing when
// the microVM has an MMDS store, else the hash of the token this life's /init
// gave envd, or the recorded one when this life sent no /init.
func envdTokenAtPause(storeMissing, envdInitialized bool, token *string, recorded string) (bool, string) {
	if !storeMissing {
		return false, ""
	}
	if !envdInitialized {
		return true, recorded
	}

	return true, keys.HashAccessToken(utils.DerefOrDefault(token, ""))
}

// ErrTLSInterceptionUnsupported refuses egress rules that make the proxy
// intercept TLS on a guest whose profile sends envd no CA bundle: the proxy
// would intercept with a CA the guest does not trust.
var ErrTLSInterceptionUnsupported = errors.New("egress rules make the proxy intercept TLS, which needs the egress CA bundle in the guest")

// RefuseTLSInterception refuses an egress config that makes the proxy
// intercept TLS for a profile that takes no CA bundle. The proxy intercepts a
// domain whenever the config holds a rule set for it, with or without a
// transform; a domain allowlist alone is not intercepted.
func RefuseTLSInterception(profile guest.Profile, egress *orchestrator.SandboxNetworkEgressConfig) error {
	if !profile.EnvdInit().NoCABundle {
		return nil
	}
	for domain, rules := range egress.GetRules() {
		if rules != nil {
			return fmt.Errorf("%w: the %s guest has no CA bundle capability, and the egress rules for %q are intercepted", ErrTLSInterceptionUnsupported, profile.Label(), domain)
		}
	}

	return nil
}

// envdCABundleFor is the CA bundle /init hands envd; a profile can send none.
func envdCABundleFor(config *Config, bundle string) string {
	if config.GuestProfile().EnvdInit().NoCABundle {
		return ""
	}

	return bundle
}

// rebootEnvdTimeoutFor is the envd wait a cold boot of this guest gets.
func rebootEnvdTimeoutFor(config *Config) time.Duration {
	if d := config.GuestProfile().EnvdInit().ColdBootEnvdTimeout; d > 0 {
		return d
	}

	return rebootEnvdTimeout
}

// restoreEnvdTimeoutFor is the envd wait a restore of this guest gets: the
// flag's, raised to the profile's floor.
func restoreEnvdTimeoutFor(config *Config, flagTimeout time.Duration) time.Duration {
	return max(flagTimeout, config.GuestProfile().EnvdInit().RestoreEnvdTimeoutFloor)
}

// rebootCPUTemplateFor is rebootCPUTemplate for the guest: a profile that
// applies no CPU template gets neither the build's nor the override.
func rebootCPUTemplateFor(boot guest.BootConfig, meta metadata.Template, override ldvalue.Value) (*cputemplate.Template, bool, error) {
	if boot.NoCPUTemplate {
		return nil, false, nil
	}

	return rebootCPUTemplate(meta, override)
}

var cpuTemplateSkippedOnce sync.Map

// logCPUTemplateSkipped says once per process and profile that its cold boots
// apply no CPU template, so a fleet-wide override that seems ignored there is
// not a mystery.
func logCPUTemplateSkipped(ctx context.Context, profile guest.Profile) {
	if _, seen := cpuTemplateSkippedOnce.LoadOrStore(profile.Name(), struct{}{}); seen {
		return
	}
	logger.L().Info(ctx, "this guest's cold boots apply no CPU template, neither the build's nor the reboot override",
		zap.String("guest_os", profile.Label()))
}
