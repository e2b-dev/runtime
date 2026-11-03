package orchestrator

import (
	"context"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// updateStoreWriteForm selects the store's write key form from the flag and
// logs a change of form, so an operator can confirm a flip on every
// replica. A flag not served, by a client not ready, a value of another
// type, a key the environment does not define or a flag turned off with
// no off variation, keeps the form already selected: the team form from
// the start until the flag is served. The flag carries an off variation of
// false, the UI's default, so a flip off reaches every replica.
func (o *Orchestrator) updateStoreWriteForm(ctx context.Context) {
	perSandbox, served := o.featureFlagsClient.BoolFlagOverride(ctx, featureflags.SandboxStorePerSandboxKeysFlag)
	if !served {
		logger.L().Debug(ctx, "The sandbox store write form flag was not served, keeping the current form", zap.String("flag", featureflags.SandboxStorePerSandboxKeysFlag.Key()))

		return
	}

	if perSandbox != o.redisStorage.WritesPerSandbox() {
		logger.L().Info(ctx, "Sandbox store write form changed", zap.Bool("per_sandbox_keys", perSandbox))
	}
	o.redisStorage.SetPerSandboxWrites(perSandbox)
}
