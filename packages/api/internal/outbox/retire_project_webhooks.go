package outbox

import (
	"context"

	"github.com/riverqueue/river"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/outbox"
	webhooks "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

type retireProjectWebhooksWorker struct {
	river.WorkerDefaults[outbox.RetireProjectWebhooks]

	client webhooks.WebhookManagementServiceClient
	steps  sharedriver.JobSteps
}

func (w *retireProjectWebhooksWorker) Work(ctx context.Context, job *river.Job[outbox.RetireProjectWebhooks]) error {
	return w.steps.Step(ctx, "retire_webhooks", func(ctx context.Context) error {
		_, err := w.client.RetireProjectWebhooks(ctx, webhooks.RetireProjectWebhooksRequest_builder{ProjectId: job.Args.ProjectID.String()}.Build())

		return err
	})
}
