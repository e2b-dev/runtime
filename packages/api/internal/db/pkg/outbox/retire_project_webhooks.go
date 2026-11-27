package outbox

import (
	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

const WebhookRetirementQueue = "webhook_retirement"

type RetireProjectWebhooks struct {
	ProjectID uuid.UUID `json:"project_id"`
}

func (RetireProjectWebhooks) Kind() string { return "retire_project_webhooks" }

func (RetireProjectWebhooks) InsertOpts() river.InsertOpts {
	return teardownInsertOpts(WebhookRetirementQueue)
}
