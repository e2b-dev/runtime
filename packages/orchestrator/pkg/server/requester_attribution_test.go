package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// TestRequesterAttribution pins that an explicit API kill records the client IP
// and User-Agent on the killed event, while a kill without them (timeout,
// eviction, internal) adds nothing.
func TestRequesterAttribution(t *testing.T) {
	t.Parallel()

	ip := "203.0.113.7"
	ua := "e2b-python-sdk/1.2.3"

	t.Run("request kill records requester fields", func(t *testing.T) {
		t.Parallel()
		in := &orchestrator.SandboxDeleteRequest{
			SandboxId:          "isbx",
			RequesterIp:        &ip,
			RequesterUserAgent: &ua,
		}
		data := map[string]any{}
		requesterAttribution(in)(data)
		assert.Equal(t, ip, data["requester_ip"])
		assert.Equal(t, ua, data["requester_user_agent"])
	})

	t.Run("kill without requester adds nothing", func(t *testing.T) {
		t.Parallel()
		in := &orchestrator.SandboxDeleteRequest{SandboxId: "isbx"}
		data := map[string]any{}
		requesterAttribution(in)(data)
		_, hasIP := data["requester_ip"]
		_, hasUA := data["requester_user_agent"]
		assert.False(t, hasIP, "requester_ip must be absent")
		assert.False(t, hasUA, "requester_user_agent must be absent")
		assert.Empty(t, data)
	})
}
