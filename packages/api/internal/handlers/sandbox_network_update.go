package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) PutSandboxesSandboxIDNetwork(c *gin.Context, sandboxID string) {
	ctx := c.Request.Context()

	var err error
	sandboxID, err = utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	teamID := auth.MustGetTeamID(c)

	body, err := ginutils.ParseBody[api.SandboxNetworkUpdateConfig](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))
		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	var allowedEntries []string
	if body.AllowOut != nil {
		allowedEntries = *body.AllowOut
	}

	var deniedEntries []string
	if body.DenyOut != nil {
		deniedEntries = *body.DenyOut
	}

	if apiErr := validateEgressRules(allowedEntries, deniedEntries); apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	var egressProxy *sandbox_network.EgressProxyConfig
	if ep := body.EgressProxy; ep != nil {
		if !a.featureFlags.BoolFlag(ctx, featureflags.BYOPProxyEnabledFlag) {
			telemetry.ReportEvent(ctx, "egressProxy update rejected by BYOPProxyEnabledFlag")
			a.sendAPIStoreError(c, http.StatusForbidden,
				"Egress proxy (egressProxy) is not enabled for this team.")

			return
		}

		canonical, err := sandbox_network.ValidateEgressProxy(ctx, apiEgressProxyToConfig(ep), nil)
		if err != nil {
			telemetry.ReportError(ctx, "invalid egress proxy config", err)
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid egress proxy config: %s", err))

			return
		}

		egressProxy = canonical
	}

	if body.Rules != nil {
		sbxInfo, err := a.orchestrator.GetSandbox(ctx, teamID, sandboxID)
		if err != nil {
			if errors.Is(err, sandbox.ErrNotFound) {
				a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))
			} else {
				telemetry.ReportError(ctx, "error getting sandbox for network update", err)
				a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandbox")
			}

			return
		}

		maxDomains := a.featureFlags.IntFlag(ctx, featureflags.MaxNetworkRuleDomains, featureflags.TeamContext(teamID.String()))
		if apiErr := validateNetworkRules(ctx, a.featureFlags, teamID, sbxInfo.EnvdVersion, maxDomains, body.Rules); apiErr != nil {
			a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

			return
		}
	}

	rules := apiRulesToDBRules(body.Rules)

	if apiErr := a.orchestrator.UpdateSandboxNetworkConfig(ctx, teamID, sandboxID, allowedEntries, deniedEntries, rules, body.AllowInternetAccess, egressProxy); apiErr != nil {
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "error updating sandbox network config", apiErr.Err)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	if len(rules) > 0 {
		domains := make([]string, 0, len(rules))
		for domain := range rules {
			domains = append(domains, domain)
		}

		a.posthog.CreateAnalyticsTeamEvent(ctx, teamID.String(), "sandbox with network transform rules updated",
			a.posthog.GetPackageToPosthogProperties(&c.Request.Header).
				Set("sandbox_id", sandboxID).
				Set("domains", domains),
		)
	}

	// Reported off the canonical config rather than the request body, so the
	// event describes the proxy the sandbox actually runs with.
	if egressProxy != nil {
		a.posthog.CreateAnalyticsTeamEvent(ctx, teamID.String(), "sandbox with egress proxy updated",
			a.posthog.GetPackageToPosthogProperties(&c.Request.Header).
				Set("sandbox_id", sandboxID).
				Set("egress_proxy_auth", egressProxy.Username != ""),
		)
	}

	c.Status(http.StatusNoContent)
}
