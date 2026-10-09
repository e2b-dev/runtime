package sandboxes

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

// gpg has no connect timeout of its own, and keyserver.ubuntu.com round-robins
// over addresses that black-hole packets. Keep this under the sandbox TTL.
const gpgKeyserverTimeout = 25 * time.Second

// ensureNetworkTestTemplate returns the template the network tests run on:
// the suite's prebuilt base template. It already carries every tool they
// exec (curl, ssh, gpg, python3), so nothing is built or installed for them.
// The former custom template installed those tools with apt on top of
// ubuntu:22.04, and one slow Ubuntu mirror then timed its build out and
// failed every network test in the shard at once.
func ensureNetworkTestTemplate(t *testing.T) string {
	t.Helper()

	return setup.SandboxTemplateID
}

// dnsProbe is a UDP DNS query for an A record, sent to the server in the
// first argument for the name in the second, as many times as the third says
// with a three-second wait each, as dig does with +timeout=3 and +tries: it
// exits 0 on a reply and 1 on none. It stands in for dig, which the base
// template does not carry.
const dnsProbe = `import socket, struct, sys
server, domain, tries = sys.argv[1], sys.argv[2], int(sys.argv[3])
query = struct.pack(">HHHHHH", 0x1234, 0x0100, 1, 0, 0, 0)
for label in domain.rstrip(".").split("."):
    query += bytes([len(label)]) + label.encode()
query += b"\x00" + struct.pack(">HH", 1, 1)
for _ in range(tries):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(3)
    try:
        sock.sendto(query, (server, 53))
        reply, _ = sock.recvfrom(512)
    except OSError:
        continue
    if reply[:2] == query[:2]:
        sys.exit(0)
sys.exit(1)
`

// =============================================================================
// Test helper functions for network egress assertions
// =============================================================================

// assertSuccessfulHTTPRequest asserts that an HTTP/HTTPS request to the given URL succeeds
func assertSuccessfulHTTPRequest(t *testing.T, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, url string, msg string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		err := utils.ExecCommand(t, ctx, sbx, envdClient, "curl", "--connect-timeout", "5", "--max-time", "10", "-Iks", url)
		require.NoError(c, err, msg)
	}, 30*time.Second, time.Second)
}

// assertBlockedHTTPRequest asserts that an HTTP/HTTPS request to the given URL is blocked
func assertBlockedHTTPRequest(t *testing.T, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, url string, msg string) {
	t.Helper()
	err := utils.ExecCommand(t, ctx, sbx, envdClient, "curl", "--connect-timeout", "3", "--max-time", "5", "-Iks", url)
	require.Error(t, err, msg)
	require.Contains(t, err.Error(), "failed with exit code", "Expected connection failure message")
}

// assertSuccessfulDNSQuery asserts that a UDP DNS query to the given server
// is answered, within the three tries dig would make.
func assertSuccessfulDNSQuery(t *testing.T, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, dnsServer, domain string, msg string) {
	t.Helper()
	err := utils.ExecCommand(t, ctx, sbx, envdClient, "python3", "-c", dnsProbe, dnsServer, domain, "3")
	require.NoError(t, err, msg)
}

// assertBlockedDNSQuery asserts that a UDP DNS query to the given server gets
// no answer, in the single try a blocked server deserves.
func assertBlockedDNSQuery(t *testing.T, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, dnsServer, domain string, msg string) {
	t.Helper()
	err := utils.ExecCommand(t, ctx, sbx, envdClient, "python3", "-c", dnsProbe, dnsServer, domain, "1")
	require.Error(t, err, msg)
}

// assertHTTPResponseFromServer asserts that an HTTPS request returns a response from the expected server
// This is used to verify that DNS spoofing redirection worked correctly
func assertHTTPResponseFromServer(t *testing.T, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, url, expectedServerHeader, msg string) {
	t.Helper()
	output, err := utils.ExecCommandWithOutput(t, ctx, sbx, envdClient, nil, "user", "curl", "--connect-timeout", "5", "--max-time", "10", "-Iks", url)
	require.NoError(t, err, msg)
	require.Contains(t, strings.ToLower(output), strings.ToLower(expectedServerHeader),
		"%s - expected server header to contain %q, got response: %s", msg, expectedServerHeader, output)
}

// =============================================================================
// IP and CIDR-based filtering tests
// =============================================================================

// TestEgressFirewallAllowSpecificIP tests that only allowed IPs can be accessed
func TestEgressFirewallAllowSpecificIP(t *testing.T) {
	t.Parallel()

	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Use Google's DNS IP (8.8.8.8) as allowed, and Cloudflare's (1.1.1.1) as implicitly blocked
	allowedIPs := []string{"8.8.8.8"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowedIPs,
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to allowed IP 8.8.8.8 to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to non-allowed IP 1.1.1.1 to fail")
}

// TestEgressFirewallBlockSpecificIP tests that blocked IPs cannot be accessed
func TestEgressFirewallBlockSpecificIP(t *testing.T) {
	t.Parallel()

	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block Cloudflare's DNS IP
	blockedIPs := []string{"1.1.1.1"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			DenyOut: &blockedIPs,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to blocked IP 1.1.1.1 to fail")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to non-blocked IP 8.8.8.8 to succeed")
}

// TestEgressFirewallAllowCIDRRange tests that CIDR ranges work for allowing IPs
func TestEgressFirewallAllowCIDRRange(t *testing.T) {
	t.Parallel()

	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Allow Cloudflare's range (1.1.1.0/24) which includes 1.1.1.1
	allowedRanges := []string{"1.1.1.0/24"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowedRanges,
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to IP within allowed CIDR range (1.1.1.1) to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to IP outside allowed CIDR range (8.8.8.8) to fail")
}

// TestEgressFirewallBlockCIDRRange tests that CIDR ranges work for blocking IPs
func TestEgressFirewallBlockCIDRRange(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block Cloudflare's DNS range (1.1.1.0/24)
	blockedRanges := []string{"1.1.1.0/24"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			DenyOut: &blockedRanges,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to IP within blocked CIDR range (1.1.1.1) to fail")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.2", "Expected curl to IP within blocked CIDR range (1.1.1.2) to fail")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to IP outside blocked CIDR range (8.8.8.8) to succeed")
}

// TestEgressFirewallAllowAndBlockCombination tests that allowOut takes precedence over blockOut
func TestEgressFirewallAllowAndBlockCombination(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but explicitly allow one IP - allowOut should take precedence
	allowedIPs := []string{"8.8.8.8"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowedIPs,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to explicitly allowed IP (8.8.8.8) to succeed even though 0.0.0.0/0 is blocked")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to non-allowed IP (1.1.1.1) to fail")
}

// TestEgressFirewallPersistsAfterResume tests that network config persists after pause/resume
func TestEgressFirewallPersistsAfterResume(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	sbxTimeout := int32(60)
	client := setup.GetAPIClient()

	// Allow only specific IPs
	allowedIPs := []string{"8.8.8.8", "8.8.4.4"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowedIPs,
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	// Test before pause
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to allowed IP (8.8.8.8) to succeed before pause")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to non-allowed IP (1.1.1.1) to fail before pause")

	// Pause the sandbox
	respPause, err := client.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err, "Expected to pause sandbox without error")
	require.Equal(t, http.StatusNoContent, respPause.StatusCode(), "Expected status code 204 No Content, got %d", respPause.StatusCode())

	// Resume the sandbox
	respResume, err := client.PostSandboxesSandboxIDResumeWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{
		Timeout: &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err, "Expected to resume sandbox without error")
	require.Equal(t, http.StatusCreated, respResume.StatusCode(), "Expected status code 201 Created, got %d", respResume.StatusCode())

	// Test after resume
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to allowed IP (8.8.8.8) to succeed after resume")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to non-allowed IP (1.1.1.1) to fail after resume")
}

// TestEgressFirewallEmptyConfig tests that empty allowOut list is treated as no restriction
func TestEgressFirewallEmptyConfig(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Create sandbox with empty allowOut list - should be treated as no restriction
	emptyAllowList := []string{}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &emptyAllowList,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to succeed with empty allowOut list")
}

// TestEgressFirewallAllowAll tests that 0.0.0.0/0 allows all traffic
func TestEgressFirewallAllowAll(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Allow all IPs using 0.0.0.0/0
	allowAll := []string{"0.0.0.0/0"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowAll,
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to 8.8.8.8 to succeed with 0.0.0.0/0 allow")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to 1.1.1.1 to succeed with 0.0.0.0/0 allow")
}

// TestEgressFirewallAllowOverridesBlock tests that allowOut takes precedence over blockOut
func TestEgressFirewallAllowOverridesBlock(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block specific IP but also allow it - allow should take precedence
	allowedIPs := []string{"1.1.1.1"}
	blockedIPs := []string{"1.1.1.1"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowedIPs,
			DenyOut:  &blockedIPs,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to allowed IP (1.1.1.1) to succeed since allow takes precedence over block")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to other IP (8.8.8.8) to succeed")
}

// TestEgressFirewallMultipleAllowedIPs tests multiple allowed IPs
func TestEgressFirewallMultipleAllowedIPs(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Allow multiple specific IPs
	allowedIPs := []string{"8.8.8.8", "8.8.4.4", "1.1.1.1"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowedIPs,
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to allowed IP (8.8.8.8) to succeed")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.4.4", "Expected curl to allowed IP (8.8.4.4) to succeed")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to allowed IP (1.1.1.1) to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://9.9.9.9", "Expected curl to non-allowed IP (9.9.9.9) to fail")
}

// TestEgressFirewallWithInternetAccessFalse tests that network config takes precedence over allow_internet_access
func TestEgressFirewallWithInternetAccessFalse(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Set both network config and allow_internet_access=false
	// Network config should take precedence - allowed IPs should still work
	allowedIPs := []string{"8.8.8.8"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithAllowInternetAccess(false),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowedIPs,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to succeed for allowed IP even with allow_internet_access=false (network config takes precedence)")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to non-allowed IP to fail")
}

// TestEgressFirewallPrivateIPRangesAlwaysBlocked tests that private IP ranges cannot be allowed
// Note: Private IP ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 169.254.0.0/16) are always blocked
// by the orchestrator for security reasons. Attempting to specify them in allowOut should result in
// a sandbox creation failure.
func TestEgressFirewallPrivateIPRangesAlwaysBlocked(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	client := setup.GetAPIClient()
	timeout := int32(60)

	testCases := []struct {
		name      string
		allowedIP string
		testIP    string
		testDesc  string
	}{
		{
			name:      "private_range_10.0.0.0/8",
			allowedIP: "10.0.0.0/8",
			testIP:    "10.0.0.1",
			testDesc:  "10.0.0.0/8 range",
		},
		{
			name:      "private_range_192.168.0.0/16",
			allowedIP: "192.168.0.0/16",
			testIP:    "192.168.0.1",
			testDesc:  "192.168.0.0/16 range",
		},
		{
			name:      "private_range_172.16.0.0/12",
			allowedIP: "172.16.0.0/12",
			testIP:    "172.16.0.1",
			testDesc:  "172.16.0.0/12 range",
		},
		{
			name:      "link_local_169.254.0.0/16",
			allowedIP: "169.254.0.0/16",
			testIP:    "169.254.0.1",
			testDesc:  "169.254.0.0/16 range (link-local)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			// Try to create a sandbox with a private IP range in allowOut
			allowedIPs := []string{tc.allowedIP}

			sbx := utils.SetupSandboxWithCleanup(t, client,
				utils.WithTemplateID(templateID),
				utils.WithTimeout(timeout),
				utils.WithNetwork(&api.SandboxNetworkConfig{
					AllowOut: &allowedIPs,
				}),
			)

			envdClient := setup.GetEnvdClient(t, ctx)

			assertBlockedHTTPRequest(t, ctx, sbx, envdClient, tc.testIP, fmt.Sprintf("Expected curl to private IP %s to fail", tc.testIP))
		})
	}
}

// TestEgressFirewallAllowAllDuplicate tests that adding 0.0.0.0/0 twice works correctly
func TestEgressFirewallAllowAllDuplicate(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Add 0.0.0.0/0 twice in the allowOut list
	allowAll := []string{"0.0.0.0/0", "0.0.0.0/0"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowAll,
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to 8.8.8.8 to succeed with duplicate 0.0.0.0/0 allow")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to 1.1.1.1 to succeed with duplicate 0.0.0.0/0 allow")
}

// TestEgressFirewallRegularIPThenAllowAll tests that adding a regular IP and then 0.0.0.0/0 works correctly
func TestEgressFirewallRegularIPThenAllowAll(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Add a specific IP followed by 0.0.0.0/0
	allowList := []string{"8.8.8.8", "0.0.0.0/0"}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.8.8", "Expected curl to 8.8.8.8 to succeed")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to 1.1.1.1 to succeed (0.0.0.0/0 allows all)")
}

// TestEgressFirewallAllowDomainThroughBlockedInternet tests that a specific domain can be allowed
// when all internet traffic is blocked via 0.0.0.0/0
func TestEgressFirewallAllowDomainThroughBlockedInternet(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow google.com domain
	allowList := []string{"google.com"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://google.com", "Expected curl to allowed domain (google.com) to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to non-allowed domain (cloudflare.com) to fail")
}

// TestEgressFirewallAllowWildcardDomainThroughBlockedInternet tests that wildcard domain patterns work
func TestEgressFirewallAllowWildcardDomainThroughBlockedInternet(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow *.google.com wildcard domain
	allowList := []string{"*.google.com"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://www.google.com", "Expected curl to subdomain (www.google.com) matching wildcard (*.google.com) to succeed")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://maps.google.com", "Expected curl to subdomain (maps.google.com) matching wildcard (*.google.com) to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to non-matching domain (cloudflare.com) to fail")
}

// TestEgressFirewallExactDomainMatchVsSubdomain tests that exact domain match does not include subdomains
func TestEgressFirewallExactDomainMatchVsSubdomain(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow only exact "google.com" (not subdomains)
	allowList := []string{"google.com"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://google.com", "Expected curl to exact domain (google.com) to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://www.google.com", "Expected curl to subdomain (www.google.com) to fail when only exact domain (google.com) is allowed")
}

// TestEgressFirewallAllowAllDomainsWildcard tests that "*" wildcard allows all domains
func TestEgressFirewallAllowAllDomainsWildcard(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow all domains with "*" wildcard
	allowList := []string{"*"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://google.com", "Expected curl to google.com to succeed with '*' wildcard")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to cloudflare.com to succeed with '*' wildcard")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://github.com", "Expected curl to github.com to succeed with '*' wildcard")
}

// TestEgressFirewallDomainCaseInsensitive tests that domain matching is case-insensitive
func TestEgressFirewallDomainCaseInsensitive(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow domain with mixed case
	allowList := []string{"Google.COM"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://google.com", "Expected curl to google.com to succeed when Google.COM is allowed (case-insensitive)")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to non-allowed domain (cloudflare.com) to fail")
}

// TestEgressFirewallAllowDomainAndIP tests mixed domain and IP allowlist
func TestEgressFirewallAllowDomainAndIP(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow both a domain and an IP
	allowList := []string{"google.com", "1.1.1.1"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://google.com", "Expected curl to allowed domain (google.com) to succeed")
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to allowed IP (1.1.1.1) to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to non-allowed domain (cloudflare.com) to fail")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://8.8.4.4", "Expected curl to non-allowed IP (8.8.4.4) to fail")
}

// TestEgressFirewallHTTPSByIPNoHostname tests that HTTPS requests by IP (no SNI hostname)
// fall back to CIDR rules when domain filtering is configured
func TestEgressFirewallHTTPSByIPNoHostname(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow only a domain (not an IP)
	allowList := []string{"cloudflare.com"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to allowed domain (cloudflare.com) to succeed")
	// HTTPS request by IP (1.1.1.1 is Cloudflare's IP) is blocked because there's no hostname/SNI to match
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://1.1.1.1", "Expected curl to IP (https://1.1.1.1) to fail when only domain is allowed (no SNI hostname)")
}

// TestEgressFirewallDomainPersistsAfterResume tests that domain-based network config persists after pause/resume
func TestEgressFirewallDomainPersistsAfterResume(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	sbxTimeout := int32(60)
	client := setup.GetAPIClient()

	// Block all internet but allow specific domain
	allowList := []string{"google.com"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	// Test before pause
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://google.com", "Expected curl to allowed domain (google.com) to succeed before pause")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to non-allowed domain (cloudflare.com) to fail before pause")

	// Pause the sandbox
	respPause, err := client.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err, "Expected to pause sandbox without error")
	require.Equal(t, http.StatusNoContent, respPause.StatusCode(), "Expected status code 204 No Content, got %d", respPause.StatusCode())

	// Resume the sandbox
	respResume, err := client.PostSandboxesSandboxIDResumeWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{
		Timeout: &sbxTimeout,
	}, setup.WithAPIKey())
	require.NoError(t, err, "Expected to resume sandbox without error")
	require.Equal(t, http.StatusCreated, respResume.StatusCode(), "Expected status code 201 Created, got %d", respResume.StatusCode())

	// Test after resume
	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "https://google.com", "Expected curl to allowed domain (google.com) to succeed after resume")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "https://cloudflare.com", "Expected curl to non-allowed domain (cloudflare.com) to fail after resume")
}

// =============================================================================
// HTTP (non-HTTPS) protocol tests
// =============================================================================

// TestEgressFirewallHTTPDomainFiltering tests that HTTP (non-HTTPS) traffic is filtered by Host header
func TestEgressFirewallHTTPDomainFiltering(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow postman-echo.com domain (supports HTTP)
	allowList := []string{"postman-echo.com"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulHTTPRequest(t, ctx, sbx, envdClient, "http://postman-echo.com/get", "Expected HTTP curl to allowed domain (httpbin.org) to succeed")
	assertBlockedHTTPRequest(t, ctx, sbx, envdClient, "http://example.com", "Expected HTTP curl to non-allowed domain (example.com) to fail")
}

// =============================================================================
// UDP protocol tests (DNS queries to specific servers)
// =============================================================================

// TestEgressFirewallUDPAllowedIP tests that UDP traffic (DNS) to allowed IPs works
func TestEgressFirewallUDPAllowedIP(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow Google DNS IP
	allowList := []string{"8.8.8.8"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulDNSQuery(t, ctx, sbx, envdClient, "8.8.8.8", "google.com", "Expected DNS query (UDP) to allowed IP (8.8.8.8) to succeed")
	assertBlockedDNSQuery(t, ctx, sbx, envdClient, "1.1.1.1", "google.com", "Expected DNS query (UDP) to non-allowed IP (1.1.1.1) to fail")
}

// TestEgressFirewallUDPAllowedCIDR tests that UDP traffic to allowed CIDR range works
func TestEgressFirewallUDPAllowedCIDR(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Block all internet but allow Google DNS CIDR range
	allowList := []string{"8.8.8.0/24"}
	blockAll := []string{sandbox_network.AllInternetTrafficCIDR}

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowList,
			DenyOut:  &blockAll,
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	assertSuccessfulDNSQuery(t, ctx, sbx, envdClient, "8.8.8.8", "google.com", "Expected DNS query (UDP) to IP within allowed CIDR (8.8.8.0/24) to succeed")
	assertBlockedDNSQuery(t, ctx, sbx, envdClient, "1.1.1.1", "google.com", "Expected DNS query (UDP) to IP outside allowed CIDR to fail")
}

// TestEgressFirewallDNSSpoofingNeutralized tests that DNS spoofing attacks are neutralized.
// This simulates an attack where:
// 1. The firewall allows traffic to "google.com"
// 2. An attacker modifies /etc/hosts to make google.com resolve to 1.1.1.1 (Cloudflare's IP)
// 3. The attacker tries to access 1.1.1.1 claiming the hostname is "google.com"
// 4. The firewall IGNORES the spoofed IP and resolves google.com itself, redirecting to the real Google IP
// 5. The connection SUCCEEDS to the real Google server (not to 1.1.1.1)
func TestEgressFirewallDNSSpoofingNeutralized(t *testing.T) {
	t.Parallel()
	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			// Block all internet but allow google.com domain
			AllowOut: &[]string{"google.com"},
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	// First, verify normal access to google.com works and response is from Google (server: gws)
	assertHTTPResponseFromServer(t, ctx, sbx, envdClient, "https://google.com", "server: gws",
		"Expected curl to google.com to succeed and return Google server header before DNS spoofing")

	// Now simulate DNS spoofing by adding an /etc/hosts entry that maps google.com to 1.1.1.1 (Cloudflare's IP)
	// This simulates what would happen if an attacker modified DNS resolution
	// If the spoofing worked (i.e., if we connected to 1.1.1.1), we would see "server: cloudflare"
	err := utils.ExecCommandAsRoot(t, ctx, sbx, envdClient, "sh", "-c", "echo '1.1.1.1 google.com' >> /etc/hosts")
	require.NoError(t, err, "Expected to modify /etc/hosts without error")

	// Now try to access google.com - this should STILL SUCCEED and return Google's response because:
	// - The sandbox resolves google.com to 1.1.1.1 (via /etc/hosts)
	// - The firewall sees a connection to 1.1.1.1 with hostname "google.com"
	// - The firewall resolves google.com itself and gets the real Google IPs
	// - The firewall REDIRECTS the connection to a real Google IP (ignoring 1.1.1.1)
	// - The connection succeeds to the real Google server
	//
	// We verify this by checking the "server" header in the response:
	// - If we reached Google: "server: gws"
	// - If we reached Cloudflare (spoofing worked): "server: cloudflare"
	assertHTTPResponseFromServer(t, ctx, sbx, envdClient, "https://google.com", "server: gws",
		"Expected response from Google (server: gws), NOT Cloudflare - firewall should redirect to real Google IP")

	t.Log("SUCCESS: DNS spoofing attack neutralized")
	t.Log("  - google.com was allowed in the firewall rules")
	t.Log("  - /etc/hosts was modified to make google.com resolve to 1.1.1.1 (Cloudflare)")
	t.Log("  - Firewall resolved google.com itself and redirected to a real Google IP")
	t.Log("  - Response came from Google (server: gws), NOT Cloudflare - spoofing was bypassed!")
}

// TestNoNetworkConfig_SSHWorks tests that SSH connections work when no network config is set.
// This is a regression test for the issue where the TCP firewall redirect rule would
// break SSH connections even when no egress filtering was configured.
// Expected: SSH connection to GitHub should succeed (TCP handshake completes),
// though we'll get "Permission denied (publickey)" since we don't have valid credentials.
func TestNoNetworkConfig_SSHWorks(t *testing.T) {
	t.Parallel()

	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Create sandbox WITHOUT any network configuration
	// This tests the default behavior - all traffic should be allowed
	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		// No network config - this is the key part of the test
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	// Test SSH connection to GitHub
	// Expected output: "git@github.com: Permission denied (publickey)."
	// This shows the TCP connection succeeded (SSH handshake completed),
	// even though we don't have valid credentials for authentication.
	t.Log("Testing SSH connection to github.com (port 22)...")
	output, err := utils.ExecCommandWithOutput(t, ctx, sbx, envdClient, nil, "user",
		"ssh", "-T", "-o", "StrictHostKeyChecking=accept-new",
		"-o", "ConnectTimeout=5", "git@github.com")
	require.Error(t, err, "Expected SSH command to exit with non-zero status due to lack of credentials")
	require.Contains(t, output, "Permission denied (publickey)")
}

// TestWithNetworkConfig_SSHWorks tests that SSH connections work when network config IS defined.
// This tests that SSH traffic (which is non-HTTP/HTTPS) is correctly handled by the firewall
// when IP-based filtering is enabled. SSH doesn't use SNI or Host headers, so we must allow
// by IP address rather than domain name.
// Expected: SSH connection to GitHub should succeed (TCP handshake completes),
// though we'll get "Permission denied (publickey)" since we don't have valid credentials.
func TestWithNetworkConfig_SSHWorks(t *testing.T) {
	t.Parallel()

	templateID := ensureNetworkTestTemplate(t)
	ctx := t.Context()
	client := setup.GetAPIClient()

	// Create sandbox WITH network configuration that allows all IPs
	// SSH is a plain TCP protocol without hostname information (no SNI/Host header),
	// so domain-based filtering won't work for SSH - we need IP-based rules.
	// Using 0.0.0.0/0 in allowOut to allow all traffic, but with denyOut set to prove
	// network config is being processed (not just bypassed).
	sbx := utils.SetupSandboxWithCleanup(t, client,
		utils.WithTemplateID(templateID),
		utils.WithTimeout(60),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &[]string{sandbox_network.AllInternetTrafficCIDR}, // Allow all IPs
			DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR}, // Would block all, but allowOut takes precedence
		}),
	)

	envdClient := setup.GetEnvdClient(t, ctx)

	// Test SSH connection to GitHub
	// Expected output: "git@github.com: Permission denied (publickey)."
	// This shows the TCP connection succeeded (SSH handshake completed),
	// even though we don't have valid credentials for authentication.
	t.Log("Testing SSH connection to github.com (port 22) with network config defined...")
	output, err := utils.ExecCommandWithOutput(t, ctx, sbx, envdClient, nil, "user",
		"ssh", "-T", "-o", "StrictHostKeyChecking=accept-new",
		"-o", "ConnectTimeout=5", "git@github.com")
	require.Error(t, err, "Expected SSH command to exit with non-zero status due to lack of credentials")
	require.Contains(t, output, "Permission denied (publickey)",
		"Expected 'Permission denied (publickey)' indicating SSH handshake succeeded but auth failed")
}

// TestGPGKeyserverWorks tests that GPG keyserver connections work correctly.
// GPG keyservers use the HKP protocol (HTTP Keyserver Protocol) typically on port 11371.
// This test is important for verifying TCP half-close handling in the firewall proxy.
// GPG's HKP client may half-close the connection after sending the request (FIN from client),
// while still waiting for the server's response. If the proxy doesn't handle half-close correctly,
// the connection would freeze waiting indefinitely and the key retrieval would fail.
// Expected: GPG should successfully receive the key from the keyserver.
func TestGPGKeyserverWorks(t *testing.T) {
	t.Parallel()

	templateID := ensureNetworkTestTemplate(t)
	client := setup.GetAPIClient()

	t.Run("without network config", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		// Create sandbox WITHOUT any network configuration
		// This tests the default behavior - all traffic should be allowed
		sbx := utils.SetupSandboxWithCleanup(t, client,
			utils.WithTemplateID(templateID),
			utils.WithTimeout(60),
			// No network config - this is the key part of the test
		)

		envdClient := setup.GetEnvdClient(t, ctx)

		// Test GPG keyserver connection to Ubuntu's keyserver
		// This tests that:
		// 1. Non-standard TCP ports (11371) work correctly through the firewall
		// 2. TCP half-close is properly handled (GPG may FIN after request but expects response)
		t.Log("Testing GPG keyserver connection to hkp://keyserver.ubuntu.com...")
		gpgCtx, cancel := context.WithTimeout(ctx, gpgKeyserverTimeout)
		defer cancel()

		output, err := utils.ExecCommandWithOutput(t, gpgCtx, sbx, envdClient, nil, "user",
			"gpg", "--keyserver", "hkp://keyserver.ubuntu.com",
			"--recv-key", "95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7")
		require.NoError(t, err, "Expected GPG keyserver command to succeed, got error: %v, output: %s", err, output)
		t.Logf("GPG keyserver output: %s", output)
	})

	t.Run("with network config", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		// Create sandbox WITH network configuration that allows all IPs
		// Using 0.0.0.0/0 in allowOut to allow all traffic, but with denyOut set to prove
		// network config is being processed (not just bypassed).
		sbx := utils.SetupSandboxWithCleanup(t, client,
			utils.WithTemplateID(templateID),
			utils.WithTimeout(60),
			utils.WithNetwork(&api.SandboxNetworkConfig{
				AllowOut: &[]string{sandbox_network.AllInternetTrafficCIDR}, // Allow all IPs
				DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR}, // Would block all, but allowOut takes precedence
			}),
		)

		envdClient := setup.GetEnvdClient(t, ctx)

		// Test GPG keyserver connection to Ubuntu's keyserver
		// This tests that:
		// 1. Non-standard TCP ports (11371) work correctly through the firewall when network config is active
		// 2. TCP half-close is properly handled (GPG may FIN after request but expects response)
		t.Log("Testing GPG keyserver connection to hkp://keyserver.ubuntu.com with network config defined...")
		gpgCtx, cancel := context.WithTimeout(ctx, gpgKeyserverTimeout)
		defer cancel()

		output, err := utils.ExecCommandWithOutput(t, gpgCtx, sbx, envdClient, nil, "user",
			"gpg", "--keyserver", "hkp://keyserver.ubuntu.com",
			"--recv-key", "95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7")
		require.NoError(t, err, "Expected GPG keyserver command to succeed, got error: %v, output: %s", err, output)
		t.Logf("GPG keyserver output: %s", output)
	})
}
