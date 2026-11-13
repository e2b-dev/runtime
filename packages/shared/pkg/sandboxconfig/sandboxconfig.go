package sandboxconfig

type SandboxNetworkTransform struct {
	Headers map[string]string `json:"headers,omitempty"`
}

type SandboxNetworkRule struct {
	Transform *SandboxNetworkTransform `json:"transform,omitempty"`
}

type SandboxNetworkEgressConfig struct {
	AllowedAddresses []string                        `json:"allowedAddresses,omitempty"`
	DeniedAddresses  []string                        `json:"deniedAddresses,omitempty"`
	Rules            map[string][]SandboxNetworkRule `json:"rules,omitempty"`

	// DnsServers lists the DNS servers allowed when AllowedAddresses has a
	// domain. Nil means the default nameserver; empty means none.
	DnsServers *[]string `json:"dnsServers,omitempty"`

	// SOCKS5 BYOP egress proxy configuration.
	EgressProxyAddress  string                       `json:"egressProxyAddress,omitempty"`
	EgressProxyUsername string                       `json:"egressProxyUsername,omitempty"`
	EgressProxyPassword string                       `json:"egressProxyPassword,omitempty"`
	EgressProxyTLS      *SandboxEgressProxyTLSConfig `json:"egressProxyTls,omitempty"`
}

// SandboxEgressProxyTLSConfig configures TLS on the hop to the BYOP SOCKS5
// proxy. CACert is a certificate, not a secret, so it is stored inline.
type SandboxEgressProxyTLSConfig struct {
	Enabled    bool   `json:"enabled"`
	ServerName string `json:"serverName,omitempty"`
	CACert     string `json:"caCert,omitempty"`
}

type SandboxNetworkIngressConfig struct {
	AllowPublicAccess *bool    `json:"allowPublicAccess,omitempty"`
	MaskRequestHost   *string  `json:"maskRequestHost,omitempty"`
	HTTPSPorts        []uint32 `json:"httpsPorts,omitempty"`
}

type SandboxNetworkConfig struct {
	Egress  *SandboxNetworkEgressConfig  `json:"egress,omitempty"`
	Ingress *SandboxNetworkIngressConfig `json:"ingress,omitempty"`
}

// HasEgressProxy reports whether a customer-supplied SOCKS5 egress proxy is
// configured. The address is the single source of truth: credentials are
// optional and are never set without it.
func (c *SandboxNetworkConfig) HasEgressProxy() bool {
	return c != nil && c.Egress != nil && c.Egress.EgressProxyAddress != ""
}

// HasEgressProxyAuth reports whether the egress proxy is configured with
// RFC 1929 credentials.
func (c *SandboxNetworkConfig) HasEgressProxyAuth() bool {
	return c.HasEgressProxy() && c.Egress.EgressProxyUsername != ""
}

// HasEgressProxyTLS reports whether the hop to the egress proxy is wrapped in
// TLS.
func (c *SandboxNetworkConfig) HasEgressProxyTLS() bool {
	return c.HasEgressProxy() && c.Egress.EgressProxyTLS != nil && c.Egress.EgressProxyTLS.Enabled
}

type SandboxVolumeMountConfig struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
	Path string `json:"path"`
}

type SandboxAutoResumePolicy string

const (
	SandboxAutoResumeAny SandboxAutoResumePolicy = "any"
	SandboxAutoResumeOff SandboxAutoResumePolicy = "off"
)

type SandboxAutoResumeConfig struct {
	Policy  SandboxAutoResumePolicy `json:"policy"`
	Timeout uint64                  `json:"timeout,omitempty"`
}

// SandboxIam is the sandbox workload identity configuration carried with the
// sandbox. A nil value means workload identity is disabled.
type SandboxIam struct {
	// Tokens holds the named workload-token definitions, keyed by token name.
	Tokens map[string]SandboxIamToken `json:"tokens,omitempty"`
}

// SandboxIamToken is a named workload-token definition. filePath is
// intentionally not stored: only absent/null is accepted at admission, so there
// is nothing to persist.
type SandboxIamToken struct {
	Audience  string `json:"audience"`
	TokenType string `json:"tokenType"`
}
