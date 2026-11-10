package mtls

import (
	"os"
	"strings"
)

// EnvPrefix is the prefix of the variables a chart renders for a listener
// or client hop key: E2B_TLS_, the key upper-cased with '-' as '_', then '_'.
func EnvPrefix(key string) string {
	return "E2B_TLS_" + strings.ToUpper(strings.ReplaceAll(key, "-", "_")) + "_"
}

// ListenerSettings are one listener's settings as a chart renders them
// under EnvPrefix(key). A configuration read with github.com/caarlos0/env
// holds a field of this type under that prefix; ListenerSettingsFromEnv
// reads the same variables directly. Process.Listener parses them.
type ListenerSettings struct {
	// Mode is off, permissive or required; blank is off.
	Mode string `env:"MODE"`
	// Allow is the allow-list, one SPIFFE ID per entry.
	Allow []string `env:"ALLOW"`
	// HealthPaths are the exact paths a plaintext request may reach under required.
	HealthPaths []string `env:"HEALTH_PATHS"`
}

// HopSettings are one client hop's settings as a chart renders them under
// EnvPrefix(key); see ListenerSettings. Process.Hop parses them.
type HopSettings struct {
	// Mode is off or on; blank is off.
	Mode string `env:"CLIENT_MODE"`
	// Expect are the server's acceptable SPIFFE IDs.
	Expect []string `env:"EXPECT"`
	// ServerName is the DNS name checked against the server's certificate,
	// empty for a server dialled by IP.
	ServerName string `env:"SERVER_NAME"`
}

// ListenerSettingsFromEnv reads the variables the chart renders for the
// listener key. An unset variable reads as empty.
func ListenerSettingsFromEnv(key string) ListenerSettings {
	prefix := EnvPrefix(key)

	return ListenerSettings{
		Mode:        os.Getenv(prefix + "MODE"),
		Allow:       splitList(os.Getenv(prefix + "ALLOW")),
		HealthPaths: splitList(os.Getenv(prefix + "HEALTH_PATHS")),
	}
}

// HopSettingsFromEnv reads the variables the chart renders for the client
// hop key. An unset variable reads as empty.
func HopSettingsFromEnv(key string) HopSettings {
	prefix := EnvPrefix(key)

	return HopSettings{
		Mode:       os.Getenv(prefix + "CLIENT_MODE"),
		Expect:     splitList(os.Getenv(prefix + "EXPECT")),
		ServerName: strings.TrimSpace(os.Getenv(prefix + "SERVER_NAME")),
	}
}

// splitList splits a comma-joined value, trimming each entry and skipping
// blanks, as the chart joins a list.
func splitList(value string) []string {
	var list []string
	for entry := range strings.SplitSeq(value, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			list = append(list, entry)
		}
	}

	return list
}
