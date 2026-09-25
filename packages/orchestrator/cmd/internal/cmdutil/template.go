package cmdutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"
)

const nilUUID = "00000000-0000-0000-0000-000000000000"

// templateInfo represents a template from the E2B API.
type templateInfo struct {
	TemplateID string   `json:"templateID"`
	BuildID    string   `json:"buildID"`
	Aliases    []string `json:"aliases"`
	Names      []string `json:"names"`
}

// ResolveTemplateID fetches the build ID for a template from the E2B API.
// Input can be a template ID, alias, or full name (e.g. "e2b/base").
//
// API endpoint resolution order:
//  1. E2B_API_URL  — full base URL, e.g. http://localhost:3000 (self-hosted / local dev)
//  2. E2B_DOMAIN   — domain suffix, produces https://api.<domain>
//  3. default      — https://api.e2b.dev
func ResolveTemplateID(input string) (string, error) {
	apiKey := os.Getenv("E2B_API_KEY")
	if apiKey == "" {
		return "", errors.New("E2B_API_KEY environment variable required for -template flag")
	}

	apiURL := resolveAPIURL() + "/templates"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-API-Key", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch templates: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return "", fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	var templates []templateInfo
	if err := json.NewDecoder(resp.Body).Decode(&templates); err != nil {
		return "", fmt.Errorf("failed to parse API response: %w", err)
	}

	var match *templateInfo
	var availableAliases []string
	for i := range templates {
		t := &templates[i]
		availableAliases = append(availableAliases, t.Aliases...)

		if t.TemplateID == input || slices.Contains(t.Aliases, input) || slices.Contains(t.Names, input) {
			match = t

			break
		}
	}

	if match == nil {
		return "", fmt.Errorf("template %q not found. Available aliases: %s", input, strings.Join(availableAliases, ", "))
	}
	if match.BuildID == "" || match.BuildID == nilUUID {
		return "", fmt.Errorf("template %q has no successful build", input)
	}

	return match.BuildID, nil
}

// resolveAPIURL returns the E2B API base URL (no trailing slash).
// Priority: E2B_API_URL > E2B_DOMAIN > default (api.e2b.dev).
func resolveAPIURL() string {
	// E2B_API_URL accepts a full base URL — useful for self-hosted deployments
	// and local dev where the API does not sit behind a domain (e.g. http://localhost:3000).
	if u := strings.TrimRight(os.Getenv("E2B_API_URL"), "/"); u != "" {
		return u
	}
	if domain := os.Getenv("E2B_DOMAIN"); domain != "" {
		return fmt.Sprintf("https://api.%s", domain)
	}
	return "https://api.e2b.dev"
}
