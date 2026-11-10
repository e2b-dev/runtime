package utils

import (
	"testing"
)

func TestIsGTEVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		curVersion  string
		minVersion  string
		expected    bool
		expectError bool
	}{
		// Valid version comparisons
		{
			name:        "current version greater than minimum",
			curVersion:  "1.2.3",
			minVersion:  "1.2.0",
			expected:    true,
			expectError: false,
		},
		{
			name:        "current version equal to minimum",
			curVersion:  "1.2.3",
			minVersion:  "1.2.3",
			expected:    true,
			expectError: false,
		},
		{
			name:        "current version less than minimum",
			curVersion:  "1.2.0",
			minVersion:  "1.2.3",
			expected:    false,
			expectError: false,
		},
		{
			name:        "versions with v prefix",
			curVersion:  "v1.2.3",
			minVersion:  "v1.2.0",
			expected:    true,
			expectError: false,
		},
		{
			name:        "mixed v prefix",
			curVersion:  "v1.2.3",
			minVersion:  "1.2.0",
			expected:    true,
			expectError: false,
		},
		{
			name:        "mixed v prefix reverse",
			curVersion:  "1.2.3",
			minVersion:  "v1.2.0",
			expected:    true,
			expectError: false,
		},
		{
			name:        "pre-release versions",
			curVersion:  "1.2.3-alpha",
			minVersion:  "1.2.3",
			expected:    false,
			expectError: false,
		},
		{
			name:        "pre-release comparison",
			curVersion:  "1.2.3-beta",
			minVersion:  "1.2.3-alpha",
			expected:    true,
			expectError: false,
		},
		{
			name:        "build metadata",
			curVersion:  "1.2.3+build.1",
			minVersion:  "1.2.3",
			expected:    true,
			expectError: false,
		},
		{
			name:        "zero versions",
			curVersion:  "0.0.0",
			minVersion:  "0.0.0",
			expected:    true,
			expectError: false,
		},
		{
			name:        "current version missing patch",
			curVersion:  "1.2",
			minVersion:  "1.0.0",
			expected:    true,
			expectError: false,
		},
		{
			name:        "current version missing minor",
			curVersion:  "1",
			minVersion:  "1.0.0",
			expected:    true,
			expectError: false,
		},
		{
			name:        "minimum version missing minor",
			curVersion:  "1.0.0",
			minVersion:  "2",
			expected:    false,
			expectError: false,
		},
		{
			name:        "empty current version",
			curVersion:  "",
			minVersion:  "1.2.0",
			expected:    false,
			expectError: true,
		},
		{
			name:        "empty minimum version",
			curVersion:  "1.2.0",
			minVersion:  "",
			expected:    false,
			expectError: true,
		},
		{
			name:        "both versions empty",
			curVersion:  "",
			minVersion:  "",
			expected:    false,
			expectError: true,
		},
		{
			name:        "invalid format with extra dots",
			curVersion:  "1.2.3.4",
			minVersion:  "1.2.0",
			expected:    false,
			expectError: true,
		},
		{
			name:        "invalid format with spaces",
			curVersion:  "1.2.3",
			minVersion:  "1.2 .0",
			expected:    false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := IsGTEVersion(tt.curVersion, tt.minVersion)

			if tt.expectError {
				if err == nil {
					t.Errorf("IsGTEVersion() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("IsGTEVersion() unexpected error: %v", err)
				}
			}

			if result != tt.expected {
				t.Errorf("IsGTEVersion() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsGTEVersionWithTimestamp(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		curVersion string
		minVersion string
		expected   bool
	}{
		{
			name:       "releases compare as semver",
			curVersion: "0.10.2",
			minVersion: "0.10.1",
			expected:   true,
		},
		{
			name:       "build against an earlier build of its line",
			curVersion: "0.10.202610231415-def456",
			minVersion: "0.10.202610230958-abc123",
			expected:   true,
		},
		{
			name:       "build against a later build of its line",
			curVersion: "0.10.202610230740-abc123",
			minVersion: "0.10.202610230958-abc123",
			expected:   false,
		},
		{
			name:       "build with a build-metadata suffix against the same build",
			curVersion: "0.10.202610230958+abc123",
			minVersion: "0.10.202610230958-abc123",
			expected:   true,
		},
		{
			name:       "build against its line's first release",
			curVersion: "0.10.202610221200-abc123",
			minVersion: "0.10.0",
			expected:   true,
		},
		{
			name:       "build against a prerelease of its line's first release",
			curVersion: "0.10.202610221200-abc123",
			minVersion: "0.10.0-rc.1",
			expected:   true,
		},
		{
			name:       "build against a later release of its line",
			curVersion: "0.10.202610221200-abc123",
			minVersion: "0.10.2",
			expected:   false,
		},
		{
			name:       "build against a release of an earlier line",
			curVersion: "0.10.202610221200-abc123",
			minVersion: "0.9.7",
			expected:   true,
		},
		{
			name:       "build against a release of a later line",
			curVersion: "0.10.202610221200-abc123",
			minVersion: "0.11.0",
			expected:   false,
		},
		{
			name:       "release against a build of its line",
			curVersion: "0.10.2",
			minVersion: "0.10.202610221200-abc123",
			expected:   false,
		},
		{
			name:       "release against a build of an earlier line",
			curVersion: "0.11.0",
			minVersion: "0.10.202610221200-abc123",
			expected:   true,
		},
		{
			name:       "release against a build of a later line",
			curVersion: "0.10.9",
			minVersion: "0.11.202610221200-abc123",
			expected:   false,
		},
		{
			name:       "thirteen-digit patch is a release",
			curVersion: "0.10.2026102212001",
			minVersion: "0.10.2",
			expected:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := IsGTEVersionWithTimestamp(tt.curVersion, tt.minVersion)
			if err != nil {
				t.Fatalf("IsGTEVersionWithTimestamp() unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("IsGTEVersionWithTimestamp() = %v, want %v", result, tt.expected)
			}
		})
	}

	if _, err := IsGTEVersionWithTimestamp("", "0.10.2"); err == nil {
		t.Error("IsGTEVersionWithTimestamp() expected error for an empty version")
	}
}
