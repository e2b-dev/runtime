package identity

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	ory "github.com/ory/client-go"
)

func TestIdentityFromOryProfileFields(t *testing.T) {
	t.Parallel()

	userID := uuid.New()

	tests := []struct {
		name           string
		traits         any
		metadataPublic map[string]any
		credentials    *map[string]ory.IdentityCredentials
		wantName       string
		wantEmail      string
		wantPicture    string
		wantProviders  []string
	}{
		{
			name: "email and name from traits, picture from metadata_public",
			traits: map[string]any{
				"email":   "ada@example.com",
				"name":    "ada lovelace",
				"picture": "https://example.com/trait-picture.jpg",
			},
			metadataPublic: map[string]any{
				"picture": "https://example.com/ada.jpg",
			},
			wantName:    "ada lovelace",
			wantEmail:   "ada@example.com",
			wantPicture: "https://example.com/ada.jpg",
		},
		{
			name: "providers from credentials and oidc config",
			traits: map[string]any{
				"email": "grace@example.com",
				"name":  "grace hopper",
			},
			credentials: &map[string]ory.IdentityCredentials{
				"password": {
					Config: map[string]any{"hashed_password": "$argon2id$v=19$m=65536,t=3,p=4$hash"},
				},
				"oidc": {
					Config: map[string]any{
						"providers": []any{
							map[string]any{"provider": " Google-project "},
							map[string]any{"provider": "github-main"},
							map[string]any{"provider": "apple"},
						},
					},
					Identifiers: []string{"google:111", "github:222"},
				},
			},
			wantName:      "grace hopper",
			wantEmail:     "grace@example.com",
			wantProviders: []string{"email", "google", "github"},
		},
		{
			name: "providers fallback from identifiers when config missing",
			credentials: &map[string]ory.IdentityCredentials{
				"password": {
					Config: map[string]any{"use_password_migration_hook": true},
				},
				"oidc": {
					Identifiers: []string{"github-gQUWnTuT:50748440", "GOOGLE-abC:111", "google:111", "apple:333"},
				},
			},
			wantProviders: []string{"email", "google", "github"},
		},
		{
			name: "empty password credential ignored",
			credentials: &map[string]ory.IdentityCredentials{
				"password": {Identifiers: []string{"ada@example.com"}},
			},
		},
		{
			name: "email provider from usable password credential",
			credentials: &map[string]ory.IdentityCredentials{
				"password": {
					Config: map[string]any{"hashed_password": "$argon2id$v=19$m=65536,t=3,p=4$hash"},
				},
			},
			wantProviders: []string{"email"},
		},
		{
			name:   "nil traits and credentials returns zero values",
			traits: nil,
		},
		{
			name: "non-string values are ignored",
			traits: map[string]any{
				"email": 42,
				"name":  map[string]any{"first": "barbara"},
			},
			metadataPublic: map[string]any{
				"picture": 42,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			oryIdentity := ory.Identity{Id: uuid.NewString(), Traits: tt.traits, MetadataPublic: tt.metadataPublic, Credentials: tt.credentials}

			got := ProfileFromIdentity(userID, identityFromOry(oryIdentity))
			if got.UserID != userID {
				t.Fatalf("UserID = %s, want %s", got.UserID, userID)
			}
			if got.Name != tt.wantName {
				t.Fatalf("Name = %q, want %q", got.Name, tt.wantName)
			}
			if got.Email != tt.wantEmail {
				t.Fatalf("Email = %q, want %q", got.Email, tt.wantEmail)
			}
			if got.ProfilePictureURL != tt.wantPicture {
				t.Fatalf("ProfilePictureURL = %q, want %q", got.ProfilePictureURL, tt.wantPicture)
			}
			if !reflect.DeepEqual(got.Providers, tt.wantProviders) {
				t.Fatalf("Providers = %v, want %v", got.Providers, tt.wantProviders)
			}
		})
	}
}
