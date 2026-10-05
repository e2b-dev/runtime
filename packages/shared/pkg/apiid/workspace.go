package apiid

import (
	"encoding/json"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

//nolint:recvcheck // Decoding mutates pointers; encoding must also work on non-addressable values.
type WorkspaceID struct {
	UUID     uuid.UUID
	PublicID string
}

func NewWorkspaceID(value uuid.UUID) WorkspaceID {
	return WorkspaceID{UUID: value, PublicID: id.WorkspaceID(value).String()}
}

func (value *WorkspaceID) UnmarshalText(text []byte) error {
	parsed, err := uuid.Parse(string(text))
	if err != nil {
		public, parseErr := id.ParseWorkspaceID(string(text))
		if parseErr != nil {
			return parseErr
		}
		parsed = uuid.UUID(public)
	}
	*value = NewWorkspaceID(parsed)

	return nil
}

func (value *WorkspaceID) Bind(text string) error {
	return value.UnmarshalText([]byte(text))
}

func (value *WorkspaceID) UnmarshalParam(text string) error {
	return value.UnmarshalText([]byte(text))
}

func (value WorkspaceID) MarshalText() ([]byte, error) {
	return []byte(id.WorkspaceID(value.UUID).String()), nil
}

func (value *WorkspaceID) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}

	return value.UnmarshalText([]byte(text))
}

func (value WorkspaceID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.WorkspaceID(value.UUID).String())
}
