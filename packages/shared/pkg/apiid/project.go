package apiid

import (
	"encoding/json"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

//nolint:recvcheck // Decoding mutates pointers; encoding must also work on non-addressable values.
type ProjectID struct {
	UUID     uuid.UUID
	PublicID string
}

func NewProjectID(value uuid.UUID) ProjectID {
	return ProjectID{UUID: value, PublicID: id.ProjectID(value).String()}
}

func (value *ProjectID) UnmarshalText(text []byte) error {
	parsed, err := uuid.Parse(string(text))
	if err != nil {
		public, parseErr := id.ParseProjectID(string(text))
		if parseErr != nil {
			return parseErr
		}
		parsed = uuid.UUID(public)
	}
	*value = NewProjectID(parsed)

	return nil
}

func (value *ProjectID) Bind(text string) error {
	return value.UnmarshalText([]byte(text))
}

func (value *ProjectID) UnmarshalParam(text string) error {
	return value.UnmarshalText([]byte(text))
}

func (value ProjectID) MarshalText() ([]byte, error) {
	return []byte(id.ProjectID(value.UUID).String()), nil
}

func (value *ProjectID) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}

	return value.UnmarshalText([]byte(text))
}

func (value ProjectID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.ProjectID(value.UUID).String())
}
