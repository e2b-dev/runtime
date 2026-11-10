package servicestatus

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

// FileName is the record's name under the orchestrator's base directory.
const FileName = "service-status.json"

// Store keeps a Standby across processes.
type Store interface {
	// Load returns the record and whether there is one.
	Load() (Snapshot, bool, error)
	Save(snap Snapshot) error
	// Clear removes the record; a missing one is fine.
	Clear() error
}

// NopStore keeps nothing.
type NopStore struct{}

func (NopStore) Load() (Snapshot, bool, error) { return Snapshot{}, false, nil }
func (NopStore) Save(Snapshot) error           { return nil }
func (NopStore) Clear() error                  { return nil }

type fileStore struct {
	path string
}

// NewFileStore keeps the record as JSON at path, replaced atomically.
func NewFileStore(path string) Store {
	return fileStore{path: path}
}

type fileRecord struct {
	Status    string    `json:"status"`
	Owner     string    `json:"owner"`
	ChangedAt time.Time `json:"changed_at"`
}

func (s fileStore) Load() (Snapshot, bool, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}

	var r fileRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return Snapshot{}, false, fmt.Errorf("decoding %s: %w", s.path, err)
	}
	status, ok := orchestratorinfo.ServiceInfoStatus_value[r.Status]
	if !ok {
		return Snapshot{}, false, fmt.Errorf("decoding %s: unknown status %q", s.path, r.Status)
	}

	return Snapshot{Status: Status(status), Owner: r.Owner, ChangedAt: r.ChangedAt}, true, nil
}

func (s fileStore) Save(snap Snapshot) error {
	data, err := json.Marshal(fileRecord{Status: snap.Status.String(), Owner: snap.Owner, ChangedAt: snap.ChangedAt})
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.path), FileName+".*")
	if err != nil {
		return err
	}
	// Gone once renamed, so only a failed write leaves work for this.
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()

		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()

		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return err
	}

	return syncDir(filepath.Dir(s.path))
}

func (s fileStore) Clear() error {
	err := os.Remove(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	return syncDir(filepath.Dir(s.path))
}

// syncDir makes a rename or a remove in dir survive a power loss.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()

		return err
	}

	return d.Close()
}
