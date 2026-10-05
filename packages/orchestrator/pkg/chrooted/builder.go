//go:build linux

package chrooted

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
)

var ErrVolumeTypeNotFound = errors.New("volume type not found")

type Builder struct {
	config cfg.Config
}

func NewBuilder(config cfg.Config) *Builder {
	return &Builder{config: config}
}

func (b *Builder) Chroot(volumeType string, teamID, volumeID uuid.UUID) (*Chrooted, error) {
	fullPath, err := b.BuildVolumePath(volumeType, teamID, volumeID)
	if err != nil {
		return nil, err
	}

	fs, err := Chroot(fullPath, WithMetadata("volume-id", volumeID.String()))
	if err != nil {
		return nil, err
	}

	return fs, nil
}

func (b *Builder) BuildVolumePath(volumeType string, teamID, volumeID uuid.UUID) (string, error) {
	volumeTypeRoot, ok := b.config.PersistentVolumeMounts[volumeType]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrVolumeTypeNotFound, volumeType)
	}

	return filepath.Join(
		volumeTypeRoot,
		fmt.Sprintf("team-%s", teamID),
		fmt.Sprintf("vol-%s", volumeID),
	), nil
}
