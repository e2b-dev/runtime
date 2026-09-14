//go:build linux

package buildcontext

import (
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type BuildContext struct {
	BuilderConfig  cfg.BuilderConfig
	Config         config.TemplateConfig
	Template       storage.Paths
	UploadErrGroup *errgroup.Group
	EnvdVersion    string
	CacheScope     string
	Version        string
	Rootfs         RootfsOptions
}

// RootfsOptions are the per-build inputs to the baked rootfs files that come
// from outside the template's own configuration. They are set once, where the
// BuildContext is built, so that the base-layer cache key and the file
// renderer read the same value: a file rendered from one evaluation and keyed
// on another is how a cached layer goes stale.
type RootfsOptions struct {
	// EnvdMemoryProtection renders envd's memory protection, one fixed request
	// on every template, into envd.service and a system.slice drop-in (see
	// core/rootfs/files/system.slice.d-envd.conf.tpl for why the slice).
	EnvdMemoryProtection bool
}
