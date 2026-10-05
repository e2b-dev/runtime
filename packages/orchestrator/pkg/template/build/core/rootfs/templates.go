//go:build linux

package rootfs

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"text/template"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

// generateFile renders one template. A template that renders text must have
// set at least one path: a body with nowhere to go is a removed or misplaced
// WriteFile, and failing the build here is what stops it installing nothing in
// silence. Rendering no text and setting no path is a template that opted out
// of this build, and is skipped.
func generateFile(t *template.Template, model *templateModel) ([]byte, error) {
	var buff bytes.Buffer
	if err := t.Execute(&buff, &model); err != nil {
		return nil, fmt.Errorf("error executing template %q: %w", t.Name(), err)
	}

	data := bytes.TrimSpace(buff.Bytes())
	if len(model.paths) == 0 {
		if len(data) == 0 {
			return nil, nil
		}

		return nil, fmt.Errorf("template %q did not set path", t.Name())
	}

	return data, nil
}

type templateModel struct {
	Context buildcontext.BuildContext

	Hostname            string
	ProvisionLogPrefix  string
	ProvisionResultPath string
	ProvisionExitPrefix string
	Nameserver          string

	paths []struct {
		path string
		mode int64
	}
}

func newTemplateModel(buildContext buildcontext.BuildContext, provisionLogPrefix, provisionResultPath string) *templateModel {
	return &templateModel{
		Context:             buildContext,
		Hostname:            "e2b.local",
		ProvisionLogPrefix:  provisionLogPrefix,
		ProvisionExitPrefix: ProvisioningExitPrefix,
		ProvisionResultPath: provisionResultPath,
		Nameserver:          sandbox_network.DefaultNameserver,
	}
}

func (t *templateModel) MemoryLimit() int {
	return int(math.Min(float64(t.Context.Config.MemoryMB)/2, 512))
}

// envd's explicit memory protection when the option is on, in MiB, rendered
// into envd.service and requested on system.slice: the same two values on
// every template whatever its RAM, since envd's footprint under a reclaim
// storm follows its own load and not the guest's size. The hard request covers
// envd's text and the heap it grows under the storm; memory.min protects only
// pages the cgroup has actually charged, so a request above envd's use costs
// the guest nothing until envd uses it. The low value is a best-effort band
// above it.
const (
	EnvdMemoryMinMiB int64 = 128
	EnvdMemoryLowMiB int64 = 256
)

func (t *templateModel) EnvdMemoryProtection() bool {
	return t.Context.Rootfs.EnvdMemoryProtection
}

func (t *templateModel) EnvdMemoryMinMiB() int64 {
	return EnvdMemoryMinMiB
}

func (t *templateModel) EnvdMemoryLowMiB() int64 {
	return EnvdMemoryLowMiB
}

func (t *templateModel) WriteFile(path string, mode int64) string {
	t.paths = append(t.paths, struct {
		path string
		mode int64
	}{
		path: strings.TrimPrefix(path, "/"),
		mode: mode,
	})

	return "" // no real return value
}
