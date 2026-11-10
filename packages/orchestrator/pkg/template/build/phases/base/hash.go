//go:build linux

package base

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/base/distro"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (bb *BaseBuilder) Hash(ctx context.Context, _ phases.LayerResult) (string, error) {
	ctx, span := tracer.Start(ctx, "hash base")
	defer span.End()

	var baseSource string
	if bb.Config.FromTemplate != nil {
		// When building from template, use the base template metadata
		baseSource = fmt.Sprintf("template:%s", bb.Config.FromTemplate.GetBuildID())
	} else {
		// Note: When "default" tag is used, the cached version might become ambiguous (not always default)
		// To update it now, you need to force the rebuild of the template, which will update this layer for all templates
		// in the team. This is okay for now, as the cache is not shared between teams, but it might need to be changed
		// when global caches are implemented.

		// When building from image, use the image name
		baseSource = bb.Config.FromImage
	}

	// For fallback/dev environments, include baked rootfs file contents and
	// the distro provisioning contract (profiles + init blocks — the rendered
	// selector is part of the script but not of the raw provisionScriptFile
	// hashed here) in the provision version. In production,
	// BuildProvisionVersion controls rollout invalidation explicitly.
	provisionVersion := cache.HashKeys(provisionScriptFile, rootfs.FilesHash(), distro.Fingerprint())
	if val := bb.featureFlags.IntFlag(
		ctx,
		featureflags.BuildProvisionVersion,
		featureflags.TemplateContext(bb.Config.TemplateID),
		featureflags.TeamContext(bb.Config.TeamID),
		// at the fallback value, keep the provisioning-contract hash above
	); val != featureflags.BuildProvisionVersion.Fallback() {
		provisionVersion = strconv.FormatInt(int64(val), 10)
	}

	attrs := []attribute.KeyValue{
		attribute.String("index_version", bb.index.Version()),
		attribute.String("provision_version", provisionVersion),
		attribute.String("base_source", baseSource),
		attribute.Int64("disk_size_mb", bb.Config.DiskSizeMB),
	}
	if rendersRootfsFiles(bb.BuildContext) {
		// The memory.min this build's base-layer files can obtain on a systemd
		// chain: the constant when the option is on, 0 when off, since the
		// legacy request the unit then carries is prorated to nothing by the
		// slice above it. The key covers the request, so this describes the
		// served files whether they were rendered here or taken from the cache.
		// Absent for a build from another template, whose files are the
		// parent's.
		//
		// It is a property of the rendered files, not a reading from any guest:
		// an init that deletes the baked systemd files (the premade NixOS
		// image) or never reads them (OpenRC) gains nothing from the request
		// reported here, and such a template still rebuilds once when the
		// option flips, for no runtime effect.
		attrs = append(attrs, attribute.Int64("envd_memory_min_mib", envdMemoryMinMiB(bb.BuildContext)))
	}
	telemetry.SetAttributes(ctx, attrs...)

	return baseLayerKey(bb.index.Version(), provisionVersion, baseSource, bb.BuildContext), nil
}

// rendersRootfsFiles reports whether this build bakes the rootfs files at all.
// A build from another template takes the parent's base layer as it is (see
// Layer's FromTemplate arm) and renders nothing, so a key contribution
// describing files it does not render would rotate every derived layer for no
// change in bytes.
func rendersRootfsFiles(buildContext buildcontext.BuildContext) bool {
	return buildContext.Config.FromTemplate == nil
}

// envdMemoryMinMiB is the memory.min the base layer's files can obtain on a
// systemd chain: the constant when the option is on, 0 when off, since the
// legacy 50 MiB the unit then carries is prorated to nothing by its slice.
func envdMemoryMinMiB(buildContext buildcontext.BuildContext) int64 {
	if !buildContext.Rootfs.EnvdMemoryProtection {
		return 0
	}

	return rootfs.EnvdMemoryMinMiB
}

// envdMemoryProtectionKey prefixes the base-layer key's contribution when
// envd's memory protection is rendered, and is appended only then, for the
// same reason the cmdline contribution is: turning the option off again must
// return a team to the key its existing layers are stored under.
const envdMemoryProtectionKey = "envd-memory-protection"

// baseLayerKey assembles the base layer's cache key from inputs that are
// already resolved. It is pure so that the properties the key must have — the
// same digest as before for a build that opts into nothing, a distinct one for
// each contribution the key takes — are pinned by tests that need neither an
// index nor a feature-flag client.
func baseLayerKey(indexVersion, provisionVersion, baseSource string, buildContext buildcontext.BuildContext) string {
	keys := []string{
		provisionVersion,
		strconv.FormatInt(buildContext.Config.DiskSizeMB, 10),
		baseSource,
	}

	// Only when there are arguments, and this is the whole reason the append is
	// conditional: HashKeys writes a separator before every key, so contributing an
	// empty string would change the digest of every cached base layer for every team,
	// including the ones that never touched this flag. Appending only when a team is
	// targeted keeps their keys byte-identical, and removes a team's variant by
	// returning them to the hash their existing layers are already stored under.
	//
	// Keyed on the arguments rather than the variant's name: the name is a label, two
	// names can carry the same arguments, and it is the arguments a build step can
	// observe. Sorted, because map iteration order is not stable and a cache key that
	// varies run to run caches nothing.
	if len(buildContext.Config.CmdlineArgs) > 0 {
		// KernelArgs.String() renders sorted, which is what makes this a stable key: Go
		// randomises map iteration, and a cache key that varies run to run caches nothing.
		keys = append(keys, "cmdline:"+fc.KernelArgs(buildContext.Config.CmdlineArgs).String())
	}

	// Contributes only when set, so builds without a template keep their cached layers.
	// Keyed on the parsed settings, so two spellings of the flag share a key.
	if buildContext.Config.CPUTemplate != nil && !buildContext.Config.CPUTemplate.IsEmpty() {
		keys = append(keys, "cpu-template:"+buildContext.Config.CPUTemplate.String())
	}

	// The rendered files differ between on and off while the template file set,
	// and so FilesHash, does not, so the values themselves are the contribution.
	// Made only when this build renders the files with the option on: a
	// from-template build inherits its parent's files whatever its own flag
	// says, and off must stay the key every existing layer is under — which it
	// is wherever BuildProvisionVersion is set explicitly. Where that flag is at
	// its fallback, provisionVersion above is a hash of the baked files, so
	// adding the drop-in template rotates every base-layer key once regardless
	// of this contribution.
	if rendersRootfsFiles(buildContext) && buildContext.Rootfs.EnvdMemoryProtection {
		keys = append(keys, fmt.Sprintf("%s:%d:%d", envdMemoryProtectionKey, rootfs.EnvdMemoryMinMiB, rootfs.EnvdMemoryLowMiB))
	}

	return cache.HashKeys(indexVersion, keys...)
}
