//go:build linux

// export-build exports a template rootfs from E2B object storage as a
// Docker-loadable OCI tar archive, or pushes it directly to a container
// registry.
//
// # Quick start — env-var style (avoids long flag strings on every invocation)
//
// Set these once (e.g. in ~/.bashrc or sourced from the Nomad job env):
//
//	export TEMPLATE_STORAGE_URL="s3://dev-template?endpoint=https://tos-s3-cn-shanghai.ivolces.com&region=cn-shanghai"
//	export AWS_ACCESS_KEY_ID=...
//	export AWS_SECRET_ACCESS_KEY=...
//	export AWS_REGION=cn-shanghai
//	export E2B_API_KEY=e2b_...           # required when using -template
//	export E2B_API_URL=http://localhost:3000  # self-hosted / local dev; defaults to https://api.e2b.dev
//
// Then export a template by alias:
//
//	sudo -E export-build -template my-alias -output /tmp/rootfs.tar
//	sudo -E export-build -template my-alias -push registry.example.com/img:v1
//
// # Explicit flag style (no env vars needed)
//
//	sudo -E export-build \
//	  -build 4f9a9809-5a78-4cad-a56a-c87b06a6facb \
//	  -storage "s3://dev-template?endpoint=https://tos-s3-cn-shanghai.ivolces.com&region=cn-shanghai" \
//	  -output /tmp/rootfs.tar \
//	  -tag my-alias:latest
//
// # Prerequisites
//
//	modprobe nbd max_part=8   # load NBD kernel module before first run
//	                          # (already loaded on orchestrator nodes)
//
// # Storage URL formats
//
//	s3://bucket?endpoint=https://...&region=us-east-1   AWS S3 / S3-compatible
//	gs://bucket                                          Google Cloud Storage
//	file:///abs/path                                     local directory
//
// Important: S3-compatible stores that do NOT support path-style addressing
// (e.g. Volcengine TOS) must omit s3ForcePathStyle from the URL.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func main() {
	buildFlag := flag.String("build", "", "build `ID` (UUID); mutually exclusive with -template")
	templateFlag := flag.String("template", "", "template alias or ID; resolved to a build ID via the E2B API\n\t(requires E2B_API_KEY; set E2B_API_URL for self-hosted/local dev, or E2B_DOMAIN for self-hosted)")
	storageFlag := flag.String("storage", "",
		"storage `URL` for the template bucket.\n"+
			"\tFormats: s3://bucket?endpoint=...&region=..., gs://bucket, file:///path\n"+
			"\tWhen omitted, falls back to TEMPLATE_STORAGE_URL env var, then to the\n"+
			"\tlegacy STORAGE_PROVIDER + TEMPLATE_BUCKET_NAME env vars.\n"+
			"\tAWS credentials are always read from the standard AWS env vars\n"+
			"\t(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, etc.).")
	outputFlag := flag.String("output", "", "write a docker-loadable tar to `file`; load with: docker load -i <file>")
	pushFlag := flag.String("push", "", "push the image to a registry `ref`, e.g. myrepo/myimage:latest")
	tagFlag := flag.String("tag", "", "image `tag` in the tar manifest\n\t(default: <template-alias>:latest when -template is used, otherwise e2b-rootfs:latest)")
	flag.Parse()

	if *buildFlag == "" && *templateFlag == "" {
		log.Fatal("one of -build or -template is required")
	}
	if *buildFlag != "" && *templateFlag != "" {
		log.Fatal("-build and -template are mutually exclusive")
	}
	if *outputFlag == "" && *pushFlag == "" {
		log.Fatal("one of -output or -push is required")
	}

	buildID := *buildFlag
	if *templateFlag != "" {
		var err error
		buildID, err = cmdutil.ResolveTemplateID(*templateFlag)
		if err != nil {
			log.Fatalf("resolve template %q: %v", *templateFlag, err)
		}
		fmt.Printf("resolved template %q -> build %s\n", *templateFlag, buildID)
	}

	// Docker repository names must be lowercase; template aliases are not
	// guaranteed to be, so normalise the default tag.
	imageTag := *tagFlag
	if imageTag == "" {
		if *templateFlag != "" {
			imageTag = strings.ToLower(*templateFlag) + ":latest"
		} else {
			imageTag = "e2b-rootfs:latest"
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() { <-sig; cancel() }()

	// NBD cleanup must not share the cancellable ctx: cancelling the context
	// while a device is still serving blocks causes a deadlock in the kernel
	// NBD driver. A separate background context lets the deferred cleanups in
	// run() drain gracefully after the main ctx is cancelled.
	nbdCtx := context.Background()

	if err := run(ctx, nbdCtx, buildID, *storageFlag, *outputFlag, *pushFlag, imageTag); err != nil {
		log.Fatalf("export failed: %v", err)
	}
}

// resolveSpec resolves the template storage destination in priority order:
//  1. -storage flag as a URL  (e.g. s3://bucket?endpoint=...&region=...)
//  2. -storage flag as a local path  (convenience alias for file:// URLs)
//  3. TEMPLATE_STORAGE_URL environment variable
//  4. Legacy STORAGE_PROVIDER + TEMPLATE_BUCKET_NAME environment variables
func resolveSpec(storageFlag string) (storage.Spec, error) {
	if storageFlag == "" {
		// Delegate to cfg which reads TEMPLATE_STORAGE_URL first, then falls
		// back to the legacy STORAGE_PROVIDER / TEMPLATE_BUCKET_NAME vars.
		return cfg.TemplateStorage()
	}
	if strings.Contains(storageFlag, "://") {
		return storage.ParseStorageURL(storageFlag)
	}
	// Plain path -- treat as a local directory (consistent with mount-build-rootfs).
	return cmdutil.TemplateSpec(storageFlag)
}

func run(ctx, nbdCtx context.Context, buildID, storageFlag, outputPath, pushRef, imageTag string) error {
	templateSpec, err := resolveSpec(storageFlag)
	if err != nil {
		return fmt.Errorf("resolve storage: %w", err)
	}
	fmt.Printf("storage provider: %s\n", templateSpec.Provider)

	fmt.Println("loading rootfs from storage...")
	rootfs, rootfsCleanup, err := testutils.TemplateRootfs(ctx, templateSpec, buildID)
	if err != nil {
		rootfsCleanup.Run(nbdCtx, 30*time.Second)
		return fmt.Errorf("load rootfs: %w", err)
	}
	defer rootfsCleanup.Run(nbdCtx, 30*time.Second)

	// COW cache absorbs writes from the NBD layer during mount, keeping the
	// stored rootfs immutable (safe to export while sandboxes are running).
	cowPath := filepath.Join(os.TempDir(), buildID+"-export-cow-"+uuid.New().String())
	defer os.RemoveAll(cowPath)

	cache, err := block.NewCache(
		int64(rootfs.Header().Metadata.Size),
		int64(rootfs.Header().Metadata.BlockSize),
		cowPath,
		false,
	)
	if err != nil {
		return fmt.Errorf("create COW cache: %w", err)
	}

	overlay := block.NewOverlay(rootfs, cache)
	defer overlay.Close()

	featureFlags, err := featureflags.NewClient()
	if err != nil {
		return fmt.Errorf("create feature flags client: %w", err)
	}

	fmt.Println("provisioning NBD device...")
	devicePath, deviceCleanup, err := nbd.GetNBDDevice(nbdCtx, testutils.NewLoggerOverlay(overlay), featureFlags)
	if err != nil {
		deviceCleanup.Run(nbdCtx, 30*time.Second)
		// If this fails with "NBD module not loaded", run: modprobe nbd max_part=8
		return fmt.Errorf("get NBD device: %w", err)
	}
	defer deviceCleanup.Run(nbdCtx, 30*time.Second)
	fmt.Printf("rootfs on device: %s\n", devicePath)

	mountPath, err := os.MkdirTemp("", buildID+"-export-mount-")
	if err != nil {
		return fmt.Errorf("create mount dir: %w", err)
	}
	defer os.RemoveAll(mountPath)

	fmt.Printf("mounting at %s...\n", mountPath)
	mountCleanup, err := nbd.MountNBDDevice(devicePath, mountPath)
	if err != nil {
		mountCleanup.Run(nbdCtx, 30*time.Second)
		return fmt.Errorf("mount NBD device: %w", err)
	}
	defer mountCleanup.Run(nbdCtx, 30*time.Second)

	// Archive to a temp file before building the OCI layer. go-containerregistry
	// calls the layer opener twice (digest computation + actual upload) and sends
	// Content-Length from the first pass. A live tar pipe can produce slightly
	// different bytes on each run (xattr PAX headers vary when filesystem metadata
	// is touched during the first read), causing an HTTP ContentLength mismatch.
	// Writing once to a file guarantees both passes read identical bytes.
	layerFile, err := os.CreateTemp("", buildID+"-layer-*.tar")
	if err != nil {
		return fmt.Errorf("create layer temp file: %w", err)
	}
	layerPath := layerFile.Name()
	defer os.Remove(layerPath)

	fmt.Println("archiving filesystem...")
	tarCmd := exec.CommandContext(ctx, "tar",
		"-C", mountPath,
		"--one-file-system", // do not cross bind-mount boundaries inside the rootfs
		"--numeric-owner",   // preserve UID/GID numerically -- no host passwd lookup
		"--xattrs",          // preserve extended attributes (e.g. security.capability)
		"--xattrs-include=*", // include all xattr namespaces; stored as PAX headers in the OCI layer
		"-c",
		".",
	)
	tarCmd.Stdout = layerFile
	tarCmd.Stderr = os.Stderr
	if err := tarCmd.Run(); err != nil {
		layerFile.Close()
		return fmt.Errorf("archive rootfs: %w", err)
	}
	if err := layerFile.Close(); err != nil {
		return fmt.Errorf("flush layer file: %w", err)
	}

	fmt.Println("building OCI image layer...")
	layer, err := tarball.LayerFromFile(layerPath)
	if err != nil {
		return fmt.Errorf("create OCI layer: %w", err)
	}

	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		return fmt.Errorf("append layer: %w", err)
	}

	cfgFile, err := img.ConfigFile()
	if err != nil {
		return fmt.Errorf("get image config: %w", err)
	}
	cfgFile.Architecture = utils.TargetArch() // respects TARGET_ARCH env var; defaults to host arch
	cfgFile.OS = "linux"
	img, err = mutate.ConfigFile(img, cfgFile)
	if err != nil {
		return fmt.Errorf("set image config: %w", err)
	}

	if outputPath != "" {
		fmt.Printf("writing tar to %s (tag: %s)...\n", outputPath, imageTag)
		tag, err := name.NewTag(imageTag)
		if err != nil {
			return fmt.Errorf("parse image tag %q: %w", imageTag, err)
		}
		if err := tarball.WriteToFile(outputPath, tag, img); err != nil {
			return fmt.Errorf("write tar: %w", err)
		}
		fmt.Printf("done -- load with: docker load -i %s\n", outputPath)
	}

	if pushRef != "" {
		fmt.Printf("pushing to %s...\n", pushRef)
		ref, err := name.ParseReference(pushRef)
		if err != nil {
			return fmt.Errorf("parse push ref %q: %w", pushRef, err)
		}
		// Credentials sourced from the standard keychain: ~/.docker/config.json,
		// credential helpers (docker-credential-*), or DOCKER_CONFIG env var.
		if err := remote.Write(ref, img, remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
			return fmt.Errorf("push image: %w", err)
		}
		fmt.Printf("done -- pushed to %s\n", pushRef)
	}

	return nil
}
