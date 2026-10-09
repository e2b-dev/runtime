package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"sync/atomic"

	googleStorage "cloud.google.com/go/storage"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type Destination struct {
	Path    string
	CRC     uint32
	exists  bool
	isLocal bool
}

func NewDestinationFromObject(ctx context.Context, o *googleStorage.ObjectHandle) (*Destination, error) {
	var crc uint32
	exists := false
	if attrs, err := o.Attrs(ctx); err == nil {
		crc = attrs.CRC32C
		exists = true
	} else if !errors.Is(err, googleStorage.ErrObjectNotExist) {
		return nil, fmt.Errorf("failed to get object attributes: %w", err)
	}

	return &Destination{
		Path:    fmt.Sprintf("gs://%s/%s", o.BucketName(), o.ObjectName()),
		CRC:     crc,
		exists:  exists,
		isLocal: false,
	}, nil
}

func NewDestinationFromPath(prefix, file string) (*Destination, error) {
	// Local storage uses templates subdirectory
	p := path.Join(prefix, "templates", file)

	if _, err := os.Stat(p); err == nil {
		f, err := os.Open(p)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		defer f.Close()

		h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		_, err = io.Copy(h, f)
		if err != nil {
			return nil, fmt.Errorf("failed to copy file: %w", err)
		}
		crc := h.Sum32()

		return &Destination{
			Path:    p,
			CRC:     crc,
			exists:  true,
			isLocal: true,
		}, nil
	}

	return &Destination{
		Path:    p,
		isLocal: true,
	}, nil
}

func NewHeaderFromObject(ctx context.Context, bucketName string, headerPath string) (*header.Header, error) {
	b, err := storage.NewGCP(ctx, bucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS bucket storage provider: %w", err)
	}

	obj, err := b.OpenBlob(ctx, headerPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	h, err := header.Deserialize(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	return h, nil
}

type osFileBlob struct {
	f *os.File
}

func (o *osFileBlob) WriteTo(_ context.Context, w io.Writer) (int64, error) {
	return io.Copy(w, o.f)
}

func (o *osFileBlob) Exists(_ context.Context) (bool, error) {
	return true, nil
}

func (o *osFileBlob) Put(_ context.Context, _ []byte, _ ...storage.PutOption) error {
	return errors.New("not implemented")
}

func NewHeaderFromPath(ctx context.Context, from, headerPath string) (*header.Header, error) {
	// Local storage uses templates subdirectory
	f, err := os.Open(path.Join(from, "templates", headerPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	h, err := header.Deserialize(ctx, &osFileBlob{f: f})
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	return h, nil
}

func getReferencedData(h *header.Header, dataFileName string) []string {
	builds := make(map[uuid.UUID]struct{})

	for _, mapping := range h.Mapping.All() {
		builds[mapping.BuildId] = struct{}{}
	}

	delete(builds, uuid.Nil)

	var dataReferences []string

	for build := range builds {
		paths := storage.Paths{
			BuildID: build.String(),
		}

		ct := h.GetBuildFrameData(build).CompressionType()

		dataReferences = append(dataReferences, paths.DataFile(dataFileName, ct))
	}

	return dataReferences
}

func localCopy(ctx context.Context, from, to *Destination) error {
	command := []string{
		"rsync",
		"-aH",
		"--whole-file",
		"--mkpath",
		"--inplace",
		from.Path,
		to.Path,
	}

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to copy local file (%v): %w\n%s", command, err, string(output))
	}

	return nil
}

func gcloudCopy(ctx context.Context, from, to *Destination) error {
	command := []string{
		"gcloud",
		"storage",
		"cp",
		"--verbosity",
		"error",
		from.Path,
		to.Path,
	}

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to copy GCS object (%v): %w\n%s", command, err, string(output))
	}

	return nil
}

// deriveArtifactBuckets maps a template bucket (gs://<prefix>-fc-templates) to the
// sibling firecracker-versions and kernels buckets in the same project/env. The build
// records no environment, so the env is taken from the template bucket location.
func deriveArtifactBuckets(templateLoc string) (fcBucket, kernelBucket string, err error) {
	if !strings.HasPrefix(templateLoc, "gs://") {
		return "", "", fmt.Errorf("-gdb requires a gs:// location, got %q", templateLoc)
	}
	bucket := strings.TrimSuffix(strings.TrimPrefix(templateLoc, "gs://"), "/")
	prefix, ok := strings.CutSuffix(bucket, "-fc-templates")
	if !ok {
		return "", "", fmt.Errorf("cannot derive versions/kernels buckets from %q (expected a *-fc-templates bucket)", templateLoc)
	}

	return "gs://" + prefix + "-fc-versions", "gs://" + prefix + "-fc-kernels", nil
}

// copyGdbArtifacts ensures the build's FC + kernel runtime and debug artifacts exist at
// the destination, copying each from the source env's versions/kernels buckets only if
// it is not already present at the destination (so a large vmlinux.debug is not recopied
// for a version already there). Required artifacts must exist at the source; the optional
// firecracker-debug.debug (FC's own symbols, not needed for guest-kernel gdb) is skipped
// if absent.
func copyGdbArtifacts(ctx context.Context, client *googleStorage.Client, from, to, arch, fcVer, kernelVer string) error {
	fcFrom, kFrom, err := deriveArtifactBuckets(from)
	if err != nil {
		return err
	}
	fcTo, kTo, err := deriveArtifactBuckets(to)
	if err != nil {
		return err
	}

	artifacts := []struct {
		name                      string
		srcBucket, dstBucket, obj string
		required                  bool
	}{
		{"firecracker", fcFrom, fcTo, path.Join(fcVer, arch, "firecracker"), true},
		{"firecracker-debug", fcFrom, fcTo, path.Join(fcVer, arch, "firecracker-debug"), true},
		{"firecracker-debug.debug", fcFrom, fcTo, path.Join(fcVer, arch, "firecracker-debug.debug"), false},
		{"vmlinux.bin", kFrom, kTo, path.Join(kernelVer, arch, "vmlinux.bin"), true},
		{"vmlinux.debug", kFrom, kTo, path.Join(kernelVer, arch, "vmlinux.debug"), true},
	}

	for _, a := range artifacts {
		srcBucket := strings.TrimPrefix(a.srcBucket, "gs://")
		dstBucket := strings.TrimPrefix(a.dstBucket, "gs://")
		src, err := NewDestinationFromObject(ctx, client.Bucket(srcBucket).Object(a.obj))
		if err != nil {
			return fmt.Errorf("stat source %s: %w", a.name, err)
		}
		dst, err := NewDestinationFromObject(ctx, client.Bucket(dstBucket).Object(a.obj))
		if err != nil {
			return fmt.Errorf("stat destination %s: %w", a.name, err)
		}

		if !src.exists { // not present at source
			if a.required {
				return fmt.Errorf("required gdb artifact %s not found at %s (is this version published with gdb support?)", a.name, src.Path)
			}
			fmt.Fprintf(os.Stderr, "-> optional gdb artifact %s not at source, skipping\n", a.name)

			continue
		}
		// Skip only when the destination already holds identical content (CRC32C match);
		// otherwise copy, replacing a divergent/stale or absent artifact rather than
		// trusting it. The dst.exists guard avoids a false 0 == 0 match when the
		// destination is missing and the source's CRC32C is genuinely zero.
		if dst.exists && src.CRC == dst.CRC {
			fmt.Fprintf(os.Stderr, "-> gdb artifact %s already current at destination, skipping\n", a.name)

			continue
		}
		fmt.Fprintf(os.Stderr, "+ copying gdb artifact '%s' to '%s'\n", src.Path, dst.Path)
		if err := gcloudCopy(ctx, src, dst); err != nil {
			return fmt.Errorf("copy %s: %w", a.name, err)
		}
	}

	return nil
}

func main() {
	buildId := flag.String("build", "", "build id")
	from := flag.String("from", "", "from destination")
	to := flag.String("to", "", "to destination")
	teamID := flag.String("team", "", "team UUID (if set, prints SQL to populate DB on stdout)")
	envdVersion := flag.String("envd-version", "", "envd version (required if team provided) — must match the version present in the template")
	vcpu := flag.Int("vcpu", 2, "vCPUs")
	memory := flag.Int("memory", 1024, "memory MB")
	disk := flag.Int("disk", 1024, "disk MB")
	tag := flag.String("tag", "default", "build assignment tag")
	gdb := flag.Bool("gdb", false, "also copy the build's FC + kernel runtime and debug artifacts (firecracker, firecracker-debug, vmlinux.bin, vmlinux.debug) into the matching versions/kernels buckets so the snapshot is gdb-ready at the destination; requires gs:// -from/-to")
	arch := flag.String("arch", "amd64", "artifact arch for -gdb (amd64 or arm64)")

	flag.Parse()

	if *teamID != "" && *envdVersion == "" {
		log.Fatal("-envd-version is required when -team is set")
	}
	// Validate the -gdb bucket preconditions up front, so a wrong invocation fails
	// instantly rather than after the multi-GB snapshot copy. deriveArtifactBuckets
	// checks both the gs:// scheme and the *-fc-templates suffix it needs to map the
	// template bucket to its versions/kernels siblings.
	if *gdb {
		if _, _, err := deriveArtifactBuckets(*from); err != nil {
			log.Fatalf("-gdb -from: %s", err)
		}
		if _, _, err := deriveArtifactBuckets(*to); err != nil {
			log.Fatalf("-gdb -to: %s", err)
		}
	}

	fmt.Fprintf(os.Stderr, "Copying build '%s' from '%s' to '%s'\n", *buildId, *from, *to)

	paths := storage.Paths{
		BuildID: *buildId,
	}

	ctx := context.Background()

	var filesToCopy []string

	// Metadata is the source of truth for whether the build is a
	// filesystem-only snapshot (no memfile, no snapfile).
	var meta metadata.Template
	if strings.HasPrefix(*from, "gs://") {
		bucketName, _ := strings.CutPrefix(*from, "gs://")

		provider, err := storage.NewGCP(ctx, bucketName, nil)
		if err != nil {
			log.Fatalf("failed to create GCS bucket storage provider: %s", err)
		}

		m, err := metadata.FromBuildID(ctx, provider, *buildId)
		if err != nil {
			log.Fatalf("failed to read build metadata: %s", err)
		}

		meta = m
	} else {
		m, err := metadata.FromFile(path.Join(*from, "templates", paths.Metadata()))
		if err != nil {
			log.Fatalf("failed to read build metadata: %s", err)
		}

		meta = m
	}

	if meta.IsFilesystemOnly() {
		fmt.Fprintf(os.Stderr, "Build is a filesystem-only snapshot; skipping memfile and snapfile\n")
	} else {
		// Extract all files referenced by the build memfile header
		buildMemfileHeaderPath := paths.MemfileHeader()

		var memfileHeader *header.Header
		if strings.HasPrefix(*from, "gs://") {
			bucketName, _ := strings.CutPrefix(*from, "gs://")

			h, err := NewHeaderFromObject(ctx, bucketName, buildMemfileHeaderPath)
			if err != nil {
				log.Fatalf("failed to create header from object: %s", err)
			}

			memfileHeader = h
		} else {
			h, err := NewHeaderFromPath(ctx, *from, buildMemfileHeaderPath)
			if err != nil {
				log.Fatalf("failed to create header from path: %s", err)
			}

			memfileHeader = h
		}

		dataReferences := getReferencedData(memfileHeader, storage.MemfileName)

		filesToCopy = append(filesToCopy, buildMemfileHeaderPath)
		filesToCopy = append(filesToCopy, dataReferences...)

		// Add the snapfile to the list of files to copy
		snapfilePath := paths.Snapfile()
		filesToCopy = append(filesToCopy, snapfilePath)
	}

	// Extract all files referenced by the build rootfs header
	buildRootfsHeaderPath := paths.RootfsHeader()

	var rootfsHeader *header.Header
	if strings.HasPrefix(*from, "gs://") {
		bucketName, _ := strings.CutPrefix(*from, "gs://")
		h, err := NewHeaderFromObject(ctx, bucketName, buildRootfsHeaderPath)
		if err != nil {
			log.Fatalf("failed to create header from object: %s", err)
		}

		rootfsHeader = h
	} else {
		h, err := NewHeaderFromPath(ctx, *from, buildRootfsHeaderPath)
		if err != nil {
			log.Fatalf("failed to create header from path: %s", err)
		}

		rootfsHeader = h
	}

	dataReferences := getReferencedData(rootfsHeader, storage.RootfsName)

	filesToCopy = append(filesToCopy, buildRootfsHeaderPath)
	filesToCopy = append(filesToCopy, dataReferences...)

	metadataPath := paths.Metadata()
	filesToCopy = append(filesToCopy, metadataPath)

	// sort files to copy
	slices.Sort(filesToCopy)

	googleStorageClient, err := googleStorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create Google Storage client: %s", err)
	}

	fmt.Fprintf(os.Stderr, "Copying %d files\n", len(filesToCopy))

	var errgroup errgroup.Group

	errgroup.SetLimit(20)

	var done atomic.Int32

	for _, file := range filesToCopy {
		errgroup.Go(func() error {
			var fromDestination *Destination
			if strings.HasPrefix(*from, "gs://") {
				bucketName, _ := strings.CutPrefix(*from, "gs://")
				fromObject := googleStorageClient.Bucket(bucketName).Object(file)
				d, destErr := NewDestinationFromObject(ctx, fromObject)
				if destErr != nil {
					return fmt.Errorf("failed to create destination from object: %w", destErr)
				}

				fromDestination = d
			} else {
				d, destErr := NewDestinationFromPath(*from, file)
				if destErr != nil {
					return fmt.Errorf("failed to create destination from path: %w", destErr)
				}

				fromDestination = d
			}

			var toDestination *Destination
			if strings.HasPrefix(*to, "gs://") {
				bucketName, _ := strings.CutPrefix(*to, "gs://")
				toObject := googleStorageClient.Bucket(bucketName).Object(file)
				d, destErr := NewDestinationFromObject(ctx, toObject)
				if destErr != nil {
					return fmt.Errorf("failed to create destination from object: %w", destErr)
				}

				toDestination = d
			} else {
				d, destErr := NewDestinationFromPath(*to, file)
				if destErr != nil {
					return fmt.Errorf("failed to create destination from path: %w", destErr)
				}

				toDestination = d

				mkdirErr := os.MkdirAll(path.Dir(toDestination.Path), 0o755)
				if mkdirErr != nil {
					return fmt.Errorf("failed to create directory: %w", mkdirErr)
				}
			}

			fmt.Fprintf(os.Stderr, "+ copying '%s' to '%s'\n", fromDestination.Path, toDestination.Path)

			if fromDestination.CRC == toDestination.CRC && fromDestination.CRC != 0 {
				fmt.Fprintf(os.Stderr, "-> [%d/%d] '%s' already exists, skipping\n", done.Load(), len(filesToCopy), toDestination.Path)

				done.Add(1)

				return nil
			}

			if fromDestination.isLocal && toDestination.isLocal {
				err := localCopy(ctx, fromDestination, toDestination)
				if err != nil {
					return fmt.Errorf("failed to copy local file: %w", err)
				}
			} else {
				err := gcloudCopy(ctx, fromDestination, toDestination)
				if err != nil {
					return fmt.Errorf("failed to copy GCS object: %w", err)
				}
			}

			done.Add(1)

			fmt.Fprintf(os.Stderr, "-> [%d/%d] '%s' copied\n", done.Load(), len(filesToCopy), toDestination.Path)

			return nil
		})
	}

	if err := errgroup.Wait(); err != nil {
		log.Fatalf("failed to copy files: %s", err)
	}

	fmt.Fprintf(os.Stderr, "Build '%s' copied to '%s'\n", *buildId, *to)

	if *teamID != "" || *gdb {
		// metadata carries the kernel + FC versions; both the -gdb artifact copy and
		// the -team SQL seed need them.
		if meta.Template.KernelVersion == "" || meta.Template.FirecrackerVersion == "" {
			log.Fatalf("kernel/firecracker version missing in build metadata (version %d)", meta.Version)
		}

		if *gdb {
			if err := copyGdbArtifacts(ctx, googleStorageClient, *from, *to, *arch, meta.Template.FirecrackerVersion, meta.Template.KernelVersion); err != nil {
				log.Fatalf("failed to copy gdb artifacts: %s", err)
			}
			fmt.Fprintf(os.Stderr, "gdb artifacts ensured at destination (arch %s)\n", *arch)
		}

		if *teamID != "" {
			envID := id.Generate()
			fmt.Fprintf(os.Stderr, "\n\nGenerated env ID: %s\n\n", envID)

			fmt.Printf("BEGIN;\n")
			fmt.Printf("INSERT INTO public.envs (id, team_id, updated_at, public, source)\n")
			fmt.Printf("VALUES ('%s', '%s', NOW(), FALSE, 'template');\n\n", envID, *teamID)
			fmt.Printf("INSERT INTO public.env_builds (id, env_id, team_id, updated_at, finished_at, status, ram_mb, vcpu, kernel_version, firecracker_version, envd_version, free_disk_size_mb, total_disk_size_mb)\n")
			fmt.Printf("VALUES ('%s', '%s', '%s', NOW(), NOW(), 'uploaded', %d, %d, '%s', '%s', '%s', %d, %d);\n\n",
				*buildId, envID, *teamID, *memory, *vcpu, meta.Template.KernelVersion, meta.Template.FirecrackerVersion, *envdVersion, *disk, *disk)
			fmt.Printf("INSERT INTO public.env_build_assignments (env_id, build_id, tag)\n")
			fmt.Printf("VALUES ('%s', '%s', '%s');\n", envID, *buildId, *tag)
			fmt.Printf("COMMIT;\n")
		}
	}
}
