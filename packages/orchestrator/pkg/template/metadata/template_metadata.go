package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/ioutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const (
	CurrentVersion = 2

	DeprecatedVersion = 1

	// FilesystemOnlyVersion is the metadata version that introduced the
	// filesystem_only field. A filesystem-only snapshot must be stamped at least
	// this version so deserialize() (which strips every field from a version <=
	// DeprecatedVersion snapshot) keeps the flag. It is pinned rather than
	// CurrentVersion: CurrentVersion advances as the format grows, and stamping a
	// minimal V1-derived snapshot with a newer version would falsely imply it
	// carries later-version fields.
	FilesystemOnlyVersion = DeprecatedVersion + 1
)

// ErrReplaceCommitted means the replacement is visible, but parent-directory
// durability could not be confirmed.
var ErrReplaceCommitted = ioutils.ErrAtomicWriteCommitted

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata")

// AccessType is a compact representation of block access type for JSON serialization.
type AccessType string

const (
	AccessRead     AccessType = "r"
	AccessWrite    AccessType = "w"
	AccessPrefetch AccessType = "p"
)

// blockToAccessType maps block.AccessType to metadata.AccessType.
var blockToAccessType = map[block.AccessType]AccessType{
	block.Read:     AccessRead,
	block.Write:    AccessWrite,
	block.Prefetch: AccessPrefetch,
}

type Version struct {
	Version any `json:"version"`
}

type Context struct {
	User    string            `json:"user,omitempty"`
	WorkDir *string           `json:"workdir,omitempty"`
	EnvVars map[string]string `json:"env_vars,omitempty"`
}

// EnvdDefaultUser reports the DefaultUser this template's build sent to envd's /init, and
// whether the metadata can establish it at all.
//
// Only one recorded value can be established, and the reason is that determinacy here is a
// property of the build's TEMPLATE VERSION while the metadata records a USER:
//
//   - finalize sends Context.User for a build at or above TemplateV2ReleaseVersion, and a
//     flat ("user", nil) for anything below it, discarding whatever was recorded.
//   - Context.User is written by three things at three different gates: the base phase
//     stamps "root" unconditionally for every from-image build; the USER phase overwrites
//     it with "user", gated on that same version; and a user-authored USER step overwrites
//     it with any name at ANY version, because only the USER phase is gated and the step
//     builders are appended unconditionally.
//
// So a recorded "app" is either a recent build whose /init received "app", or an older one
// whose /init received "user" — and the build's template version is not carried in the
// snapshot (Template.Version is the metadata FORMAT version). Same ambiguity for "root".
// Only consts.TemplateDefaultUser is safe, because both branches send exactly that.
//
// No workdir is reported for the same reason and it is not an oversight: finalize sends
// Context.WorkDir above the version gate and nil below it, so a recorded workdir is
// unprovable by the same argument. Re-sending one would move the working directory of
// every command that omits a cwd, on every start, for a template that never had it.
func (t Template) EnvdDefaultUser() (user string, ok bool) {
	if t.Context.User != consts.TemplateDefaultUser {
		return "", false
	}

	return t.Context.User, true
}

func (c Context) WithUser(user string) Context {
	c.User = user

	return c
}

type FromTemplate struct {
	Alias   string `json:"alias"`
	BuildID string `json:"build_id"`
}

type Start struct {
	StartCmd string  `json:"start_command"`
	ReadyCmd string  `json:"ready_command"`
	Context  Context `json:"context"`
}

type TemplateMetadata struct {
	BuildID            string `json:"build_id"`
	KernelVersion      string `json:"kernel_version"`
	FirecrackerVersion string `json:"firecracker_version"`
}

// MemoryPrefetchMapping stores block offsets that should be prefetched when starting a sandbox.
// This is used to speed up sandbox starts by proactively fetching blocks that are likely to be needed.
type MemoryPrefetchMapping struct {
	// Indices is an ordered array of block indices to prefetch, preserving the order in which blocks were accessed
	Indices []uint64 `json:"indices"`
	// AccessTypes stores the access type ("r"/"w"/"p") for each block, aligned with Indices
	AccessTypes []AccessType `json:"access_types"`
	// BlockSize is the size of each block in bytes
	BlockSize int64 `json:"block_size"`
}

// AccessTypeFromBlock converts a block.AccessType to metadata.AccessType.
func AccessTypeFromBlock(at block.AccessType) AccessType {
	return blockToAccessType[at]
}

// Count returns the number of blocks to prefetch.
func (p *MemoryPrefetchMapping) Count() int {
	if p == nil {
		return 0
	}

	return len(p.Indices)
}

// PrefetchOrigin names the producer of a prefetch mapping, so a resume can
// report where the mapping it replays came from.
type PrefetchOrigin string

const (
	// PrefetchOriginBuild is the template builder's optimize phase, or the
	// resume-build tool: recorded before the build is published.
	PrefetchOriginBuild PrefetchOrigin = "build"
	// PrefetchOriginCheckpoint is the resume-fresh checkpoint, which traces
	// its own resume and embeds the mapping before the upload.
	PrefetchOriginCheckpoint PrefetchOrigin = "checkpoint"
	// PrefetchOriginHarvest is the throwaway resume after a pause or an
	// in-place checkpoint, persisted after the build is published.
	PrefetchOriginHarvest PrefetchOrigin = "harvest"
)

type Prefetch struct {
	Memory *MemoryPrefetchMapping `json:"memory"`
	// Origin is the producer of Memory. Empty on a mapping written before the
	// field existed, which every reader treats like any other mapping.
	Origin PrefetchOrigin `json:"origin,omitempty"`
}

// InitOrigin reports the origin of the init mapping p carries, "none" when
// there is no mapping and "unknown" when the mapping predates origins.
func (p *Prefetch) InitOrigin() string {
	if p == nil || p.Memory.Count() == 0 {
		return "none"
	}
	if p.Origin == "" {
		return "unknown"
	}

	return string(p.Origin)
}

// Balloon is the balloon device configuration a template was built with.
type Balloon struct {
	Reporting bool `json:"reporting"`
	Hinting   bool `json:"hinting"`
}

type Template struct {
	Version      uint64           `json:"version"`
	Template     TemplateMetadata `json:"template"`
	Context      Context          `json:"context"`
	Start        *Start           `json:"start,omitempty"`
	FromImage    *string          `json:"from_image,omitempty"`
	FromTemplate *FromTemplate    `json:"from_template,omitempty"`
	Prefetch     *Prefetch        `json:"prefetch,omitempty"`

	// CmdlineArgs records the extra guest kernel command line parameters this template's
	// kernel booted with, parsed and normalised. Nil is the default command line, which is
	// what every pre-existing template has.
	//
	// It stores the PARAMETERS, not the flag value they came from, and that is the point:
	// the flag is edited freely, so what it says can change after this template was built.
	// A cold boot that re-read the flag would silently reproduce a different command line
	// than the build used. Storing the parsed parameters makes a snapshot self-describing.
	//
	// It sits at the top level, alongside Context and Start, because it is inherited lineage
	// state rather than per-build identity: chosen once, at build, with no source to
	// re-derive it from. The nested TemplateMetadata is the wrong home for exactly that
	// reason — SameVersionTemplate replaces that struct wholesale on every pause, so a field
	// living there would be dropped by the most frequently executed path in the system.
	//
	// It exists so the cold boot of a filesystem-only snapshot can re-apply the same command
	// line; a memory resume restores a running kernel and never re-reads it.
	CmdlineArgs map[string]string `json:"cmdline_args,omitempty"`

	// Balloon records the free-page mechanisms this template's balloon was
	// built with. It is lineage state like CmdlineArgs: chosen once at build,
	// carried by the copy-constructors, and it is what lets a resume label its
	// first faults before the VM starts. Nil is a template built before the
	// field existed; the mode is then read from the device after start.
	// Sandbox.Pause re-stamps it from device truth when that is known, so a
	// cold-booted lineage (no balloon) does not inherit its template's.
	Balloon *Balloon `json:"balloon,omitempty"`

	// CPUTemplate is the CPU template this template's guest booted with. Nil is none. Stored
	// parsed, since the flag can change after the build; a filesystem-only cold boot replays
	// it. Top level like CmdlineArgs, so SameVersionTemplate carries it on every pause.
	CPUTemplate *cputemplate.Template `json:"cpu_template,omitempty"`

	// BuildCPUTemplate is the CPU template the build booted with. It never changes after the
	// build, while CPUTemplate follows what the guest runs: a reboot override replaces
	// CPUTemplate only, so clearing the override boots BuildCPUTemplate again.
	BuildCPUTemplate *cputemplate.Template `json:"build_cpu_template,omitempty"`

	// VcpuCount is how many vCPUs the VM in this snapshot has, which a resume limits the sandbox
	// below without trusting the request. 0 is a legacy snapshot whose stored original count must
	// arrive as max_vcpus (or vcpu when the allocation is unchanged).
	VcpuCount int64 `json:"vcpu_count,omitempty"`

	// FilesystemOnly marks a snapshot that persists only the filesystem (no
	// memory snapshot); resuming it must cold-boot (reboot) from the rootfs. The
	// zero value (false) is a full memory snapshot, so pre-existing snapshots
	// (no field) are correctly treated as memory without a migration. Stamped at
	// pause time; it is the resume path's source of truth for
	// reboot-vs-memory-resume. Deliberately NOT carried by the
	// With*/SameVersionTemplate copy-constructors — Sandbox.Pause re-stamps it.
	FilesystemOnly bool `json:"filesystem_only,omitempty"`

	// FsQuiesced records that a filesystem-only snapshot's rootfs was captured
	// while frozen (FIFREEZE — native for envd >= 0.6.6, or via the exec API for
	// older envd) rather than merely sync'd, so it is crash-consistent. The zero
	// value (false) means unknown / not-frozen — a legacy snapshot (no field) or a
	// sync fallback. Only meaningful alongside FilesystemOnly. Stamped at pause and
	// re-stamped every pause; deliberately NOT carried by the copy-constructors.
	// This is a building block: it lets a later feature decide safely whether a
	// snapshot is one it can cold-boot / rewrite without repairing the journal.
	FsQuiesced bool `json:"fs_quiesced,omitempty"`

	// RunningEnvdVersion is the version of the envd running in this memory snapshot when it
	// differs from the template-baked one: the version the running envd last reported, else
	// the one a confirmed live upgrade delivered, else the one this life was resumed with.
	// Empty is the baked version, which is what every snapshot from before the field has. A
	// memory resume of this snapshot gates its first envd /init on it, before the restored
	// envd has reported anything, unless the resume is given a version
	// (WithInheritedEnvdVersion); a cold boot runs the rootfs binary, which a live upgrade
	// does not replace, so it never reads the field. Stamped at every pause and empty for a
	// filesystem-only one. SameVersionTemplate, NewVersionTemplate and BasedOn drop it;
	// WithPrefetch keeps it, since it rewrites the same snapshot's metadata, as do the
	// in-place stamps. V1 metadata discards it, and such a lineage gates on the baked
	// version unless given one.
	RunningEnvdVersion string `json:"running_envd_version,omitempty"`

	// GuestOS names the guest profile of the template. Empty is the default
	// profile, which every pre-existing template has. It is lineage state like
	// CmdlineArgs: a build fixes it, and the copy-constructors carry it. The
	// reader refuses a value that no registered profile claims, and
	// GuestProfile turns it into the profile.
	GuestOS string `json:"guest_os,omitempty"`
	// RootfsDriveID, NetIfaceID and RootfsFile are the device names a memory
	// snapshot bakes when this runtime did not make it: the block device ID and the network
	// interface ID the API addresses after the load, and the file name the rootfs
	// is presented at under the sandbox directory. Empty is the runtime's own name for
	// each, which every snapshot it made has. Lineage state of the memory image:
	// carried by the copy-constructors that keep the same memory, stamped from the
	// running VM at every pause, and dropped by BasedOn, whose build cold-boots
	// and names its own devices.
	RootfsDriveID string `json:"rootfs_drive_id,omitempty"`
	NetIfaceID    string `json:"net_iface_id,omitempty"`
	RootfsFile    string `json:"rootfs_file,omitempty"`
	// NoMmdsStore marks a memory image whose microVM has no MMDS store, which
	// only an image made outside this runtime and its paused lineage have:
	// Firecracker cannot add a store after a restore, so the envd in it
	// accepts no new access token. EnvdAccessTokenHash is then the
	// keys.HashAccessToken of the token that envd holds (of "" for none), and
	// empty when no pause could know it. Both are device truth stamped at every
	// pause, carried with the memory like the device names, and dropped by
	// BasedOn, whose build cold-boots with a store.
	NoMmdsStore         bool   `json:"no_mmds_store,omitempty"`
	EnvdAccessTokenHash string `json:"envd_access_token_hash,omitempty"`
}

// guestOSKey is the metadata key of GuestOS. retiredGuestOSKey is the spelling
// a pre-release writer used; the decoder ignores it, which would serve such a
// build under the default profile, so a document carrying it is refused instead.
const (
	guestOSKey        = "guest_os"
	retiredGuestOSKey = "guestOS"
)

// guestOSKeyIn returns the key under which a metadata document names a guest
// OS with a value other than "" or null (both mean the default), and whether
// that key is the retired spelling, which wins when both are present. Keys
// match case-insensitively, as the struct decoder matches them.
func guestOSKeyIn(data []byte) (key string, raw json.RawMessage, retired bool) {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	for k, v := range fields {
		if string(v) == `""` || string(v) == "null" {
			continue
		}
		switch {
		case strings.EqualFold(k, retiredGuestOSKey):
			return k, v, true
		case strings.EqualFold(k, guestOSKey) && key == "":
			key, raw = k, v
		}
	}

	return key, raw, false
}

// GuestProfile returns the guest profile that claims the template's guest OS.
// It is the one place that turns the attribute into a profile.
func (t Template) GuestProfile() (guest.Profile, error) {
	return guest.Lookup(t.GuestOS)
}

// rootfsFileRE is the shape of a bare file name the sandbox start script can
// take unquoted.
var rootfsFileRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ValidRootfsFile reports whether name is a bare file name the sandbox start
// script can take unquoted: letters, digits, '.', '_' and '-', and neither "."
// nor "..".
func ValidRootfsFile(name string) bool {
	return name != "." && name != ".." && rootfsFileRE.MatchString(name)
}

// WithEnvdToken returns a copy of the template stamped with whether its memory
// image lacks an MMDS store and, if so, the hash of the token its envd holds.
func (t Template) WithEnvdToken(noMmdsStore bool, envdAccessTokenHash string) Template {
	t.NoMmdsStore = noMmdsStore
	t.EnvdAccessTokenHash = ""
	if noMmdsStore {
		t.EnvdAccessTokenHash = envdAccessTokenHash
	}

	return t
}

// WithDeviceNames returns a copy of the template stamped with the device names
// the running VM bakes into its memory snapshot; empty names are the runtime's own.
func (t Template) WithDeviceNames(rootfsDriveID, netIfaceID, rootfsFile string) Template {
	t.RootfsDriveID = rootfsDriveID
	t.NetIfaceID = netIfaceID
	t.RootfsFile = rootfsFile

	return t
}

// IsFilesystemOnly reports whether this snapshot persists only the filesystem
// (no memory snapshot), so resuming it must cold-boot (reboot) from the rootfs.
func (t Template) IsFilesystemOnly() bool {
	return t.FilesystemOnly
}

// MarkFilesystemOnly records whether this snapshot persists only the filesystem.
//
// When marking it filesystem-only, the metadata version is upgraded to at least
// FilesystemOnlyVersion if needed: deserialize() strips every field (including
// filesystem_only) from a snapshot whose version is <= DeprecatedVersion, so a
// snapshot taken from a V1 template would otherwise lose the flag on resume and
// be wrongly memory-resumed — and since the filesystem-only pause uploaded no
// memory snapshot, that resume hard-fails. Clearing the flag never changes the
// version.
func (t Template) MarkFilesystemOnly(filesystemOnly bool) Template {
	t.FilesystemOnly = filesystemOnly
	if filesystemOnly && t.Version < FilesystemOnlyVersion {
		t.Version = FilesystemOnlyVersion
	}

	return t
}

// IsFsQuiesced reports whether a filesystem-only snapshot's rootfs was frozen
// (crash-consistent) at pause.
func (t Template) IsFsQuiesced() bool {
	return t.FsQuiesced
}

// MarkFsQuiesced records whether the rootfs was frozen at pause. It is only set
// true together with FilesystemOnly, whose MarkFilesystemOnly already lifts the
// metadata version to >= FilesystemOnlyVersion; deserialize() fully unmarshals
// those versions, so the flag survives without a further version bump.
func (t Template) MarkFsQuiesced(quiesced bool) Template {
	t.FsQuiesced = quiesced

	return t
}

func V1TemplateVersion() Template {
	return Template{
		Version: 1,
	}
}

// BasedOn derives the metadata of a build that starts FROM another template.
//
// Deliberately does NOT carry CmdlineArgs, CPU templates or VcpuCount: a new build resolves its
// own settings and cold-boots a VM at its own size, which Pause records in its snapshot.
func (t Template) BasedOn(
	ft FromTemplate,
) Template {
	return Template{
		Version:      CurrentVersion,
		Template:     t.Template,
		Context:      t.Context,
		Start:        t.Start,
		FromTemplate: &ft,
		FromImage:    nil,
		GuestOS:      t.GuestOS,
	}
}

func (t Template) NewVersionTemplate(metadata TemplateMetadata) Template {
	return Template{
		Version:          CurrentVersion,
		Template:         metadata,
		Context:          t.Context,
		Start:            t.Start,
		FromTemplate:     t.FromTemplate,
		FromImage:        t.FromImage,
		CmdlineArgs:      t.CmdlineArgs,
		Balloon:          t.Balloon,
		CPUTemplate:      t.CPUTemplate,
		BuildCPUTemplate: t.BuildCPUTemplate,
		VcpuCount:        t.VcpuCount,
		GuestOS:          t.GuestOS,
		RootfsDriveID:    t.RootfsDriveID,
		NetIfaceID:       t.NetIfaceID,
		RootfsFile:       t.RootfsFile,

		NoMmdsStore:         t.NoMmdsStore,
		EnvdAccessTokenHash: t.EnvdAccessTokenHash,
	}
}

func (t Template) SameVersionTemplate(metadata TemplateMetadata) Template {
	return Template{
		Version:          t.Version,
		Template:         metadata,
		Context:          t.Context,
		Start:            t.Start,
		FromTemplate:     t.FromTemplate,
		FromImage:        t.FromImage,
		CmdlineArgs:      t.CmdlineArgs,
		Balloon:          t.Balloon,
		CPUTemplate:      t.CPUTemplate,
		BuildCPUTemplate: t.BuildCPUTemplate,
		VcpuCount:        t.VcpuCount,
		GuestOS:          t.GuestOS,
		RootfsDriveID:    t.RootfsDriveID,
		NetIfaceID:       t.NetIfaceID,
		RootfsFile:       t.RootfsFile,

		NoMmdsStore:         t.NoMmdsStore,
		EnvdAccessTokenHash: t.EnvdAccessTokenHash,
	}
}

// WithBalloon returns a copy of the template stamped with the balloon
// mechanisms the device actually runs.
func (t Template) WithBalloon(reporting, hinting bool) Template {
	t.Balloon = &Balloon{Reporting: reporting, Hinting: hinting}

	return t
}

// WithVcpuCount returns a copy of the template stamped with the vCPUs the VM has. A V1 file keeps only
// its version, so the count is lost there; lifting the version would change the rootfs path a memory
// snapshot was taken with, so a V1 lineage stays unrecorded and resumes by the request's maximum.
func (t Template) WithVcpuCount(vcpus int64) Template {
	t.VcpuCount = vcpus

	return t
}

// WithPrefetch returns a copy of the template with the given prefetch mapping.
func (t Template) WithPrefetch(prefetch *Prefetch) Template {
	return Template{
		Version:            t.Version,
		Template:           t.Template,
		Context:            t.Context,
		Start:              t.Start,
		FromTemplate:       t.FromTemplate,
		FromImage:          t.FromImage,
		Prefetch:           prefetch,
		CmdlineArgs:        t.CmdlineArgs,
		Balloon:            t.Balloon,
		CPUTemplate:        t.CPUTemplate,
		BuildCPUTemplate:   t.BuildCPUTemplate,
		VcpuCount:          t.VcpuCount,
		RunningEnvdVersion: t.RunningEnvdVersion,
		GuestOS:            t.GuestOS,
		RootfsDriveID:      t.RootfsDriveID,
		NetIfaceID:         t.NetIfaceID,
		RootfsFile:         t.RootfsFile,

		NoMmdsStore:         t.NoMmdsStore,
		EnvdAccessTokenHash: t.EnvdAccessTokenHash,
	}
}

// WithRunningEnvdVersion returns a copy of the template stamped with the envd version
// running in its memory snapshot, or "" for the template-baked one.
func (t Template) WithRunningEnvdVersion(v string) Template {
	t.RunningEnvdVersion = v

	return t
}

func (t Template) ToFile(path string) error {
	mr, err := serialize(t)
	if err != nil {
		return err
	}

	err = ioutils.WriteToFileFromReader(path, mr)
	if err != nil {
		return fmt.Errorf("failed to write metadata to file: %w", err)
	}

	return nil
}

func (t Template) ReplaceFile(path string) error {
	mr, err := serialize(t)
	if err != nil {
		return err
	}

	err = ioutils.WriteToFileFromReaderAtomically(path, mr)
	if err != nil {
		return fmt.Errorf("failed to replace metadata file: %w", err)
	}

	return nil
}

func FromFile(path string) (Template, error) {
	return fromFile(path, claimedGuest)
}

// FromFileAnyGuest reads a metadata file without checking that a registered
// guest profile claims its guest OS, for a tool that copies a build whatever its guest.
func FromFileAnyGuest(path string) (Template, error) {
	return fromFile(path, anyGuest)
}

func fromFile(path string, guests guestCheck) (Template, error) {
	f, err := os.Open(path)
	if err != nil {
		return Template{}, fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer f.Close()

	templateMetadata, err := deserializeWith(f, guests)
	if err != nil {
		return Template{}, fmt.Errorf("failed to deserialize metadata: %w", err)
	}

	return templateMetadata, nil
}

func FromBuildID(ctx context.Context, s storage.StorageProvider, buildID string) (Template, error) {
	return fromTemplate(ctx, s, storage.Paths{
		BuildID: buildID,
	}, claimedGuest)
}

// FromBuildIDAnyGuest reads a build's metadata without checking that a
// registered guest profile claims its guest OS, for a tool that copies a build whatever its guest.
func FromBuildIDAnyGuest(ctx context.Context, s storage.StorageProvider, buildID string) (Template, error) {
	return fromTemplate(ctx, s, storage.Paths{
		BuildID: buildID,
	}, anyGuest)
}

func fromTemplate(ctx context.Context, s storage.StorageProvider, paths storage.Paths, guests guestCheck) (Template, error) {
	ctx, span := tracer.Start(ctx, "from template")
	defer span.End()

	obj, err := s.OpenBlob(ctx, paths.Metadata())
	if err != nil {
		return Template{}, fmt.Errorf("error opening object for template metadata: %w", err)
	}

	data, err := storage.GetBlob(ctx, obj)
	if err != nil {
		return Template{}, fmt.Errorf("error reading template metadata from object: %w", err)
	}

	templateMetadata, err := deserializeWith(bytes.NewReader(data), guests)
	if err != nil {
		return Template{}, err
	}

	return templateMetadata, nil
}

// guestCheck is whether a reader refuses a guest OS that no registered profile claims.
type guestCheck bool

const (
	claimedGuest guestCheck = true
	anyGuest     guestCheck = false
)

func deserialize(reader io.Reader) (Template, error) {
	return deserializeWith(reader, claimedGuest)
}

func deserializeWith(reader io.Reader, guests guestCheck) (Template, error) {
	// Read all data into bytes first to avoid double stream read
	data, err := io.ReadAll(reader)
	if err != nil {
		return Template{}, fmt.Errorf("error reading template metadata: %w", err)
	}

	var v Version
	err = json.Unmarshal(data, &v)
	if err != nil {
		return Template{}, fmt.Errorf("error unmarshaling template version: %w", err)
	}

	// Handle deprecated version formats gracefully
	// When any type is used, Go will unmarshal any numeric value as a float64
	if version, ok := v.Version.(float64); !ok || version <= float64(DeprecatedVersion) {
		// A stripped version keeps nothing but its number. A guest OS it
		// dropped would serve the build under the default profile, so a guest OS
		// in either spelling refuses the file.
		if key, raw, _ := guestOSKeyIn(data); key != "" {
			return Template{}, fmt.Errorf("template metadata version %v cannot carry %s %q: it needs version %d or later", v.Version, key, raw, DeprecatedVersion+1)
		}

		return Template{Version: DeprecatedVersion}, nil
	}

	var templateMetadata Template
	err = json.Unmarshal(data, &templateMetadata)
	if err != nil {
		return Template{}, fmt.Errorf("error unmarshaling template metadata: %w", err)
	}

	if key, raw, retired := guestOSKeyIn(data); retired {
		return Template{}, fmt.Errorf("template metadata carries the retired key %s %q: the guest OS is %s", key, raw, guestOSKey)
	}
	if guests == claimedGuest {
		if _, err := templateMetadata.GuestProfile(); err != nil {
			return Template{}, fmt.Errorf("template metadata: %w", err)
		}
	}
	// The name reaches the start script's shell unquoted.
	if templateMetadata.RootfsFile != "" && !ValidRootfsFile(templateMetadata.RootfsFile) {
		return Template{}, fmt.Errorf("template metadata names rootfs file %q, which is not a bare file name of letters, digits, '.', '_' and '-'", templateMetadata.RootfsFile)
	}

	return templateMetadata, nil
}

// serialize serializes a template to a reader for uploading.
func serialize(template Template) (io.Reader, error) {
	marshaled, err := json.Marshal(template)
	if err != nil {
		return nil, fmt.Errorf("error serializing template metadata: %w", err)
	}

	buf := bytes.NewBuffer(marshaled)

	return buf, nil
}
