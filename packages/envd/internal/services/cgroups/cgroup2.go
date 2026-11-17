//go:build linux

package cgroups

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type Cgroup2Manager struct {
	cgroupFDs   map[ProcessType]int
	cgroupPaths map[ProcessType]string
	rootPath    string
}

var (
	_ Manager       = (*Cgroup2Manager)(nil)
	_ PathManager   = (*Cgroup2Manager)(nil)
	_ MemoryLimiter = (*Cgroup2Manager)(nil)
)

type cgroup2Config struct {
	rootPath     string
	processTypes map[ProcessType]Cgroup2Config
}

type Cgroup2ManagerOption func(*cgroup2Config)

func WithCgroup2RootSysFSPath(path string) Cgroup2ManagerOption {
	return func(config *cgroup2Config) {
		config.rootPath = path
	}
}

func WithCgroup2ProcessType(processType ProcessType, path string, properties map[string]string) Cgroup2ManagerOption {
	return func(config *cgroup2Config) {
		if config.processTypes == nil {
			config.processTypes = make(map[ProcessType]Cgroup2Config)
		}
		config.processTypes[processType] = Cgroup2Config{Path: path, Properties: properties}
	}
}

type Cgroup2Config struct {
	Path       string
	Properties map[string]string
}

func NewCgroup2Manager(opts ...Cgroup2ManagerOption) (*Cgroup2Manager, error) {
	config := cgroup2Config{
		rootPath: "/sys/fs/cgroup",
	}

	for _, opt := range opts {
		opt(&config)
	}

	// Verify cgroup v2 is available by checking the filesystem type.
	// On cgroup v1, /sys/fs/cgroup is a tmpfs and directories/files can be
	// created freely, causing Cgroup2Manager to "succeed" with invalid fds
	// that the kernel rejects with EBADF on clone3(CLONE_INTO_CGROUP).
	var st unix.Statfs_t
	if err := unix.Statfs(config.rootPath, &st); err != nil {
		return nil, fmt.Errorf("failed to statfs cgroup root %s: %w", config.rootPath, err)
	}
	if st.Type != unix.CGROUP2_SUPER_MAGIC {
		return nil, fmt.Errorf("cgroup root %s is not a cgroup2 filesystem (type=0x%x)", config.rootPath, st.Type)
	}

	cgroupFDs, cgroupPaths, err := createCgroups(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cgroups: %w", err)
	}

	return &Cgroup2Manager{cgroupFDs: cgroupFDs, cgroupPaths: cgroupPaths, rootPath: config.rootPath}, nil
}

func createCgroups(configs cgroup2Config) (map[ProcessType]int, map[ProcessType]string, error) {
	var (
		fdResults   = make(map[ProcessType]int)
		pathResults = make(map[ProcessType]string)
		errs        []error
	)

	for procType, config := range configs.processTypes {
		fullPath := filepath.Join(configs.rootPath, config.Path)
		fd, err := createCgroup(fullPath, config.Properties)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to create %s cgroup: %w", procType, err))

			continue
		}
		fdResults[procType] = fd
		pathResults[procType] = fullPath
	}

	if len(errs) > 0 {
		for procType, fd := range fdResults {
			err := unix.Close(fd)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to close cgroup fd for %s: %w", procType, err))
			}
		}

		return nil, nil, errors.Join(errs...)
	}

	return fdResults, pathResults, nil
}

// writeCgroupProp writes a cgroupfs property without O_CREATE so missing
// properties error out rather than being silently created on a tmpfs fallback.
func writeCgroupProp(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(value)

	return err
}

func createCgroup(fullPath string, properties map[string]string) (int, error) {
	if err := os.MkdirAll(fullPath, 0o755); err != nil {
		return -1, fmt.Errorf("failed to create cgroup root: %w", err)
	}

	var errs []error
	for name, value := range properties {
		if err := writeCgroupProp(filepath.Join(fullPath, name), value); err != nil {
			// Skip properties whose controller isn't enabled in subtree_control.
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
				fmt.Fprintf(os.Stderr, "cgroup property %q unavailable at %q, skipping\n", name, fullPath)

				continue
			}
			errs = append(errs, fmt.Errorf("failed to write cgroup property %q: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return -1, errors.Join(errs...)
	}

	return unix.Open(fullPath, unix.O_RDONLY, 0)
}

func (c Cgroup2Manager) GetFileDescriptor(procType ProcessType) (int, bool) {
	fd, ok := c.cgroupFDs[procType]

	return fd, ok
}

// Root is the cgroup2 mount point, cleaned. Every caller that classifies a walked path does so
// by relative position under this root -- the audit's allowlist, the thaw's guest-frozen record,
// the sweep's own bookkeeping -- and filepath.Rel is textual: a configured root with a trailing
// slash or a "." segment yields a relative path that matches nothing, so the classification
// silently reports everything as unknown. Cleaning here keeps that from depending on how the
// caller happened to spell the mount point.
func (c Cgroup2Manager) Root() string { return filepath.Clean(c.rootPath) }

// PathOf reports where a ProcessType's cgroup lives, cleaned so it compares equal to the paths
// a walk builds with filepath.Join.
func (c Cgroup2Manager) PathOf(procType ProcessType) (string, bool) {
	path, ok := c.cgroupPaths[procType]
	if !ok {
		return "", false
	}

	return filepath.Clean(path), true
}

// ChildrenOf lists immediate child cgroups. A cgroup with no children is not an error:
// the walk visits leaves routinely.
func (c Cgroup2Manager) ChildrenOf(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	children := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			children = append(children, filepath.Join(path, e.Name()))
		}
	}

	return children, nil
}

func (c Cgroup2Manager) FreezeAt(path string) error {
	return writeCgroupProp(filepath.Join(path, "cgroup.freeze"), "1")
}

func (c Cgroup2Manager) UnfreezeAt(path string) error {
	return writeCgroupProp(filepath.Join(path, "cgroup.freeze"), "0")
}

// FrozenAt reads the settled state from cgroup.events, like Frozen does for a
// ProcessType: 1 only once the tasks have stopped.
func (c Cgroup2Manager) FrozenAt(path string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
	if err != nil {
		return false, err
	}

	return frozenFromEvents(b), nil
}

// FreezeRequestedAt reads cgroup.freeze -- this cgroup's own requested state -- rather
// than cgroup.events, which also reports frozen=1 when only an ancestor was frozen.
func (c Cgroup2Manager) FreezeRequestedAt(path string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(path, "cgroup.freeze"))
	if err != nil {
		return false, err
	}

	return strings.TrimSpace(string(b)) == "1", nil
}

func (c Cgroup2Manager) Freeze(procType ProcessType) error {
	return c.setFreezeState(procType, "1")
}

func (c Cgroup2Manager) Unfreeze(procType ProcessType) error {
	return c.setFreezeState(procType, "0")
}

// Frozen reads the "frozen" field of the cgroup's cgroup.events. The file lists one
// "key value" pair per line; absence of the key is reported as not frozen, which is
// what a cgroup that has never been frozen looks like.
func (c Cgroup2Manager) Frozen(procType ProcessType) (bool, error) {
	path, ok := c.cgroupPaths[procType]
	if !ok {
		return false, fmt.Errorf("unknown process type: %s", procType)
	}

	b, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
	if err != nil {
		return false, err
	}

	return frozenFromEvents(b), nil
}

// frozenFromEvents pulls the "frozen" field out of a cgroup.events body. The file lists
// one "key value" pair per line; absence of the key is reported as not frozen, which is
// what a cgroup that has never been frozen looks like.
func frozenFromEvents(b []byte) bool {
	for line := range strings.SplitSeq(string(b), "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), " ")
		if found && key == "frozen" {
			return value == "1"
		}
	}

	return false
}

func (c Cgroup2Manager) setFreezeState(procType ProcessType, value string) error {
	path, ok := c.cgroupPaths[procType]
	if !ok {
		return fmt.Errorf("unknown process type: %s", procType)
	}

	return writeCgroupProp(filepath.Join(path, "cgroup.freeze"), value)
}

// SetMemoryLimit writes limit to memory.high and then memory.max, so that lowering it
// reclaims under memory.high before memory.max can OOM-kill.
func (c Cgroup2Manager) SetMemoryLimit(procType ProcessType, limit uint64) error {
	path, ok := c.cgroupPaths[procType]
	if !ok {
		return fmt.Errorf("unknown process type: %s", procType)
	}

	value := strconv.FormatUint(limit, 10)
	for _, name := range []string{"memory.high", "memory.max"} {
		if err := writeCgroupProp(filepath.Join(path, name), value); err != nil {
			return fmt.Errorf("failed to write %s for %s: %w", name, procType, err)
		}
	}

	return nil
}

func (c Cgroup2Manager) Close() error {
	var errs []error
	for procType, fd := range c.cgroupFDs {
		if err := unix.Close(fd); err != nil {
			errs = append(errs, fmt.Errorf("failed to close cgroup fd for %s: %w", procType, err))
		}
		delete(c.cgroupFDs, procType)
	}

	return errors.Join(errs...)
}
