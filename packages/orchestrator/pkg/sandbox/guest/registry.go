package guest

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// ErrUnclaimed is the error of a guest OS attribute that no registered profile claims.
var ErrUnclaimed = errors.New("no registered guest profile claims the guest OS")

// Registry maps guest OS attribute values to profiles.
type Registry struct {
	profiles map[string]Profile
}

// NewRegistry returns a registry of the default profile and the given profiles.
// It refuses a nil profile, a typed nil included, and a profile that claims the empty name.
// It refuses two profiles that claim one name or one label.
// It refuses an empty label.
func NewRegistry(profiles ...Profile) (*Registry, error) {
	r := newRegistry()
	labels := map[string]bool{DefaultLabel: true}
	for _, p := range profiles {
		if utils.IsNil(p) {
			return nil, errors.New("guest profile is nil")
		}
		name := p.Name()
		if name == DefaultName {
			return nil, errors.New("a guest profile cannot claim the empty guest OS, which the default profile owns")
		}
		if _, ok := r.profiles[name]; ok {
			return nil, fmt.Errorf("two guest profiles claim guest OS %q", name)
		}
		label := p.Label()
		if label == "" {
			return nil, fmt.Errorf("the guest profile of guest OS %q has no label", name)
		}
		if labels[label] {
			return nil, fmt.Errorf("the guest profile of guest OS %q reuses the label %q", name, label)
		}
		labels[label] = true
		r.profiles[name] = p
	}

	return r, nil
}

// Lookup returns the profile that claims name.
func (r *Registry) Lookup(name string) (Profile, error) {
	if p, ok := r.profiles[name]; ok {
		return p, nil
	}

	return nil, fmt.Errorf("%w %q (claimed: %s)", ErrUnclaimed, name, r.claimed())
}

func (r *Registry) claimed() string {
	names := make([]string, 0, len(r.profiles))
	for name := range r.profiles {
		names = append(names, fmt.Sprintf("%q", name))
	}
	slices.Sort(names)

	return strings.Join(names, ", ")
}

func newRegistry() *Registry {
	return &Registry{profiles: map[string]Profile{DefaultName: Default()}}
}

var (
	installed       atomic.Pointer[Registry]
	defaultRegistry = sync.OnceValue(newRegistry)
)

// Install makes r the registry of the process.
// Call Install only at process start and in tests.
// Install refuses a nil registry.
func Install(r *Registry) error {
	if r == nil {
		return errors.New("guest: Install of a nil registry")
	}
	installed.Store(r)

	return nil
}

// Installed returns the registry of the process: the installed one, or the
// registry of the default profile alone until Install is called.
func Installed() *Registry {
	if r := installed.Load(); r != nil {
		return r
	}

	return defaultRegistry()
}

// Lookup returns the profile that claims name in the registry of the process.
func Lookup(name string) (Profile, error) {
	return Installed().Lookup(name)
}
