package mtls

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// AllowList is the set of SPIFFE IDs a listener admits, or the names a client
// accepts from a server. Entries are full IDs; the only wildcard is a trailing
// * in the ServiceAccount segment:
//
//	spiffe://cluster-a.example.internal/ns/platform/sa/worker-*
//
// A nil *AllowList admits nobody. Replace swaps in a new list atomically and
// keeps the current one when the new one does not parse.
type AllowList struct {
	set atomic.Pointer[allowSet]
	// wildcards is the parse option the list was made with; Replace keeps it.
	wildcards bool
}

// allowSet is one immutable parsed list.
type allowSet struct {
	entries  []string
	exact    map[string]struct{}
	prefixes []string
}

// AllowListOption configures parsing.
type AllowListOption func(*allowListOptions)

type allowListOptions struct {
	wildcards bool
}

// WithoutWildcards refuses entries with a trailing *, for ports whose callers
// must be named one by one.
func WithoutWildcards() AllowListOption {
	return func(o *allowListOptions) { o.wildcards = false }
}

// ParseAllowList validates every entry. Blank entries are skipped. An empty
// result is a valid list that admits nobody: a listener asked to run
// required with one serves permissive and logs it, and FlagModeSource
// refuses the flip at the flag.
func ParseAllowList(entries []string, opts ...AllowListOption) (*AllowList, error) {
	options := allowListOptions{wildcards: true}
	for _, o := range opts {
		o(&options)
	}
	set, err := parseAllowSet(entries, options.wildcards)
	if err != nil {
		return nil, err
	}

	list := &AllowList{wildcards: options.wildcards}
	list.set.Store(set)

	return list, nil
}

// Replace parses entries under the options the list was made with and swaps
// them in. On error the current list stays.
func (a *AllowList) Replace(entries []string) error {
	set, err := parseAllowSet(entries, a.wildcards)
	if err != nil {
		return err
	}

	a.set.Store(set)

	return nil
}

// Allows reports whether id, in the canonical form PeerID returns, is on the list.
func (a *AllowList) Allows(id string) bool {
	return a.snapshot().allows(id)
}

// Len is the number of entries.
func (a *AllowList) Len() int {
	return len(a.snapshot().entries)
}

// Entries returns the canonical entries, wildcards with their trailing *.
func (a *AllowList) Entries() []string {
	s := a.snapshot()
	out := make([]string, len(s.entries))
	copy(out, s.entries)

	return out
}

// snapshot returns the current set, or an empty one for a nil list.
func (a *AllowList) snapshot() *allowSet {
	if a == nil {
		return &allowSet{}
	}
	if s := a.set.Load(); s != nil {
		return s
	}

	return &allowSet{}
}

func (s *allowSet) allows(id string) bool {
	if s == nil || id == "" {
		return false
	}
	if _, ok := s.exact[id]; ok {
		return true
	}
	// A wildcard completes the ServiceAccount name: what it stands for cannot
	// run into another path segment.
	for _, prefix := range s.prefixes {
		if rest, ok := strings.CutPrefix(id, prefix); ok && !strings.Contains(rest, "/") {
			return true
		}
	}

	return false
}

func parseAllowSet(entries []string, wildcards bool) (*allowSet, error) {
	set := &allowSet{exact: make(map[string]struct{}, len(entries))}
	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}

		id, wildcard, err := parseAllowEntry(entry)
		if err != nil {
			return nil, err
		}

		if wildcard {
			if !wildcards {
				return nil, fmt.Errorf("%w: %q: wildcards are not allowed on this listener", ErrInvalidAllowList, raw)
			}
			set.prefixes = append(set.prefixes, id)
			set.entries = append(set.entries, id+"*")

			continue
		}

		set.exact[id] = struct{}{}
		set.entries = append(set.entries, id)
	}

	return set, nil
}

// parseAllowEntry returns the canonical ID, without the trailing * for a
// wildcard, and whether the entry is a wildcard.
func parseAllowEntry(entry string) (string, bool, error) {
	wildcard := strings.HasSuffix(entry, "*")
	candidate := strings.TrimSuffix(entry, "*")
	if strings.Contains(candidate, "*") {
		return "", false, fmt.Errorf("%w: %q: * is only allowed at the end of the ServiceAccount segment", ErrInvalidAllowList, entry)
	}
	// url.Parse would decode an escape and drop an empty query or fragment,
	// storing an ID spelled differently from the entry; none of the three
	// can appear in a SPIFFE ID, so the entry is refused as written.
	if strings.ContainsAny(candidate, "%?#") {
		return "", false, fmt.Errorf("%w: %q: a SPIFFE ID has only a trust domain and a path", ErrInvalidAllowList, entry)
	}

	u, err := url.Parse(candidate)
	if err != nil {
		return "", false, fmt.Errorf("%w: %q: %w", ErrInvalidAllowList, entry, err)
	}
	if !wildcard {
		id, err := parseSPIFFEURL(u)
		if err != nil {
			return "", false, fmt.Errorf("%w: %q: %w", ErrInvalidAllowList, entry, err)
		}

		return id, false, nil
	}

	if !inServiceAccountSegment(u.Path) {
		return "", false, fmt.Errorf("%w: %q: a wildcard must end the ServiceAccount segment, as in /ns/<namespace>/sa/<prefix>*", ErrInvalidAllowList, entry)
	}
	// A segment may not be empty, so the prefix is validated with a stand-in
	// character where the wildcard was, then stored without it.
	const standIn = "x"
	probe := *u
	probe.Path += standIn
	id, err := parseSPIFFEURL(&probe)
	if err != nil {
		return "", false, fmt.Errorf("%w: %q: %w", ErrInvalidAllowList, entry, err)
	}

	return strings.TrimSuffix(id, standIn), true, nil
}

// inServiceAccountSegment reports whether path is /ns/<namespace>/sa/<rest>,
// where rest may be empty.
func inServiceAccountSegment(path string) bool {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	return len(segments) == 4 && segments[0] == "ns" && segments[1] != "" && segments[2] == "sa"
}

// parseSPIFFEURL returns the canonical form of a SPIFFE ID given as a URL:
// the spiffe scheme, a trust domain lowercased here (go-spiffe accepts only
// lowercase), a non-empty path, and nothing else; the trust-domain and path
// character rules, dot and empty segments and trailing slashes are go-spiffe's.
func parseSPIFFEURL(u *url.URL) (string, error) {
	switch {
	case u.Scheme != "spiffe":
		return "", errors.New("scheme must be spiffe")
	case u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Host != u.Hostname():
		return "", errors.New("a SPIFFE ID has only a trust domain and a path")
	case u.Hostname() == "":
		return "", errors.New("trust domain is empty")
	case u.Path == "" || u.Path == "/":
		return "", errors.New("path is empty")
	}

	id, err := spiffeid.FromString("spiffe://" + strings.ToLower(u.Hostname()) + u.Path)
	if err != nil {
		return "", err
	}

	return id.String(), nil
}
