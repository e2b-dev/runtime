package utils

import "sync"

// EnvVars distinguishes system-set ("internal") entries from user-provided
// ones; internal entries cannot be touched by the user-facing methods.
type EnvVars struct {
	mu sync.RWMutex
	m  map[string]envVarEntry
}

type envVarEntry struct {
	value    string
	internal bool
}

func NewEnvVars() *EnvVars {
	return &EnvVars{m: make(map[string]envVarEntry)}
}

// Store sets an internal entry, overwriting any existing one.
func (e *EnvVars) Store(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.m[key] = envVarEntry{value: value, internal: true}
}

// StoreUser sets a user entry; returns false if an internal entry exists.
func (e *EnvVars) StoreUser(key, value string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, ok := e.m[key]; ok && existing.internal {
		return false
	}
	e.m[key] = envVarEntry{value: value}

	return true
}

func (e *EnvVars) Load(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.m[key]
	if !ok {
		return "", false
	}

	return v.value, true
}

// All returns a snapshot of all entries as a plain map.
func (e *EnvVars) All() map[string]string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string]string, len(e.m))
	for k, v := range e.m {
		out[k] = v.value
	}

	return out
}

// UserVars returns a snapshot of the user entries only.
func (e *EnvVars) UserVars() map[string]string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string]string)
	for k, v := range e.m {
		if !v.internal {
			out[k] = v.value
		}
	}

	return out
}

// ReplaceUserVars replaces all user entries with newVars; internal entries
// are left untouched even if they appear in newVars.
func (e *EnvVars) ReplaceUserVars(newVars map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for k, v := range e.m {
		if v.internal {
			continue
		}
		if _, keep := newVars[k]; !keep {
			delete(e.m, k)
		}
	}
	for k, v := range newVars {
		if existing, ok := e.m[k]; ok && existing.internal {
			continue
		}
		e.m[k] = envVarEntry{value: v}
	}
}
