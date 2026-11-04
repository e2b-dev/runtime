package resumebuild

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveLocal(t *testing.T) {
	t.Parallel()

	t.Run("override returned when present", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		override := filepath.Join(dir, "override.bin")
		if err := os.WriteFile(override, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := resolveLocal(override, filepath.Join(dir, "local"))
		if err != nil || got != override {
			t.Fatalf("got %q, err %v; want %q", got, err, override)
		}
	})

	t.Run("missing override errors", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		if _, err := resolveLocal(filepath.Join(dir, "nope"), filepath.Join(dir, "local")); err == nil {
			t.Fatal("expected error for missing override")
		}
	})

	t.Run("local copy returned when present", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		local := filepath.Join(dir, "local.bin")
		if err := os.WriteFile(local, []byte("y"), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := resolveLocal("", local)
		if err != nil || got != local {
			t.Fatalf("got %q, err %v; want %q", got, err, local)
		}
	})

	t.Run("absent errors (artifacts are not fetched)", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		if _, err := resolveLocal("", filepath.Join(dir, "missing")); err == nil {
			t.Fatal("expected error when artifact absent")
		}
	})
}
