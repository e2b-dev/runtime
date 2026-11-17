//go:build linux

package finalize

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// Embedded scripts ship verbatim to the guest shell; a parse error fails every build.
func requireScriptParses(t *testing.T, shell, name, src string) {
	t.Helper()

	shellPath, err := exec.LookPath(shell)
	require.NoErrorf(t, err, "%s is required to parse-check the guest script", shell)

	script := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(script, []byte(src), 0o600))

	out, err := exec.CommandContext(t.Context(), shellPath, "-n", script).CombinedOutput()
	require.NoErrorf(t, err, "%s is not valid %s:\n%s", name, shell, out)
}

func TestPackCertBundleCmdIsValidSh(t *testing.T) {
	t.Parallel()

	requireScriptParses(t, "sh", "pack-cert-bundle.sh", packCertBundleCmd)
}

func TestConfigureScriptIsValidBash(t *testing.T) {
	t.Parallel()

	requireScriptParses(t, "bash", "configure.sh", configureScriptFile)
}

// The configuration script runs in a login shell, where /etc/profile.d hooks
// can wrap the cd builtin — RVM's wrapper reads a variable that is unset under
// `set -u`, killing the script. No line of the script may invoke cd.
func TestConfigureScriptInvokesNoCd(t *testing.T) {
	t.Parallel()

	for i, line := range strings.Split(configureScriptFile, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		require.NotRegexpf(t, `(^|[;&|(])\s*cd\b`, line, "configure.sh line %d invokes cd", i+1)
	}
}

func TestSkeletonCopySurvivesLoginShellCdHookUnderNounset(t *testing.T) {
	t.Parallel()

	// Both temp paths embed the literals the test helper rewrites, so a
	// wrong substitution order in runSkeletonCopy mangles them.
	base := t.TempDir()
	skel := filepath.Join(base, "home", "user", "skel")
	home := filepath.Join(base, "home", "user", "home")
	require.NoError(t, os.MkdirAll(filepath.Join(skel, ".config", "sub"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(skel, "-dash"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".profile"), []byte("from-skel"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".config", "sub", "rc"), []byte("nested"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, "-dash", "f"), []byte("dashdir"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".bashrc"), []byte("skel-bashrc"), 0o644))
	// The pre-existing -dash dir forces the merge path: its new file goes
	// through dirname on a leading-dash relative path.
	require.NoError(t, os.MkdirAll(filepath.Join(home, "-dash"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(home, ".bashrc"), []byte("existing"), 0o644))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed under a booby-trapped cd hook + nounset:\n%s", out)

	for path, want := range map[string]string{
		filepath.Join(home, ".profile"):             "from-skel",
		filepath.Join(home, ".config", "sub", "rc"): "nested",
		filepath.Join(home, "-dash", "f"):           "dashdir",
		filepath.Join(home, ".bashrc"):              "existing",
	} {
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, string(got), path)
	}
}

// The old walk cd-ed into /etc/skel, which resolved a symlinked skel dir; the
// find walk must keep doing so.
func TestSkeletonCopyFollowsSymlinkedSkel(t *testing.T) {
	t.Parallel()

	realSkel := t.TempDir()
	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(realSkel, ".profile"), []byte("via-symlink"), 0o644))
	skel := filepath.Join(t.TempDir(), "skel")
	require.NoError(t, os.Symlink(realSkel, skel))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed on a symlinked skel dir:\n%s", out)

	got, err := os.ReadFile(filepath.Join(home, ".profile"))
	require.NoError(t, err, "no file copied through the symlinked skel dir")
	require.Equal(t, "via-symlink", string(got))
}

// A dangling symlink in /home/user fails `-e` but must count as existing:
// copying onto it makes cp fail and abort the whole configure step.
func TestSkeletonCopyKeepsDanglingHomeSymlink(t *testing.T) {
	t.Parallel()

	skel := t.TempDir()
	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".bashrc"), []byte("skel-bashrc"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".profile"), []byte("from-skel"), 0o644))
	target := filepath.Join(home, "missing-target")
	require.NoError(t, os.Symlink(target, filepath.Join(home, ".bashrc")))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed on a dangling symlink in home:\n%s", out)

	got, err := os.Readlink(filepath.Join(home, ".bashrc"))
	require.NoError(t, err, "dangling home symlink was replaced")
	require.Equal(t, target, got)
	content, err := os.ReadFile(filepath.Join(home, ".profile"))
	require.NoError(t, err)
	require.Equal(t, "from-skel", string(content))
}

// A path collision (skel ships a dir where home has a regular file) skips the
// affected files instead of aborting the whole configure step.
func TestSkeletonCopySkipsFilesUnderNonDirParent(t *testing.T) {
	t.Parallel()

	skel := t.TempDir()
	home := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(skel, ".config"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".config", "rc"), []byte("nested"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(skel, "-dash"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(skel, "-dash", "f"), []byte("dashdir"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".profile"), []byte("from-skel"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(home, ".config"), []byte("keep"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(home, "-dash"), []byte("keep-dash"), 0o644))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy aborted on a file/dir path collision:\n%s", out)

	got, err := os.ReadFile(filepath.Join(home, ".config"))
	require.NoError(t, err)
	require.Equal(t, "keep", string(got), "colliding regular file was clobbered")
	got, err = os.ReadFile(filepath.Join(home, "-dash"))
	require.NoError(t, err)
	require.Equal(t, "keep-dash", string(got), "colliding dash-named file was clobbered")
	content, err := os.ReadFile(filepath.Join(home, ".profile"))
	require.NoError(t, err)
	require.Equal(t, "from-skel", string(content))
}

// Restrictive skel dir modes (.ssh, .gnupg) must survive the copy, not be
// widened to the mkdir default.
func TestSkeletonCopyPreservesSkelDirModes(t *testing.T) {
	t.Parallel()

	skel := t.TempDir()
	home := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(skel, ".ssh"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".ssh", "config"), []byte("sshconf"), 0o600))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed:\n%s", out)

	info, err := os.Stat(filepath.Join(home, ".ssh"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), ".ssh mode widened")
	info, err = os.Stat(filepath.Join(home, ".ssh", "config"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), ".ssh/config mode widened")
}

// Symlinked skel dotfiles (distro- or Nix-style) are copied as links, as the
// pre-rewrite `cp -rn` did.
func TestSkeletonCopyCopiesSymlinkedDotfiles(t *testing.T) {
	t.Parallel()

	skel := t.TempDir()
	home := t.TempDir()
	dirTarget := t.TempDir()
	require.NoError(t, os.Symlink("profile-target", filepath.Join(skel, ".profile")))
	require.NoError(t, os.Symlink(dirTarget, filepath.Join(skel, ".config")))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed on a symlinked skel dotfile:\n%s", out)

	got, err := os.Readlink(filepath.Join(home, ".profile"))
	require.NoError(t, err, "symlinked skel dotfile was not copied")
	require.Equal(t, "profile-target", got)
	got, err = os.Readlink(filepath.Join(home, ".config"))
	require.NoError(t, err, "skel symlink to a directory was not copied as a link")
	require.Equal(t, dirTarget, got)
}

// A newline in a skel filename must not split the walk into bogus paths that
// make cp abort the configure step.
func TestSkeletonCopyHandlesNewlineFilenames(t *testing.T) {
	t.Parallel()

	skel := t.TempDir()
	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(skel, "odd\nname"), []byte("odd"), 0o644))
	// A dir name ending in a newline, pre-created in home, forces the parent
	// guard: command substitution would strip the newline and test the wrong path.
	require.NoError(t, os.Mkdir(filepath.Join(skel, "d\n"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(skel, "d\n", "f"), []byte("under-newline-dir"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(home, "d\n"), 0o755))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed on a newline filename:\n%s", out)

	got, err := os.ReadFile(filepath.Join(home, "odd\nname"))
	require.NoError(t, err, "newline-named file was not copied")
	require.Equal(t, "odd", string(got))
	got, err = os.ReadFile(filepath.Join(home, "d\n", "f"))
	require.NoError(t, err, "file under a newline-named dir was not copied")
	require.Equal(t, "under-newline-dir", string(got))
}

// A home entry symlinked to a directory elsewhere must not receive skel files
// through it: the copy would land outside home and the later chown.
func TestSkeletonCopySkipsSymlinkedParent(t *testing.T) {
	t.Parallel()

	skel := t.TempDir()
	home := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(skel, ".config"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".config", "rc"), []byte("nested"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".profile"), []byte("from-skel"), 0o644))
	require.NoError(t, os.Symlink(outside, filepath.Join(home, ".config")))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed on a symlinked home dir:\n%s", out)

	_, err = os.Lstat(filepath.Join(outside, "rc"))
	require.ErrorIs(t, err, os.ErrNotExist, "skel file written through the symlinked home dir")
	link, err := os.Readlink(filepath.Join(home, ".config"))
	require.NoError(t, err, "symlinked home dir was replaced")
	require.Equal(t, outside, link)
	got, err := os.ReadFile(filepath.Join(home, ".profile"))
	require.NoError(t, err)
	require.Equal(t, "from-skel", string(got))
}

// Sockets, fifos and device nodes in /etc/skel must not reach cp: the guest
// cp may be unable to recreate them and would abort the configure step.
func TestSkeletonCopySkipsSpecialFiles(t *testing.T) {
	t.Parallel()

	skel := t.TempDir()
	home := t.TempDir()
	require.NoError(t, syscall.Mkfifo(filepath.Join(skel, "fifo"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".profile"), []byte("from-skel"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(skel, ".cache"), 0o700))
	require.NoError(t, syscall.Mkfifo(filepath.Join(skel, ".cache", "fifo"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(skel, ".cache", "rc"), []byte("nested"), 0o644))

	out, err := runSkeletonCopy(t, skel, home)
	require.NoErrorf(t, err, "skeleton copy failed on a fifo in skel:\n%s", out)

	_, err = os.Lstat(filepath.Join(home, "fifo"))
	require.ErrorIs(t, err, os.ErrNotExist, "fifo was copied")
	_, err = os.Lstat(filepath.Join(home, ".cache", "fifo"))
	require.ErrorIs(t, err, os.ErrNotExist, "fifo under a dir new to home was copied")
	info, err := os.Stat(filepath.Join(home, ".cache"))
	require.NoError(t, err, "dir new to home was not created")
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), ".cache mode widened")
	got, err := os.ReadFile(filepath.Join(home, ".cache", "rc"))
	require.NoError(t, err, "regular file under a dir new to home was not copied")
	require.Equal(t, "nested", string(got))
	content, err := os.ReadFile(filepath.Join(home, ".profile"))
	require.NoError(t, err)
	require.Equal(t, "from-skel", string(content))
}

// runSkeletonCopy executes the embedded skeleton-copy block with skel/home
// remapped, under set -u and an RVM-style booby-trapped cd hook that mirrors
// RVM stable 1.29.12 (scripts/functions/environment:267): an arithmetic read
// of an unset variable, fatal under nounset.
func runSkeletonCopy(t *testing.T, skel, home string) ([]byte, error) {
	t.Helper()

	bashPath, err := exec.LookPath("bash")
	require.NoError(t, err, "bash is required to run the skeleton-copy block")

	block := skeletonCopyBlock(t)
	// /home/user first: a temp path may itself contain "/home/user", and the
	// reverse order would rewrite it inside the substituted skel path.
	block = strings.ReplaceAll(block, "/home/user", home)
	block = strings.ReplaceAll(block, "/etc/skel", skel)

	script := `cd() { (( rvm_bash_nounset == 1 )) && set -o nounset; builtin cd "$@"; }
set -euo pipefail
` + block

	return exec.CommandContext(t.Context(), bashPath, "-c", script).CombinedOutput()
}

// skeletonCopyBlock extracts the `if [ -d /home/user ]` block from the
// embedded configure.sh, tracking if/fi nesting to find its terminator.
func skeletonCopyBlock(t *testing.T) string {
	t.Helper()

	lines := strings.Split(configureScriptFile, "\n")
	start := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "if [ -d /home/user ]") {
			start = i

			break
		}
	}
	require.NotEqual(t, -1, start, "skeleton-copy block not found in configure.sh")
	depth := 0
	for i := start; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		switch {
		case strings.HasPrefix(trimmed, "if "):
			depth++
		case trimmed == "fi":
			depth--
			if depth == 0 {
				return strings.Join(lines[start:i+1], "\n")
			}
		}
	}
	require.Fail(t, "unterminated skeleton-copy block in configure.sh")

	return ""
}
