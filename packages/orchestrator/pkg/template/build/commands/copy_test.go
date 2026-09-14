//go:build linux

package commands

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestEnvironment creates a temporary directory structure for testing
func setupTestEnvironment(t *testing.T) (sourceDir, targetBaseDir, workDir string) {
	t.Helper()
	tmpBase := t.TempDir()
	sourceDir = filepath.Join(tmpBase, "source")
	targetBaseDir = filepath.Join(tmpBase, "target")

	require.NoError(t, os.MkdirAll(sourceDir, 0o755))
	require.NoError(t, os.MkdirAll(targetBaseDir, 0o755))

	return sourceDir, targetBaseDir, workDir
}

// executeScript runs the generated bash script and returns the result
func executeScript(t *testing.T, script string, workDir string) (stdout, stderr string, exitCode int) {
	t.Helper()
	scriptFile := filepath.Join(workDir, "test_script.sh")
	err := os.WriteFile(scriptFile, []byte(script), 0o755)
	defer os.Remove(scriptFile)

	require.NoError(t, err, "Failed to write script file")

	cmd := exec.CommandContext(t.Context(), "/bin/bash", scriptFile)
	cmd.Dir = workDir

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err = cmd.Run()
	stdout = outBuf.String()
	stderr = errBuf.String()

	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatalf("Failed to execute script: %v", err)
		}
	} else {
		exitCode = 0
	}

	return stdout, stderr, exitCode
}

// getCurrentUser returns current user and group for testing
func getCurrentUser() (uid, gid int) {
	uid = os.Getuid()
	gid = os.Getgid()

	return uid, gid
}

// getFilePermissions returns the permission bits of a file
func getFilePermissions(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err, "Failed to stat file")

	return info.Mode().Perm()
}

// renderTemplate is a helper to render the template
func renderTemplate(t *testing.T, data copyScriptData) string {
	t.Helper()
	var buf bytes.Buffer
	err := copyScriptTemplate.Execute(&buf, data)
	require.NoError(t, err, "Template execution should not fail")

	return buf.String()
}

// createFilesAndDirs creates files, directories, and symlinks from a map
// Values: "file", "dir", "symlink"
func createFilesAndDirs(t *testing.T, baseDir string, paths map[string]string) {
	t.Helper()
	createFilesAndDirsWithContent(t, baseDir, paths, "dummy")
}

// createFilesAndDirsWithContent is createFilesAndDirs with custom file content
func createFilesAndDirsWithContent(t *testing.T, baseDir string, paths map[string]string, content string) {
	t.Helper()
	for path, entryType := range paths {
		fullPath := filepath.Join(baseDir, path)

		switch entryType {
		case "dir":
			require.NoError(t, os.MkdirAll(fullPath, 0o755))
		case "file":
			// Ensure parent dir exists
			dir := filepath.Dir(fullPath)
			require.NoError(t, os.MkdirAll(dir, 0o755))
			require.NoError(t, os.WriteFile(fullPath, []byte(content), 0o644))
		case "symlink":
			// Create symlink target outside the tree
			dir := filepath.Dir(fullPath)
			require.NoError(t, os.MkdirAll(dir, 0o755))
			targetFile := filepath.Join(baseDir, "..", "symlink_target_"+filepath.Base(path))
			require.NoError(t, os.WriteFile(targetFile, []byte("symlink target"), 0o644))
			require.NoError(t, os.Symlink(targetFile, fullPath))
		default:
			// "symlink:<target>" creates a symlink pointing at the given path
			if target, ok := strings.CutPrefix(entryType, "symlink:"); ok {
				require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
				require.NoError(t, os.Symlink(target, fullPath))

				continue
			}
			t.Fatalf("Unknown entry type: %s", entryType)
		}
	}
}

// verifyFilesAndDirs verifies files, directories, and symlinks exist
func verifyFilesAndDirs(t *testing.T, baseDir string, paths map[string]string) {
	t.Helper()
	for path, entryType := range paths {
		fullPath := filepath.Join(baseDir, path)

		switch entryType {
		case "dir":
			assert.DirExists(t, fullPath, "Directory %s should exist", path)
		case "file":
			assert.FileExists(t, fullPath, "File %s should exist", path)
		case "symlink":
			info, err := os.Lstat(fullPath)
			require.NoError(t, err, "Symlink %s should exist", path)
			assert.Equal(t, os.ModeSymlink, info.Mode()&os.ModeSymlink, "%s should be a symlink", path)
		case "regular":
			// A regular file, NOT a symlink pointing at one
			info, err := os.Lstat(fullPath)
			require.NoError(t, err, "File %s should exist", path)
			assert.True(t, info.Mode().IsRegular(), "%s should be a regular file, got %s", path, info.Mode())
		default:
			t.Fatalf("Unknown entry type: %s", entryType)
		}
	}
}

// testCase defines a comprehensive test scenario
type testCase struct {
	name        string
	description string

	// Setup: map of paths to their types
	// Types: "file", "dir", "symlink"
	// Example: {"app/": "dir", "app/main.js": "file", "link": "symlink"}
	files map[string]string

	// Setup: paths pre-created in the target before the copy runs,
	// to simulate copying over an existing filesystem tree
	preexistingTargetPaths map[string]string

	// Input: the path within the extracted files to copy from
	// Examples: "." (root), "app/" (subdirectory), "src/main.js" (specific file)
	copyFrom string

	// Input: where to copy to (relative or absolute)
	copyTo string

	// Optional: permissions to apply
	permissions string

	// Optional: owner (will be set to current user:group if empty)
	owner string

	// Expected results
	shouldSucceed    bool
	expectedExitCode int
	expectedError    string // substring to look for in stderr

	// Verification: what paths to check in the target with their types
	expectedPaths map[string]string

	// Verification: paths that must NOT exist in the target
	absentPaths []string

	// Verification: file contents to check in the target
	expectedContents map[string]string

	// Verification: octal permissions to check in the target
	expectedPerms map[string]string
}

func TestParseCopyArgs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		args          []string
		defaultUser   string
		expected      *copyArgs
		expectedError string
	}{
		{
			name:        "minimum_valid_arguments",
			args:        []string{"/local/path", "/container/path"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/local/path",
				TargetPath:  "/container/path",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "with_owner_specified",
			args:        []string{"/local/path", "/container/path", "root"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/local/path",
				TargetPath:  "/container/path",
				Owner:       "root:root",
				Permissions: "",
			},
		},
		{
			name:        "owner_with_group",
			args:        []string{"/local/path", "/container/path", "www-data:www-data"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/local/path",
				TargetPath:  "/container/path",
				Owner:       "www-data:www-data",
				Permissions: "",
			},
		},
		{
			name:        "owner_with_different_group",
			args:        []string{"/local/path", "/container/path", "user:staff"},
			defaultUser: "defaultuser",
			expected: &copyArgs{
				SourcePath:  "/local/path",
				TargetPath:  "/container/path",
				Owner:       "user:staff",
				Permissions: "",
			},
		},
		{
			name:        "empty_owner_uses_default",
			args:        []string{"/local/path", "/container/path", ""},
			defaultUser: "ubuntu",
			expected: &copyArgs{
				SourcePath:  "/local/path",
				TargetPath:  "/container/path",
				Owner:       "ubuntu:ubuntu",
				Permissions: "",
			},
		},
		{
			name:        "with_permissions_755",
			args:        []string{"/local/path", "/container/path", "root", "755"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/local/path",
				TargetPath:  "/container/path",
				Owner:       "root:root",
				Permissions: "755",
			},
		},
		{
			name:        "with_permissions_644",
			args:        []string{"/local/path", "/container/path", "www-data:www-data", "644"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/local/path",
				TargetPath:  "/container/path",
				Owner:       "www-data:www-data",
				Permissions: "644",
			},
		},
		{
			name:        "glob_pattern_single_asterisk",
			args:        []string{"/app/*.js", "/dest/"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/app",
				TargetPath:  "/dest/",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "glob_pattern_double_asterisk",
			args:        []string{"/app/**/*.ts", "/dest/"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/app",
				TargetPath:  "/dest/",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "glob_pattern_question_mark",
			args:        []string{"/app/file?.txt", "/dest/"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/app",
				TargetPath:  "/dest/",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "glob_pattern_brackets",
			args:        []string{"/app/[abc].txt", "/dest/"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/app",
				TargetPath:  "/dest/",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "no_glob_pattern_with_trailing_slash",
			args:        []string{"/app/src/", "/dest/"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/app/src",
				TargetPath:  "/dest/",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "no_glob_pattern_without_trailing_slash",
			args:        []string{"/app/src", "/dest/"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/app/src",
				TargetPath:  "/dest/",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "relative_paths",
			args:        []string{"./local/path", "container/path"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "./local/path",
				TargetPath:  "container/path",
				Owner:       "user:user",
				Permissions: "",
			},
		},
		{
			name:        "complex_glob_at_end",
			args:        []string{"/src/components/**/*.{ts,tsx}", "/dest/"},
			defaultUser: "developer",
			expected: &copyArgs{
				SourcePath:  "/src/components",
				TargetPath:  "/dest/",
				Owner:       "developer:developer",
				Permissions: "",
			},
		},
		{
			name:        "all_arguments_provided",
			args:        []string{"/source/*.go", "/target/", "admin:sudo", "700"},
			defaultUser: "user",
			expected: &copyArgs{
				SourcePath:  "/source",
				TargetPath:  "/target/",
				Owner:       "admin:sudo",
				Permissions: "700",
			},
		},
		{
			name:          "error_no_arguments",
			args:          []string{},
			defaultUser:   "user",
			expectedError: "COPY requires a local path and a container path argument",
		},
		{
			name:          "error_one_argument",
			args:          []string{"/local/path"},
			defaultUser:   "user",
			expectedError: "COPY requires a local path and a container path argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := parseCopyArgs(tt.args, tt.defaultUser)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.SourcePath, result.SourcePath, "SourcePath mismatch")
				assert.Equal(t, tt.expected.TargetPath, result.TargetPath, "TargetPath mismatch")
				assert.Equal(t, tt.expected.Owner, result.Owner, "Owner mismatch")
				assert.Equal(t, tt.expected.Permissions, result.Permissions, "Permissions mismatch")
			}
		})
	}
}

func TestCopyScriptBehavior(t *testing.T) { //nolint:paralleltest // no idea why this one doesn't work, but it doesn't
	uid, gid := getCurrentUser()
	currentUser := fmt.Sprintf("%d:%d", uid, gid)

	tests := []testCase{
		{
			name:        "single_file_root_level",
			description: "Single file at root, copied to target file path",
			files: map[string]string{
				"test.txt": "file",
			},
			copyFrom:      "test.txt",
			copyTo:        "dest/file.txt",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/file.txt": "file",
			},
		},
		{
			name:        "single_file_to_new_nested_directory_trailing_slash",
			description: "COPY file /a/b/c/ must create the directory chain and place the file inside (Docker semantics)",
			files: map[string]string{
				"test.sh": "file",
			},
			copyFrom:      "test.sh",
			copyTo:        "a/b/c/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"a/b/c/test.sh": "file",
			},
		},
		{
			name:        "single_file_to_preexisting_directory_no_slash",
			description: "COPY file /existing-dir (no trailing slash, dir exists) places the file inside it",
			files: map[string]string{
				"test.sh": "file",
			},
			preexistingTargetPaths: map[string]string{
				"dest/": "dir",
			},
			copyFrom:      "test.sh",
			copyTo:        "dest",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/test.sh": "file",
			},
		},
		{
			name:        "symlink_to_new_directory_trailing_slash",
			description: "COPY symlink /newdir/ must create the directory and place the symlink inside (Docker semantics)",
			files: map[string]string{
				"link.txt": "symlink",
			},
			copyFrom:      "link.txt",
			copyTo:        "newdir/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"newdir/link.txt": "symlink",
			},
		},
		{
			name:        "multiple_files_root_level",
			description: "Multiple files at root, copied to target directory",
			files: map[string]string{
				"file1.txt": "file",
				"file2.txt": "file",
			},
			copyFrom:      ".",
			copyTo:        "dest/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/file1.txt": "file",
				"dest/file2.txt": "file",
			},
		},
		{
			name:        "directory",
			description: "Nested app/ directory with subdirectories",
			files: map[string]string{
				"app/":                    "dir",
				"app/main.js":             "file",
				"app/src/":                "dir",
				"app/src/index.js":        "file",
				"app/src/utils/":          "dir",
				"app/src/utils/helper.js": "file",
			},
			copyFrom:      ".",
			copyTo:        "work/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"work/app/main.js":             "file",
				"work/app/src/":                "dir",
				"work/app/src/index.js":        "file",
				"work/app/src/utils/":          "dir",
				"work/app/src/utils/helper.js": "file",
			},
		},
		{
			name:        "nested_directory_structure_to_target",
			description: "Nested app/ directory with subdirectories",
			files: map[string]string{
				"app/":                    "dir",
				"app/main.js":             "file",
				"app/src/":                "dir",
				"app/src/index.js":        "file",
				"app/src/utils/":          "dir",
				"app/src/utils/helper.js": "file",
			},
			copyFrom:      "app/",
			copyTo:        "app/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"app/main.js":             "file",
				"app/src/":                "dir",
				"app/src/index.js":        "file",
				"app/src/utils/":          "dir",
				"app/src/utils/helper.js": "file",
			},
		},
		{
			name:        "copy_from_deeply_nested_subfolder",
			description: "Copy from a deeply nested subfolder within source",
			files: map[string]string{
				"project/":                          "dir",
				"project/src/":                      "dir",
				"project/src/components/":           "dir",
				"project/src/components/Button.tsx": "file",
				"project/src/components/Input.tsx":  "file",
				"project/src/utils/":                "dir",
				"project/src/utils/helpers.ts":      "file",
			},
			copyFrom:      "project/src/components/",
			copyTo:        ".",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"Button.tsx": "file",
				"Input.tsx":  "file",
			},
		},
		{
			name:        "copy_from_nested_folder_with_subdirs",
			description: "Copy from nested folder that itself contains subdirectories",
			files: map[string]string{
				"project/":                         "dir",
				"project/tests/":                   "dir",
				"project/tests/unit/":              "dir",
				"project/tests/unit/test1.ts":      "file",
				"project/tests/unit/test2.ts":      "file",
				"project/tests/integration/":       "dir",
				"project/tests/integration/api.ts": "file",
			},
			copyFrom:      "project/tests/",
			copyTo:        "test-suite/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"test-suite/unit/":              "dir",
				"test-suite/unit/test1.ts":      "file",
				"test-suite/unit/test2.ts":      "file",
				"test-suite/integration/":       "dir",
				"test-suite/integration/api.ts": "file",
			},
		},
		{
			name:        "with_permissions_755",
			description: "File copied with 755 permissions",
			files: map[string]string{
				"script.sh": "file",
			},
			copyFrom:      "script.sh",
			copyTo:        "/dest/script.sh",
			permissions:   "755",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/script.sh": "file",
			},
		},
		{
			name:        "with_permissions_644",
			description: "File copied with 644 permissions",
			files: map[string]string{
				"readme.md": "file",
			},
			copyFrom:      "readme.md",
			copyTo:        "/dest/readme.md",
			permissions:   "644",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/readme.md": "file",
			},
		},
		{
			name:        "directory_with_permissions_recursive",
			description: "Directory contents get permissions applied recursively",
			files: map[string]string{
				"app/":          "dir",
				"app/file1.txt": "file",
				"app/file2.txt": "file",
			},
			copyFrom:      "app/",
			copyTo:        "/dest/",
			permissions:   "700",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/file1.txt": "file",
				"dest/file2.txt": "file",
			},
		},
		{
			name:        "symlink_preservation",
			description: "Symlinks should be preserved as symlinks",
			files: map[string]string{
				"link.txt": "symlink",
			},
			copyFrom:      "link.txt",
			copyTo:        "/dest/link.txt",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/link.txt": "symlink",
			},
		},
		{
			name:        "nested_folder_with_symlinks",
			description: "Nested folders containing symlinks",
			files: map[string]string{
				"app/":            "dir",
				"app/config.json": "file",
				"app/link":        "symlink",
				"app/data/":       "dir",
				"app/data/file":   "file",
				"app/data/link2":  "symlink",
			},
			copyFrom:      "app/",
			copyTo:        "/dest/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/config.json": "file",
				"dest/link":        "symlink",
				"dest/data/":       "dir",
				"dest/data/file":   "file",
				"dest/data/link2":  "symlink",
			},
		},
		{
			name:        "hidden_files_and_directories",
			description: "Hidden files (.dotfiles) should be moved",
			files: map[string]string{
				"app/":                   "dir",
				"app/visible.txt":        "file",
				"app/.dotfile":           "file",
				"app/.hidden/":           "dir",
				"app/.hidden/nested.txt": "file",
			},
			copyFrom:      "app/",
			copyTo:        "/dest/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/visible.txt":        "file",
				"dest/.dotfile":           "file",
				"dest/.hidden/":           "dir",
				"dest/.hidden/nested.txt": "file",
			},
		},
		{
			name:        "deeply_nested_file",
			description: "Deeply nested file should be copied correctly",
			files: map[string]string{
				"app/l1/l2/l3/l4/l5/deep.txt": "file",
			},
			copyFrom:      "app/l1/l2/l3/l4/l5/deep.txt",
			copyTo:        ".",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"deep.txt": "file",
			},
		},
		{
			name:        "merge_directory_into_existing_tree",
			description: "COPY rootfs/etc /etc: merge into a target directory that already has content",
			files: map[string]string{
				"etc/motd":                  "file",
				"etc/profile.d/test-env.sh": "file",
			},
			copyFrom: "etc/",
			copyTo:   "etc/",
			preexistingTargetPaths: map[string]string{
				"etc/profile.d/existing.sh": "file",
			},
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"etc/motd":                  "file",
				"etc/profile.d/test-env.sh": "file",
				"etc/profile.d/existing.sh": "file",
			},
		},
		{
			name:        "merge_root_directory_into_existing_root",
			description: "COPY rootfs/ /: every top-level dir already exists in the target root",
			files: map[string]string{
				"rootfs/etc/profile.d/test-env.sh": "file",
				"rootfs/usr/local/bin/tool":        "file",
			},
			copyFrom: "rootfs/",
			copyTo:   ".",
			preexistingTargetPaths: map[string]string{
				"etc/profile.d/00-existing.sh": "file",
				"usr/local/bin/existing-tool":  "file",
			},
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"etc/profile.d/test-env.sh":    "file",
				"etc/profile.d/00-existing.sh": "file",
				"usr/local/bin/tool":           "file",
				"usr/local/bin/existing-tool":  "file",
			},
		},
		{
			name:        "overwrite_existing_file_in_target",
			description: "Files that already exist in the target are overwritten",
			files: map[string]string{
				"etc/motd": "file",
			},
			copyFrom: "etc/",
			copyTo:   "etc/",
			preexistingTargetPaths: map[string]string{
				"etc/motd": "file",
			},
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"etc/motd": "file",
			},
			expectedContents: map[string]string{
				"etc/motd": "dummy",
			},
		},
		{
			name:        "preserve_target_directory_metadata",
			description: "Only directory contents are copied; the existing target directory keeps its own permissions",
			files: map[string]string{
				"etc/motd": "file",
			},
			copyFrom: "etc/",
			copyTo:   "etc/",
			// 700 must apply to the copied contents, not the existing target dir
			permissions: "700",
			preexistingTargetPaths: map[string]string{
				"etc/": "dir",
			},
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"etc/motd": "file",
			},
			expectedPerms: map[string]string{
				"etc": "755",
			},
		},
		{
			name:        "directory_with_readonly_permissions",
			description: "Read-only permissions on the copied tree do not break source cleanup",
			files: map[string]string{
				"app/config.json": "file",
			},
			copyFrom:      "app/",
			copyTo:        "dest/",
			permissions:   "500",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/config.json": "file",
			},
		},
		{
			name:        "replace_target_symlink_to_file",
			description: "A source file replaces a destination symlink instead of writing through it",
			files: map[string]string{
				"etc/resolv.conf": "file",
			},
			copyFrom: "etc/",
			copyTo:   "etc/",
			preexistingTargetPaths: map[string]string{
				// resolv.conf points outside the target tree (as on Ubuntu images)
				"../real/resolv.conf": "file",
				"etc/resolv.conf":     "symlink:../../real/resolv.conf",
			},
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"etc/resolv.conf": "regular",
			},
			expectedContents: map[string]string{
				"etc/resolv.conf": "dummy",
				// The symlink's old target must not have been written through
				"../real/resolv.conf": "preexisting",
			},
		},
		{
			name:        "follow_target_symlink_to_directory",
			description: "A source directory merges through a destination directory symlink (usrmerge layout)",
			files: map[string]string{
				"lib/mylib.so": "file",
			},
			copyFrom: ".",
			copyTo:   ".",
			preexistingTargetPaths: map[string]string{
				"usr/lib/existing.so": "file",
				"lib":                 "symlink:usr/lib",
			},
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"lib":                 "symlink",
				"usr/lib/mylib.so":    "file",
				"usr/lib/existing.so": "file",
			},
		},
		{
			name:        "copy_directory_that_is_not_first_alphabetically",
			description: "The entry named by the source path is copied, not the first entry in its parent",
			files: map[string]string{
				"project/components/Button.tsx": "file",
				"project/utils/helpers.ts":      "file",
			},
			copyFrom:      "project/utils/",
			copyTo:        "out/",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"out/helpers.ts": "file",
			},
			absentPaths: []string{
				"out/Button.tsx",
				"out/components",
			},
		},
		{
			name:        "copy_file_that_is_not_first_alphabetically",
			description: "The file named by the source path is copied, not the first entry in its parent",
			files: map[string]string{
				"assets/logo.png": "file",
				"config.json":     "file",
			},
			copyFrom:      "config.json",
			copyTo:        "dest/config.json",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"dest/config.json": "file",
			},
			absentPaths: []string{
				"dest/logo.png",
				"dest/config.json/logo.png",
			},
		},
		{
			name:        "deeply_nested_folder",
			description: "Deeply nested folder should be copied correctly",
			files: map[string]string{
				"app/l1/l2/l3/l4/l5/deep.txt": "file",
			},
			copyFrom:      "app/l1/l2/l3/l4",
			copyTo:        ".",
			shouldSucceed: true,
			expectedPaths: map[string]string{
				"l5/deep.txt": "file",
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // no idea why this one doesn't work, but it doesn't
		t.Run(tc.name, func(t *testing.T) {
			sourceDir, targetBaseDir, workDir := setupTestEnvironment(t)

			// Internal: create the hash/unpack directory structure (mimics tar extraction)
			// This is hidden from the test case definition
			hashDir := filepath.Join(sourceDir, "hash_"+tc.name)
			unpackDir := filepath.Join(hashDir, "unpack")
			require.NoError(t, os.MkdirAll(unpackDir, 0o755))

			// Create files and directories
			createFilesAndDirs(t, unpackDir, tc.files)

			// Verify expected paths exist
			if len(tc.files) > 0 {
				verifyFilesAndDirs(t, unpackDir, tc.files)
			}

			// Pre-populate the target to simulate an existing filesystem tree
			if len(tc.preexistingTargetPaths) > 0 {
				createFilesAndDirsWithContent(t, targetBaseDir, tc.preexistingTargetPaths, "preexisting")
			}

			// Internal: construct SourcePath (sbxUnpackPath + user's copyFrom path)
			// This mimics how copy.go constructs the path: filepath.Join(sbxUnpackPath, sourcePath)
			sourcePath := filepath.Join(unpackDir, tc.copyFrom)

			// Make target path absolute, preserving a trailing slash
			// (filepath.Join strips it, but Docker COPY semantics depend on it)
			targetPathOrFile := filepath.Join(targetBaseDir, tc.copyTo)
			if strings.HasSuffix(tc.copyTo, "/") {
				targetPathOrFile += "/"
			}

			// Set owner if not specified
			owner := tc.owner
			if owner == "" {
				owner = currentUser
			}

			// Generate script
			script := renderTemplate(t, copyScriptData{
				SourcePath:  sourcePath,
				TargetPath:  targetPathOrFile,
				Owner:       owner,
				Permissions: tc.permissions,
			})

			// Execute script
			stdout, stderr, exitCode := executeScript(t, script, workDir)

			// Verify exit code
			if tc.shouldSucceed {
				assert.Equal(t, 0, exitCode, "Script should succeed. stdout: %s, stderr: %s", stdout, stderr)
			} else {
				if tc.expectedExitCode != 0 {
					assert.Equal(t, tc.expectedExitCode, exitCode, "Exit code should match")
				} else {
					assert.NotEqual(t, 0, exitCode, "Script should fail")
				}
			}

			// Verify error message if specified
			if tc.expectedError != "" {
				assert.Contains(t, stderr, tc.expectedError, "Error message should contain expected text")
			}

			// Verify expected paths exist
			if len(tc.expectedPaths) > 0 {
				verifyFilesAndDirs(t, targetBaseDir, tc.expectedPaths)
			}

			// Verify paths that must not exist in the target
			for _, path := range tc.absentPaths {
				assert.NoFileExists(t, filepath.Join(targetBaseDir, path), "Path %s should not exist", path)
				assert.NoDirExists(t, filepath.Join(targetBaseDir, path), "Path %s should not exist", path)
			}

			// Verify file contents
			for path, expectedContent := range tc.expectedContents {
				content, err := os.ReadFile(filepath.Join(targetBaseDir, path))
				require.NoError(t, err, "Failed to read file %s", path)
				assert.Equal(t, expectedContent, string(content), "File %s content mismatch", path)
			}

			// Verify permissions of specific paths
			for path, permStr := range tc.expectedPerms {
				perms := getFilePermissions(t, filepath.Join(targetBaseDir, path))
				expectedPerms := os.FileMode(0)
				fmt.Sscanf(permStr, "%o", &expectedPerms)
				assert.Equal(t, expectedPerms, perms, "Path %s should have %s permissions", path, permStr)
			}

			// Special verification for permissions tests
			if tc.permissions != "" && tc.shouldSucceed {
				for path, entryType := range tc.expectedPaths {
					if entryType == "file" || entryType == "folder" {
						fullPath := filepath.Join(targetBaseDir, path)
						perms := getFilePermissions(t, fullPath)
						expectedPerms := os.FileMode(0)
						fmt.Sscanf(tc.permissions, "%o", &expectedPerms)
						assert.Equal(t, expectedPerms, perms, "File %s should have %s permissions", path, tc.permissions)
					}
				}
			}
		})
	}
}
