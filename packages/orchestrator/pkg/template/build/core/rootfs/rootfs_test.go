//go:build linux

package rootfs

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
)

// bakeLayers renders the additional OCI layers for a template with memoryMB of
// RAM under the given rootfs options, against a throwaway envd and busybox.
func bakeLayers(t *testing.T, memoryMB int64, opts buildcontext.RootfsOptions) []containerregistry.Layer {
	t.Helper()

	tempDir := t.TempDir()
	envdPath := filepath.Join(tempDir, "envd")
	require.NoError(t, os.WriteFile(envdPath, []byte("echo hello"), 0o755))

	busyboxVersion := "1.36.1"
	busyboxDir := filepath.Join(tempDir, "busybox")
	require.NoError(t, os.MkdirAll(filepath.Join(busyboxDir, busyboxVersion, runtime.GOARCH), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(busyboxDir, busyboxVersion, runtime.GOARCH, "busybox"), []byte("busybox-binary"), 0o755))

	buildContext := buildcontext.BuildContext{
		BuilderConfig: cfg.BuilderConfig{
			HostEnvdPath:   envdPath,
			HostBusyboxDir: busyboxDir,
			BusyboxVersion: busyboxVersion,
		},
		Config: config.TemplateConfig{MemoryMB: memoryMB},
		Rootfs: opts,
	}

	layers, err := additionalOCILayers(buildContext, "provision.sh", "provision.log", "provision.result")
	require.NoError(t, err)
	require.Len(t, layers, 2)

	return layers
}

// tarEntries reads a layer and returns its regular files by path and its
// symlinks by path, the latter mapped to their targets.
func tarEntries(t *testing.T, layer containerregistry.Layer) (files, symlinks map[string]string) {
	t.Helper()

	reader, err := layer.Uncompressed()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, reader.Close())
	})

	files, symlinks = map[string]string{}, map[string]string{}
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		switch header.Typeflag {
		case tar.TypeReg:
			var buffer bytes.Buffer
			count, err := io.CopyN(&buffer, tarReader, header.Size)
			require.NoError(t, err)
			assert.Equal(t, header.Size, count)
			files[header.Name] = buffer.String()
		case tar.TypeSymlink:
			symlinks[header.Name] = header.Linkname
		}
	}

	return files, symlinks
}

// bakedFiles is the files layer rendered for memoryMB under opts, by path.
func bakedFiles(t *testing.T, memoryMB int64, opts buildcontext.RootfsOptions) map[string]string {
	t.Helper()

	files, _ := tarEntries(t, bakeLayers(t, memoryMB, opts)[0])

	return files
}

func TestAdditionalOCILayers(t *testing.T) {
	t.Parallel()
	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		layers := bakeLayers(t, 100, buildcontext.RootfsOptions{})
		actualFiles, _ := tarEntries(t, layers[0])
		assert.Len(t, actualFiles, 21)

		// The provisioning boot must be self-contained on the baked busybox:
		// minimal images (distroless) may have no /bin/sh, and busybox init
		// hands any inittab line with shell metacharacters to /bin/sh — so the
		// pipeline lives in the runner script and every inittab entry is a
		// plain exec.
		inittab := actualFiles["etc/inittab"]
		require.NotEmpty(t, inittab)
		for line := range strings.SplitSeq(inittab, "\n") {
			if !strings.HasPrefix(line, "::") {
				continue
			}
			assert.NotContains(t, line, "|", "inittab entries must not need /bin/sh: %s", line)
			assert.NotContains(t, line, "$", "inittab entries must not need /bin/sh: %s", line)
		}
		runner := actualFiles["usr/local/bin/e2b-provision-runner"]
		require.NotEmpty(t, runner, "provision runner must be baked")
		assert.Contains(t, runner, "#!/usr/bin/busybox ash")

		// Both init families' envd services seed certs via the shared script.
		seedCerts := actualFiles["usr/local/bin/e2b-seed-certs"]
		require.NotEmpty(t, seedCerts, "cert seeding script must be baked")
		assert.Contains(t, actualFiles["etc/systemd/system/envd.service"], "ExecStartPre=/usr/local/bin/e2b-seed-certs")

		// Ships verbatim into every guest boot; parse it the way the guest's sh will.
		seedScript := filepath.Join(t.TempDir(), "e2b-seed-certs")
		require.NoError(t, os.WriteFile(seedScript, []byte(seedCerts), 0o700))
		shOut, shErr := exec.CommandContext(t.Context(), "sh", "-n", seedScript).CombinedOutput()
		require.NoErrorf(t, shErr, "e2b-seed-certs is not valid sh:\n%s", shOut)

		// Both init families run the same boot-time chrony source selector, and
		// the file it writes is the only source chrony.conf gets — a PHC
		// refclock baked at build time is fatal to chronyd on a node without
		// the device.
		chronySource := actualFiles["usr/local/bin/e2b-chrony-source"]
		require.NotEmpty(t, chronySource, "chrony source selector must be baked")
		assert.Contains(t, chronySource, "/run/chrony-e2b/source.conf")
		assert.Contains(t, chronySource, "refclock PHC /dev/ptp0")
		assert.Contains(t, chronySource, "pool pool.ntp.org")
		assert.Contains(t, actualFiles["etc/systemd/system/e2b-chrony-source.service"],
			"ExecStart=/usr/local/bin/e2b-chrony-source")
		openrcChronySource := actualFiles["usr/local/share/e2b/chrony-source.openrc"]
		require.NotEmpty(t, openrcChronySource, "OpenRC chrony source service must be baked")
		assert.Contains(t, openrcChronySource, "before chronyd")

		// envd must be preset-enabled: first boot (machine-id is removed by
		// provisioning) applies the distro preset policy, and the RHEL
		// family's "disable *" would otherwise delete envd's autostart link.
		assert.Equal(t, "enable envd.service\n", actualFiles["etc/systemd/system-preset/00-e2b.preset"])

		// The OpenRC counterpart (Alpine) ships alongside the systemd unit; it
		// must supervise envd and honor the memory limit.
		// It lives OUTSIDE /etc/init.d — Debian's update-rc.d aborts on a
		// non-LSB script there — and is installed by the OpenRC init setup.
		openrcEnvd := actualFiles["usr/local/share/e2b/envd.openrc"]
		require.NotEmpty(t, openrcEnvd, "OpenRC envd service must be baked")
		assert.Contains(t, openrcEnvd, "#!/sbin/openrc-run")
		assert.Contains(t, openrcEnvd, "supervisor=supervise-daemon")
		assert.Contains(t, openrcEnvd, "GOMEMLIMIT=50MiB")
		assert.Equal(t, "e2b.local", actualFiles["etc/hostname"])
		assert.Equal(t, "nameserver 8.8.8.8", actualFiles["etc/resolv.conf"])

		// verify that memory function works
		assert.Contains(t, actualFiles["etc/systemd/system/envd.service"], `"GOMEMLIMIT=50MiB"`)

		// verify that systemd is configured to retry envd forever
		assert.Contains(t, actualFiles["etc/systemd/system/envd.service"], "StartLimitIntervalSec=0")

		// Regression guard: envd must be ordered after systemd-tmpfiles-setup.service.
		// updateEnvd stages its replacement binary in /tmp during early boot, and on
		// our Ubuntu/Debian base images systemd-tmpfiles-setup.service wipes /tmp's
		// contents at boot (`D /tmp` rule run with --remove). Without this ordering
		// envd can answer the build's upload before the wipe, and the staged
		// /tmp/envd_updated is deleted, so the follow-up chmod/mv fails with ENOENT.
		envdAfter := ""
		for line := range strings.SplitSeq(actualFiles["etc/systemd/system/envd.service"], "\n") {
			if strings.HasPrefix(line, "After=") {
				envdAfter = line

				break
			}
		}
		require.NotEmpty(t, envdAfter, "envd.service must declare an After= ordering")
		assert.Contains(t, envdAfter, "systemd-tmpfiles-setup.service",
			"envd.service After= must order envd after the boot-time /tmp wipe")

		// ensure that both files have identical content
		disabledContent := strings.TrimSpace(`
[Service]
WatchdogSec=0`)
		assert.Equal(t, disabledContent, actualFiles["etc/systemd/system/systemd-journald.service.d/override.conf"])
		assert.Equal(t, disabledContent, actualFiles["etc/systemd/system/systemd-networkd.service.d/override.conf"])

		// Regression guard: the envd autostart symlink must not dangle.
		// A relative target resolves inside multi-user.target.wants/ and dangles,
		// and provision.sh's offline `systemctl enable` prunes dangling .wants
		// links — silently disabling envd autostart on e.g. Fedora.
		_, actualSymlinks := tarEntries(t, layers[1])
		envdWants := actualSymlinks["etc/systemd/system/multi-user.target.wants/envd.service"]
		require.NotEmpty(t, envdWants, "envd autostart symlink must be present")
		assert.Equal(t, "/etc/systemd/system/envd.service", envdWants,
			"envd autostart symlink target must be absolute so it never dangles")
	})
}

// hasLine reports whether s contains line as a whole line, so that
// "MemoryMin=16M" does not match "MemoryMin=160M".
func hasLine(s, line string) bool {
	return slices.Contains(strings.Split(s, "\n"), line)
}

func TestEnvdMemoryProtectionRender(t *testing.T) {
	t.Parallel()

	const (
		dropIn = "etc/systemd/system/system.slice.d/10-e2b-envd.conf"
		unit   = "etc/systemd/system/envd.service"
	)
	off := buildcontext.RootfsOptions{}
	on := buildcontext.RootfsOptions{EnvdMemoryProtection: true}

	t.Run("off renders the unit byte for byte as before the option existed, and no drop-in", func(t *testing.T) {
		t.Parallel()

		// Captured from the render before the option existed, for a 4096 MB
		// template. It changes only when what the unit renders for that
		// template is changed on purpose, in envd.service.tpl or in the model
		// values it reads (GOMEMLIMIT comes from MemoryLimit).
		golden, err := os.ReadFile("testdata/envd.service.golden")
		require.NoError(t, err)

		files := bakedFiles(t, 4096, off)
		assert.Equal(t, string(golden), files[unit])
		assert.NotContains(t, files, dropIn)
	})

	t.Run("on requests the same protection on exactly the slice and the unit, whatever the RAM", func(t *testing.T) {
		t.Parallel()

		// The sizes in the design's table, plus one pathological value. The
		// request is a constant, and the same two lines at every size is what
		// keeps a coupling to the template's RAM from creeping back in; the
		// lines are literals so that a changed constant fails here.
		for _, memoryMB := range []int64{128, 512, 1024, 4096, 16384, 0} {
			t.Run(strconv.FormatInt(memoryMB, 10), func(t *testing.T) {
				t.Parallel()

				files := bakedFiles(t, memoryMB, on)
				require.Contains(t, files, dropIn)
				assert.True(t, hasLine(files[dropIn], "[Slice]"), files[dropIn])

				for _, path := range []string{dropIn, unit} {
					for _, line := range []string{"MemoryMin=128M", "MemoryLow=256M"} {
						assert.True(t, hasLine(files[path], line), "%s lacks %s:\n%s", path, line, files[path])
					}
				}

				// No third file carries a request: a request placed elsewhere
				// on the chain would change what the kernel grants.
				for path, data := range files {
					if path == dropIn || path == unit {
						continue
					}
					assert.NotContains(t, data, "MemoryMin=", path)
					assert.NotContains(t, data, "MemoryLow=", path)
				}
			})
		}
	})

	t.Run("the option changes nothing but the drop-in and the unit's two request lines", func(t *testing.T) {
		t.Parallel()

		offFiles := bakedFiles(t, 4096, off)
		onFiles := bakedFiles(t, 4096, on)

		require.Contains(t, onFiles, dropIn)
		delete(onFiles, dropIn)
		require.Len(t, onFiles, len(offFiles))
		for path, offData := range offFiles {
			if path == unit {
				continue
			}
			assert.Equal(t, offData, onFiles[path], path)
		}

		offLines := strings.Split(offFiles[unit], "\n")
		onLines := strings.Split(onFiles[unit], "\n")
		require.Len(t, onLines, len(offLines))
		var changed []string
		for i := range offLines {
			if offLines[i] != onLines[i] {
				changed = append(changed, offLines[i]+" -> "+onLines[i])
			}
		}
		assert.Equal(t, []string{
			"MemoryMin=50M -> MemoryMin=128M",
			"MemoryLow=100M -> MemoryLow=256M",
		}, changed)
	})
}
