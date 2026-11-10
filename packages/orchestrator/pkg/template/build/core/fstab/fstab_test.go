package fstab_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/fstab"
)

func ok(entries int) fstab.Report {
	return fstab.Report{Outcome: fstab.OutcomeOK, Entries: entries}
}

func blocked(class fstab.SourceClass) fstab.Report {
	return fstab.Report{Outcome: fstab.OutcomeOK, Entries: 1, Blockers: 1, BySource: map[fstab.SourceClass]int{class: 1}}
}

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
		want fstab.Report
	}{
		{"required missing device", "/dev/sdz1 /mnt/data ext4 defaults 0 0", blocked(fstab.SourceDev)},
		{"nofail", "/dev/sdz1 /mnt/data ext4 defaults,nofail 0 0", ok(1)},
		{"nofail ordered before local-fs", "/dev/sdz1 /mnt/data ext4 nofail,x-systemd.before=local-fs.target,x-systemd.device-timeout=90s 0 0", blocked(fstab.SourceDev)},
		{"nofail ordered before another unit", "/dev/sdz1 /mnt/data ext4 nofail,x-systemd.before=app.service 0 0", ok(1)},
		{"wanted by another target", "/dev/sdz1 /mnt/data ext4 x-systemd.wanted-by=maintenance.target 0 0", ok(1)},
		{"required by another target", "/dev/sdz1 /mnt/data ext4 x-systemd.required-by=multi-user.target 0 0", ok(1)},
		{"wanted by local-fs", "/dev/sdz1 /mnt/data ext4 x-systemd.wanted-by=local-fs.target 0 0", blocked(fstab.SourceDev)},
		{"required by local-fs and another target", "/dev/sdz1 /mnt/data ext4 x-systemd.required-by=app.service,x-systemd.required-by=local-fs.target 0 0", blocked(fstab.SourceDev)},
		{"noauto ordered before local-fs", "/dev/sdz1 /mnt/data ext4 noauto,x-systemd.before=local-fs.target 0 0", ok(1)},
		{"noauto", "/dev/sdz1 /mnt/data ext4 noauto 0 0", ok(1)},
		{"noauto overridden by auto", "/dev/sdz1 /mnt/data ext4 noauto,auto 0 0", blocked(fstab.SourceDev)},
		{"auto overridden by noauto", "/dev/sdz1 /mnt/data ext4 auto,noauto 0 0", ok(1)},
		{"noauto wanted by local-fs", "/dev/sdz1 /mnt/data ext4 noauto,x-systemd.wanted-by=local-fs.target 0 0", blocked(fstab.SourceDev)},
		{"noauto required by another target", "/dev/sdz1 /mnt/data ext4 noauto,x-systemd.required-by=app.service 0 0", ok(1)},
		{"nofail overridden by fail", "/dev/sdz1 /mnt/data ext4 nofail,fail 0 0", blocked(fstab.SourceDev)},
		{"fail overridden by nofail", "/dev/sdz1 /mnt/data ext4 fail,nofail 0 0", ok(1)},
		{"automount", "/dev/sdz1 /mnt/data ext4 x-systemd.automount 0 0", ok(1)},
		{"netdev", "/dev/sdz1 /mnt/data ext4 _netdev 0 0", ok(1)},
		{"bind", "/srv /mnt/data none bind 0 0", ok(1)},
		{"root by uuid", "UUID=0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f / ext4 defaults 0 1", ok(1)},
		{"uuid without a known identity", "UUID=0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f /mnt/data ext4 defaults 0 2", unresolved},
		{"label without a known identity", "LABEL=data /mnt/data ext4 defaults 0 2", unresolved},
		{"nofail uuid without a known identity", "UUID=0f0f0f0f /mnt/data ext4 nofail 0 2", ok(1)},
		{"swap by uuid without a known identity", "UUID=0f0f0f0f none swap defaults 0 0", ok(1)},
		{"partuuid", "PARTUUID=0f0f0f0f-01 /mnt/data ext4 defaults 0 2", blocked(fstab.SourcePartUUID)},
		{"partlabel", "PARTLABEL=data /mnt/data ext4 defaults 0 2", blocked(fstab.SourcePartLabel)},
		{"id", "ID=virtio-data /mnt/data ext4 defaults 0 2", blocked(fstab.SourceDev)},
		{"attached drive", "/dev/vda /mnt/data ext4 defaults 0 0", ok(1)},
		{"root alias", "/dev/root / ext4 defaults 0 1", ok(1)},
		{"partition of the drive", "/dev/vda1 /boot ext4 defaults 0 2", blocked(fstab.SourceDev)},
		{"by-uuid path", "/dev/disk/by-uuid/0f0f0f0f /mnt/data ext4 defaults 0 2", blocked(fstab.SourceDev)},
		{"tmpfs", "tmpfs /tmp tmpfs defaults 0 0", ok(1)},
		{"proc", "proc /proc proc defaults 0 0", ok(1)},
		{"nfs", "host:/export /nfs nfs defaults 0 0", ok(1)},
		{"cifs", "//srv/share /smb cifs defaults 0 0", ok(1)},
		{"fuse subtype", "none /x fuse.sshfs defaults 0 0", ok(1)},
		{"swap file", "/swapfile none swap sw 0 0", ok(1)},
		{"swap on missing device", "/dev/sdb5 none swap defaults 0 0", fstab.Report{Outcome: fstab.OutcomeOK, Entries: 1, SwapMissing: 1}},
		{"initrd mount still required without an initrd", "/dev/sdz1 /var ext4 defaults,x-initrd.mount 0 0", blocked(fstab.SourceDev)},
		{"initrd mount with nofail", "/dev/sdz1 /var ext4 x-initrd.mount,nofail 0 0", ok(1)},
		{"device timeout does not exempt", "/dev/sdz1 /mnt/data ext4 x-systemd.device-timeout=5s 0 0", blocked(fstab.SourceDev)},
		{"comments and blank lines", "# header\n\n   \n  # indented\n/dev/sdz1 /mnt/data ext4 defaults 0 0\n", blocked(fstab.SourceDev)},
		{"crlf", "/dev/vda /mnt/a ext4 defaults 0 0\r\n/dev/sdz1 /mnt/data ext4 defaults 0 0\r\n", fstab.Report{
			Outcome: fstab.OutcomeOK, Entries: 2, Blockers: 1, BySource: map[fstab.SourceClass]int{fstab.SourceDev: 1},
		}},
		{"tabs", "/dev/sdz1\t/mnt/data\text4\tdefaults\t0\t0", blocked(fstab.SourceDev)},
		{"inline comment hides options", "/dev/sdz1 /mnt/data ext4 #nofail", blocked(fstab.SourceDev)},
		{"inline comment after fields", "/dev/sdz1 /mnt/data ext4 nofail 0 0 # keep", ok(1)},
		{"extra fields", "/dev/sdz1 /mnt/data ext4 defaults 0 0 extra more", blocked(fstab.SourceDev)},
		{"escaped space in mount point", `/dev/sdz1 /\040 ext4 defaults 0 0`, blocked(fstab.SourceDev)},
		{"escaped root is root", `/dev/sdz1 \057 ext4 defaults 0 0`, ok(1)},
		{"two fields", "/dev/sdz1 /mnt/data", fstab.Report{Outcome: fstab.OutcomeOK, Malformed: 1}},
		{"too large", string(bytes.Repeat([]byte("A"), 1<<20)), fstab.Report{Outcome: fstab.OutcomeUnknown, Reason: fstab.ReasonTooLarge}},
		{"nul byte", "/dev/sdz1 /mnt/data ext4\x00 defaults", fstab.Report{Outcome: fstab.OutcomeUnknown, Reason: fstab.ReasonNotText}},
		{"invalid utf-8", "/dev/sdz1 /mnt/\xff ext4 defaults", fstab.Report{Outcome: fstab.OutcomeUnknown, Reason: fstab.ReasonNotText}},
		{"empty", "", ok(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, fstab.Parse([]byte(tt.data), fstab.DefaultTarget()))
		})
	}
}

var unresolved = fstab.Report{Outcome: fstab.OutcomeOK, Entries: 1, Unresolved: 1}

func TestParseMatchesTargetIdentity(t *testing.T) {
	t.Parallel()

	known := fstab.DefaultTarget()
	known.IdentityKnown = true
	known.UUID = "0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f"
	known.Label = "rootfs"

	// A filesystem made without a label or UUID has neither, so nothing matches it.
	blank := fstab.DefaultTarget()
	blank.IdentityKnown = true

	tests := []struct {
		name   string
		target fstab.Target
		data   string
		want   fstab.Report
	}{
		{"root filesystem by uuid", known, "UUID=0F0F0F0F-0F0F-0F0F-0F0F-0F0F0F0F0F0F /mnt/root ext4 defaults 0 0", ok(1)},
		{"root filesystem by label", known, "LABEL=rootfs /mnt/root ext4 defaults 0 0", ok(1)},
		{"other uuid", known, "UUID=1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f /mnt/data ext4 defaults 0 0", blocked(fstab.SourceUUID)},
		{"other label", known, "LABEL=data /mnt/data ext4 defaults 0 0", blocked(fstab.SourceLabel)},
		{"empty uuid", known, "UUID= /mnt/data ext4 defaults 0 0", blocked(fstab.SourceUUID)},
		{"no uuid on the filesystem", blank, "UUID=0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f /mnt/data ext4 defaults 0 0", blocked(fstab.SourceUUID)},
		{"no label on the filesystem", blank, "LABEL= /mnt/data ext4 defaults 0 0", blocked(fstab.SourceLabel)},
		{"device path ignores identity", known, "/dev/sdz1 /mnt/data ext4 defaults 0 0", blocked(fstab.SourceDev)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, fstab.Parse([]byte(tt.data), tt.target))
		})
	}
}

func TestParseNeverRetainsInput(t *testing.T) {
	t.Parallel()

	data := "/dev/SENTINEL-9f3a /mnt/SENTINEL-9f3a ext4 defaults 0 0\nSENTINEL-9f3a\n"
	report := fstab.Parse([]byte(data), fstab.DefaultTarget())

	assert.Equal(t, 1, report.Blockers)
	assert.NotContains(t, fmt.Sprintf("%+v", report), "SENTINEL-9f3a")
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"/dev/sdz1 /mnt/data ext4 defaults 0 0",
		"UUID=0f0f0f0f /mnt/data ext4 nofail 0 0\n# c\n",
		"/dev/sdb5 none swap defaults 0 0\r\n",
		`/dev/sdz1 /mnt/a\040b ext4 defaults 0 0`,
		"/dev/sdz1 /mnt/data",
		"",
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r := fstab.Parse(data, fstab.DefaultTarget())
		if r.Blockers+r.SwapMissing+r.Unresolved > r.Entries {
			t.Fatalf("blockers %d + swap %d + unresolved %d exceed entries %d", r.Blockers, r.SwapMissing, r.Unresolved, r.Entries)
		}
	})
}
