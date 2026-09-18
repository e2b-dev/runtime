// Init-system axis of the distro profiles: one provisioning-time shell block
// per init system, rendered into the selector as e2b_init_setup() so provision.sh
// stays init-agnostic.
package distro

import "strings"

// InitSystem is the guest init family a profile boots with.
type InitSystem string

const (
	// InitSystemd — Debian/Ubuntu, RHEL/Fedora, Arch (systemd via envd.service).
	InitSystemd InitSystem = "systemd"
	// InitOpenRC — Alpine (busybox init → OpenRC via the baked /etc/init.d/envd).
	InitOpenRC InitSystem = "openrc"
	// InitNixOS — premade NixOS (declarative; provisioning masks nothing).
	InitNixOS InitSystem = "nixos"
)

// initSetup is the provisioning-time shell block per init system. Bodies may
// reference the selector's profile variables (e.g. $E2B_TIMESYNC_UNIT), defined
// in the same case arm before the function is called.
var initSetup = map[InitSystem]string{
	InitSystemd: `echo "Don't wait for ttyS0 (serial console kernel logs)"
# This is required when the Firecracker kernel args has specified console=ttyS0
systemctl mask serial-getty@ttyS0.service

echo "Disable network online wait"
systemctl mask systemd-networkd-wait-online.service

echo "Disable system first boot wizard"
# This was problem with Ubuntu 24.04, that differently calculate wizard should be called
# and Linux boot was stuck in wizard until envd wait timeout
systemctl mask systemd-firstboot.service

echo "Enable time synchronization ($E2B_TIMESYNC_UNIT)"
# Distro-correct chrony unit (chrony on Debian, chronyd on RHEL/Arch).
systemctl enable "$E2B_TIMESYNC_UNIT"

echo "Pull the boot-time time-source selector into $E2B_TIMESYNC_UNIT"
# e2b-chrony-source.service writes the source line chrony.conf includes; the
# unit that must pull it in is only known here (the name differs per family).
mkdir -p "/etc/systemd/system/$E2B_TIMESYNC_UNIT.service.d"
printf '[Unit]\nRequires=e2b-chrony-source.service\nAfter=e2b-chrony-source.service\n' \
    >"/etc/systemd/system/$E2B_TIMESYNC_UNIT.service.d/e2b-chrony-source.conf"

echo "Enable SSH ($E2B_SSH_UNIT)"
# provision.sh writes the sandbox sshd_config on every family, but nothing was
# turning the unit on: Debian's postinst and the RHEL RPM scriptlet enable it
# themselves, Arch does not, so Arch sandboxes shipped with SSH configured and
# dead. Enabling is idempotent where the packaging already did it.
systemctl enable "$E2B_SSH_UNIT.service"

echo "Enable envd autostart"
# Belt-and-suspenders with the baked 00-e2b.preset: on the RHEL family the
# package transaction above runs systemd's RPM scriptlet 'systemctl preset-all'
# (policy 'disable *'), which deletes the baked wants-symlink.
systemctl enable envd.service

echo "Disable chrony-wait"
# chrony-wait blocks multi-user.target until the first clock sync (~8s);
# chrony still syncs in the background, nothing needs to wait for it.
# masking a unit that doesn't exist on this distro still succeeds (systemctl
# mask just writes the /dev/null symlink), so a failure here is real.
systemctl mask chrony-wait.service

echo "Disable slow boot units not needed in the sandbox"
# binfmt registrations (foreign-arch exec) take ~1s of CPU early in boot and
# compete with envd start; e2scrub is for LVM-backed ext4 only.
systemctl mask systemd-binfmt.service
systemctl mask e2scrub_reap.service`,

	// OpenRC (Alpine). The image is still running the one-shot PROVISIONING
	// inittab (it is what launched this script); replace it with the real
	// boot sequence and wire the runlevels a container image ships without.
	InitOpenRC: `echo "Installing boot inittab (busybox init -> OpenRC runlevels)"
printf '%s\n' \
    '::sysinit:/bin/sh -c "mkdir -p /dev/pts && mount -t devpts devpts /dev/pts 2>/dev/null; [ -c /dev/ptmx ] || mknod /dev/ptmx c 5 2 2>/dev/null"' \
    '::sysinit:/sbin/openrc sysinit' \
    '::sysinit:/sbin/openrc boot' \
    '::wait:/sbin/openrc default' \
    '::shutdown:/sbin/openrc shutdown' \
    '::ctrlaltdel:/sbin/reboot' \
    > /etc/inittab

echo "Registering base OpenRC services"
# Container images carry no runlevel wiring at all (setup-alpine does this on
# real installs): kernel filesystems in sysinit, system prep in boot. bootmisc
# also wipes /tmp in the boot runlevel, so envd (default runlevel) can never
# race the wipe — the ordering systemd needs After= for is inherent here.
# Which scripts exist varies by image (mdev vs udev, procfs presence) — check
# and say so instead of swallowing rc-update errors.
for svc in devfs sysfs procfs dmesg mdev; do
    if [ -e "/etc/init.d/$svc" ]; then
        rc-update add "$svc" sysinit
    else
        echo "OpenRC service $svc not present on this image; skipping"
    fi
done
for svc in localmount sysctl hostname bootmisc; do
    if [ -e "/etc/init.d/$svc" ]; then
        rc-update add "$svc" boot
    else
        echo "OpenRC service $svc not present on this image; skipping"
    fi
done

# The FC guest's eth0 is configured by the kernel (ip=), but OpenRC services
# declaring a "need net" dependency (chronyd) trigger the networking service,
# which errors out on a missing /etc/network/interfaces and takes chronyd
# down with it. A loopback-only interfaces file lets networking start (and
# provide "net") without touching the kernel-managed eth0.
printf 'auto lo\niface lo inet loopback\n' > /etc/network/interfaces
if [ -e /etc/init.d/networking ]; then
    rc-update add networking boot
else
    # openrc ships this script and the profile installs openrc, so this is
    # unreachable on the images we support. If it ever fires, nothing provides
    # "net": chronyd is still enabled below so the failure shows up in the boot
    # log rather than the sandbox silently running without time sync.
    echo "OpenRC networking service not present on this image; nothing provides 'net', so time sync will fail to start"
fi

echo "Enable time synchronization ($E2B_TIMESYNC_UNIT)"
rc-update add "$E2B_TIMESYNC_UNIT" default

echo "Install the boot-time time-source selector"
# Writes the source line chrony.conf includes, in the boot runlevel so it is
# done before chronyd starts in default. Baked outside /etc/init.d for the same
# reason as the envd service script.
cp /usr/local/share/e2b/chrony-source.openrc /etc/init.d/e2b-chrony-source
chmod 0755 /etc/init.d/e2b-chrony-source
rc-update add e2b-chrony-source boot

echo "Disabling the chronyd seccomp filter"
# Alpine's OpenRC init script hardcodes '-F 1', which loads chronyd's seccomp
# filter, and Alpine's chrony build (-NTS -SECHASH -DEBUG) takes a SIGSYS — "Bad
# system call" right after "Loaded seccomp filter (level 1)" — as soon as the PHC
# refclock is driven, leaving the service in OpenRC's "crashed" state with the
# clock unsynced. Unconditional, NOT gated on the PHC being present: which source
# chronyd drives is decided at boot by e2b-chrony-source, so provisioning cannot
# know. It costs nothing when the pool branch is taken, and the systemd families
# pass no -F at all, so this is parity rather than a new hole. The init script
# splices $command_args in after its own -F 1 and chronyd honours the last -F.
# conf.d must be named for the init script OpenRC sources it for.
mkdir -p /etc/conf.d
echo 'command_args="-F 0"' >>"/etc/conf.d/$E2B_TIMESYNC_UNIT"

echo "Symlink ionice and nice into /usr/bin for envd OOM wrapper"
# envd hard-codes /usr/bin/ionice and /usr/bin/nice in its OOM-score wrapper.
# util-linux-misc installs ionice to /bin/ionice (not /usr/bin); busybox ships
# nice to /bin/nice. Without these symlinks every envd-spawned command exits 127.
ln -sf /bin/ionice /usr/bin/ionice
ln -sf /bin/nice   /usr/bin/nice
echo "Enable envd autostart"
# The service script is baked at a neutral path (envd.openrc.tpl) so the
# Debian family's update-rc.d never sees it; install it for OpenRC here.
cp /usr/local/share/e2b/envd.openrc /etc/init.d/envd
chmod 0755 /etc/init.d/envd
rc-update add envd default

echo "Enable sshd"
if [ -e /etc/init.d/sshd ]; then
    rc-update add sshd default
else
    echo "sshd service not present on this image; skipping"
fi`,

	// NixOS is declaratively configured; drop the baked systemd units so
	// activation can own /etc/systemd/system as a store symlink (foreign files
	// there make systemd boot with no units at all).
	InitNixOS: `echo "NixOS is declaratively configured; removing the baked systemd drop-ins"
rm -rf /etc/systemd/system`,
}

// indentBlock indents every non-empty line of a shell block for embedding
// inside the generated selector function bodies.
func indentBlock(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}

	return strings.Join(lines, "\n")
}
