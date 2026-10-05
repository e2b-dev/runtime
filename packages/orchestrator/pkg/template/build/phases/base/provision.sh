#!/bin/sh
set -eu

BUSYBOX="{{ .BusyBox }}"
RESULT_PATH="{{ .ResultPath }}"

echo "Starting provisioning script"

{{ if eq .Provider "gcp" }}
# GCP Specific logic
{{ end }}

echo "Making configuration immutable"
$BUSYBOX chattr +i /etc/resolv.conf

# Identify the base image by its DECLARED /etc/os-release ID, never by probing
# for a package manager. The selector (generated from the distro profile
# registry) defines the E2B_* vars and pkg/CA shell functions, or exits 1 on an
# unsupported distribution. Images with no os-release (distroless, scratch) are
# rejected by name rather than guessed.
echo "Detecting base image distribution"
if [ -r /etc/os-release ]; then
    . /etc/os-release
    E2B_DISTRO_ID="${ID:-unknown}"
    # Derivative-to-parent pointer, e.g. Kali declares ID=kali ID_LIKE=debian.
    # Only consulted when the declared ID matches no profile.
    E2B_ID_LIKE="${ID_LIKE:-}"
else
    E2B_DISTRO_ID="unknown (image has no /etc/os-release)"
    E2B_ID_LIKE=""
fi

{{ .DistroSelector }}

echo "Provisioning for distro '$E2B_DISTRO_ID' (init=$E2B_INIT_BIN, timesync=$E2B_TIMESYNC_UNIT, admin-group=$E2B_ADMIN_GROUP)"

# Persist the resolved identity for later build phases (finalize sources it),
# so the profile's values are defined once, here.
mkdir -p /usr/local/share/e2b
{
    echo "E2B_DISTRO_ID='$E2B_DISTRO_ID'"
    echo "E2B_INIT_SYSTEM='$E2B_INIT_SYSTEM'"
    echo "E2B_ADMIN_GROUP='$E2B_ADMIN_GROUP'"
} > /usr/local/share/e2b/distro.env

# Helper function to check if a package is installed (distro-specific query)
is_package_installed() {
    e2b_pkg_query "$1"
}

# Install required packages if not already installed
PACKAGES="$E2B_PACKAGES"
echo "Checking presence of the following packages: $PACKAGES"

MISSING=""
for pkg in $PACKAGES; do
    if ! is_package_installed "$pkg"; then
        echo "Package $pkg is missing, will install it."
        MISSING="$MISSING $pkg"
    fi
done

if [ -n "$MISSING" ]; then
    echo "Missing packages detected, installing:$MISSING"
    # shellcheck disable=SC2086
    e2b_pkg_install $MISSING
else
    echo "All required packages are already installed."
fi

# Ensure the CA trust bundle exists where envd expects it; e2b_ca_refresh
# regenerates it per family. A refresh failure fails provisioning (set -e) — a
# silently broken trust store is worse than a legible build error.
if [ ! -s "$E2B_CA_BUNDLE" ]; then
    echo "CA trust bundle missing at $E2B_CA_BUNDLE — running the profile's refresh"
    e2b_ca_refresh
fi

# Set /dev/fuse permissions to 666 for non-root access
# Use systemd-tmpfiles to set permissions at boot
mkdir -p /etc/tmpfiles.d
echo 'z /dev/fuse 0666 root root -' > /etc/tmpfiles.d/fuse.conf

echo "Setting up shell"
# Not every base image ships /etc/profile.d or /root; create them so the
# drop-ins below always have a home.
mkdir -p /etc/profile.d /root
echo "export SHELL='/bin/bash'" >/etc/profile.d/shell.sh
echo "export PS1='\w \$ '" >/etc/profile.d/prompt.sh
echo "export PS1='\w \$ '" >>"/etc/profile"
echo "export PS1='\w \$ '" >>"/root/.bashrc"

echo "Use .bashrc and .profile"
echo "if [ -f ~/.bashrc ]; then source ~/.bashrc; fi; if [ -f ~/.profile ]; then source ~/.profile; fi" >>/etc/profile

echo "Remove root password"
# A minimal image may not ship /etc/passwd; only clear the root password when it
# exists rather than failing provisioning.
if [ -f /etc/passwd ]; then
    passwd -d root
else
    echo "No /etc/passwd on this image; skipping root password removal"
fi

echo "Setting up chrony"
mkdir -p /etc/chrony
{
    # The source line (PHC refclock vs NTP pool) is chosen at BOOT by
    # e2b-chrony-source, not here: a template provisioned on a node with
    # /dev/ptp0 can cold-boot on a node without it, and a refclock line for a
    # missing PHC is FATAL to chronyd.
    echo "include /run/chrony-e2b/source.conf"
    # Step (jump) the clock instead of slewing when the offset exceeds 1s, but
    # only for the first 3 updates after chronyd starts. chronyd restarts on
    # every cold boot/reboot, so this corrects a large boot-time offset fast
    # (TLS needs a correct clock) without risking a backward jump under a
    # running workload. Needed because chrony-wait is masked, so boot no
    # longer blocks on first sync.
    echo "makestep 1.0 3"
} >/etc/chrony/chrony.conf

# Add a proxy config, as some environments expects it there (e.g. timemaster in Node Dockerimage)
echo "include /etc/chrony/chrony.conf" >/etc/chrony.conf

echo "Setting up SSH"
mkdir -p /etc/ssh
# A drop-in leaves the package's sshd_config untouched: appending to it marks the conffile
# modified, and a later openssh upgrade in a customer build step then stops on dpkg's
# "modified configuration file" prompt until the build times out. Images whose sshd_config
# has no Include line (stock upstream config) still get the append.
if grep -qE '^Include[[:space:]]+/etc/ssh/sshd_config\.d/' /etc/ssh/sshd_config 2>/dev/null; then
  E2B_SSHD_CONF=/etc/ssh/sshd_config.d/00-e2b.conf
  mkdir -p /etc/ssh/sshd_config.d
else
  E2B_SSHD_CONF=/etc/ssh/sshd_config
fi
cat <<EOF >>"$E2B_SSHD_CONF"
PermitRootLogin yes
PermitEmptyPasswords yes
PasswordAuthentication yes
EOF

echo "Increasing inotify watch limit"
echo 'fs.inotify.max_user_watches=65536' | tee -a /etc/sysctl.conf

# Disable kcompactd background page migration. With 2 MiB host-side hugepage
# backing of guest RAM, every migration dirties a destination hugepage from
# the host UFFD's perspective and lands in the next memfile diff, with no
# corresponding workload benefit between snapshots. We trigger compaction
# explicitly pre-pause instead.
echo "Disabling proactive memory compaction"
echo 'vm.compaction_proactiveness=0' | tee -a /etc/sysctl.conf

# Init-system-specific boot arrangement (autostarts, boot-noise silencing, the
# OpenRC inittab swap), rendered per family by the distro selector (distro/init.go).
e2b_init_setup

# Clean machine-id from Docker
rm -rf /etc/machine-id

echo "Linking $E2B_INIT_BIN to init"
# Not every image ships /usr/sbin; create it before linking init.
mkdir -p /usr/sbin
ln -sf "$E2B_INIT_BIN" /usr/sbin/init
# /sbin is a real directory on non-usr-merged distros (Alpine) where the line
# above doesn't reach the /sbin/init the kernel is pointed at; link it too.
[ -L /sbin ] || ln -sf "$E2B_INIT_BIN" /sbin/init

echo "Unlocking immutable configuration"
$BUSYBOX chattr -i /etc/resolv.conf

echo "Finished provisioning script"

# Delete itself
rm -rf /etc/init.d/rcS
rm -rf /usr/local/bin/provision.sh

# Report successful provisioning
printf "0" > "$RESULT_PATH"
