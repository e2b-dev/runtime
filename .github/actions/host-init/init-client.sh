#!/usr/bin/env bash

set -euo pipefail

# Set timestamp format
PS4='[\D{%Y-%m-%d %H:%M:%S}] '
# Enable command tracing
set -x

# Add cache disk for orchestrator and swapfile
MOUNT_POINT="/orchestrator"

# Step 2: Create the mount point
sudo mkdir -p $MOUNT_POINT

sudo mkdir -p /orchestrator/sandbox
sudo mkdir -p /orchestrator/template
sudo mkdir -p /orchestrator/build

# Add swapfile. Skipped when the host already swaps (some runner images
# ship an active /swapfile, where fallocate fails with "text file busy").
SWAPFILE="/swapfile"
if [ -z "$(sudo swapon --show --noheadings 2>/dev/null)" ]; then
    sudo fallocate -l 1G $SWAPFILE
    sudo chmod 600 $SWAPFILE
    sudo mkswap $SWAPFILE
    sudo swapon $SWAPFILE
fi
# No fstab entry: this script serves ephemeral CI hosts, which never reboot.

# Set swap settings
sudo sysctl vm.swappiness=10
sudo sysctl vm.vfs_cache_pressure=50

# Add tmpfs for snapshotting
# TODO: Parametrize this
sudo mkdir -p /mnt/snapshot-cache
sudo mount -t tmpfs -o size=65G tmpfs /mnt/snapshot-cache

ulimit -n 1048576

# Applied directly rather than appended to /etc/sysctl.conf: ephemeral CI
# hosts never reboot, so persistence has nothing to persist for. The
# production init also raises the net.core/tcp backlog knobs; CI runs a
# handful of concurrent sandboxes, nowhere near the kernel defaults, so
# only the memory-map budget (every VM, uffd and cache mapping counts
# against it) is raised here.
sudo sysctl -w vm.max_map_count=1048576

echo "Disabling inotify for NBD devices"
# https://lore.kernel.org/lkml/20220422054224.19527-1-matthew.ruffell@canonical.com/
cat <<EOH >/etc/udev/rules.d/97-nbd-device.rules
# Disable inotify watching of change events for NBD devices
ACTION=="add|change", KERNEL=="nbd*", OPTIONS:="nowatch"
EOH

sudo udevadm control --reload-rules
sudo udevadm trigger

# Load the nbd module. Production hosts provision themselves; this script
# only serves CI. The orchestrator and the template manager each pre-warm
# NBD_POOL_SIZE (default 64) device slots from this shared pool, so 64 total
# starved the second process and rapid pause/resume tests saw 503s; 128
# covers exactly both warm pools — a test holding a device just delays a
# warm-loop refill, which is backpressure, not failure — without the
# node-creation storm of 4096. The nowatch udev rule above must be in place
# before the devices appear. Overridable for local experiments.
# Skipped when devices already exist: a kernel with the driver built in
# (CONFIG_BLK_DEV_NBD=y) boots with its own device set and modprobe has no
# module tree to load from there.
if [ ! -e /dev/nbd0 ]; then
    sudo modprobe nbd nbds_max="${NBDS_MAX:-128}"
fi

# Create the directory for the fc mounts
mkdir -p /fc-vm

# Download envd buckets
envd_dir="/fc-envd"
mkdir -p $envd_dir

cp packages/envd/bin/debug/envd "${envd_dir}/."

chmod -R 755 $envd_dir
ls -lh $envd_dir
du -h "${envd_dir}/envd"

# Download kernels. The bucket prefix holds every version ever published
# (multiple GiB); a CI run boots exactly one — the sandbox-template pin.
# In-test template builds converge on the same version because
# start-services exports DEFAULT_KERNEL_VERSION from the same pin, so the
# code default never engages in CI. If a test still fails on a missing
# kernel file, backtrack here: add the version to KERNEL_VERSIONS
# (space-separated override, honored even over a cache-restored directory)
# or fix the pin derivation. An unresolved pin falls back to pulling
# everything rather than guessing.
kernels_dir="/fc-kernels"
mkdir -p $kernels_dir

if [[ -n "${KERNEL_VERSIONS:-}" ]]; then
    echo "Pulling overridden kernels: ${KERNEL_VERSIONS}"
    for version in ${KERNEL_VERSIONS}; do
        if [[ ! -d "${kernels_dir}/${version}" ]]; then
            gsutil -m cp -r "gs://e2b-artifact-binaries/kernels/${version}" "${kernels_dir}/"
        fi
    done
    chmod -R 755 $kernels_dir
# Already populated means the exact-key cache in the host-init action
# restored it (the key pins the same versions this run derives), so the
# pull is skipped. The same guard covers the two directories below.
elif [[ -n "$(ls -A $kernels_dir 2>/dev/null)" ]]; then
    echo "Kernels restored from cache; skipping the pull"
else
    template_pin=$(grep -oE 'KERNEL_VERSION: "[^"]+"' .github/actions/build-sandbox-template/action.yml | head -1 | cut -d'"' -f2 || true)

    if [[ -n "$template_pin" ]]; then
        echo "Pulling pinned kernel: ${template_pin}"
        gsutil -m cp -r "gs://e2b-artifact-binaries/kernels/${template_pin}" "${kernels_dir}/"
    else
        # An unresolved pin means the defining file moved or changed shape;
        # a partial pull would fail later on the missing version, so pull all
        # — but never cache the everything-pull (see the save step's guard).
        echo "Could not resolve the pinned kernel version; pulling all of them"
        if [ -n "${GITHUB_ENV:-}" ]; then echo "ARTIFACTS_FULL_PULL=true" >> "$GITHUB_ENV"; fi
        gsutil -m cp -r gs://e2b-artifact-binaries/kernels/* "${kernels_dir}"
    fi
    chmod -R 755 $kernels_dir
fi
ls -lh $kernels_dir

# Download FC versions. Same shape as the kernels: the prefix grows with
# every release, but a run uses only the sandbox-template pin (in-test
# builds converge on it through DEFAULT_FIRECRACKER_VERSION from
# start-services). FC_VERSIONS overrides, an unresolved pin pulls all,
# and a missing version fails loudly at sandbox start — backtrack here.
fc_versions_dir="/fc-versions"
mkdir -p $fc_versions_dir
if [[ -n "${FC_VERSIONS:-}" ]]; then
    echo "Pulling overridden firecrackers: ${FC_VERSIONS}"
    for version in ${FC_VERSIONS}; do
        if [[ ! -d "${fc_versions_dir}/${version}" ]]; then
            gsutil -m cp -r "gs://e2b-artifact-binaries/firecrackers/${version}" "${fc_versions_dir}/"
        fi
    done
    chmod -R 755 $fc_versions_dir
elif [[ -n "$(ls -A $fc_versions_dir 2>/dev/null)" ]]; then
    echo "Firecracker versions restored from cache; skipping the pull"
else
    fc_pin=$(grep -oE 'FIRECRACKER_VERSION: "[^"]+"' .github/actions/build-sandbox-template/action.yml | head -1 | cut -d'"' -f2 || true)

    if [[ -n "$fc_pin" ]]; then
        echo "Pulling pinned firecracker: ${fc_pin}"
        gsutil -m cp -r "gs://e2b-artifact-binaries/firecrackers/${fc_pin}" "${fc_versions_dir}/"
    else
        echo "Could not resolve the pinned firecracker version; pulling all of them"
        if [ -n "${GITHUB_ENV:-}" ]; then echo "ARTIFACTS_FULL_PULL=true" >> "$GITHUB_ENV"; fi
        gsutil -m cp -r gs://e2b-artifact-binaries/firecrackers/* "${fc_versions_dir}"
    fi
    chmod -R 755 $fc_versions_dir
fi
ls -lh $fc_versions_dir

# Download busybox
busybox_dir="/fc-busybox"
mkdir -p $busybox_dir
if [[ -n "$(ls -A $busybox_dir 2>/dev/null)" ]]; then
    echo "Busybox restored from cache; skipping the pull"
else
    gsutil -m cp -r gs://e2b-artifact-binaries/busybox/* "${busybox_dir}"
    chmod -R 755 $busybox_dir
fi
ls -lh $busybox_dir

# Set up huge pages
# We are not enabling Transparent Huge Pages for now, as they are not swappable and may result in slowdowns + we are not using swap right now.
# The THP are by default set to madvise
# We are allocating the hugepages at the start when the memory is not fragmented yet
echo "[Setting up huge pages]"
sudo mkdir -p /mnt/hugepages
mount -t hugetlbfs none /mnt/hugepages
# Increase proactive compaction to reduce memory fragmentation for using overcomitted huge pages

available_ram=$(grep MemTotal /proc/meminfo | awk '{print $2}') # in KiB
available_ram=$(($available_ram / 1024))                        # in MiB
echo "- Total memory: $available_ram MiB"

min_normal_ram=$((4 * 1024))                             # 4 GiB
min_normal_percentage_ram=$(($available_ram * 16 / 100)) # 16% of the total memory
max_normal_ram=$((42 * 1024))                            # 42 GiB

max() {
    if (($1 > $2)); then
        echo "$1"
    else
        echo "$2"
    fi
}

min() {
    if (($1 < $2)); then
        echo "$1"
    else
        echo "$2"
    fi
}

ensure_even() {
    if (($1 % 2 == 0)); then
        echo "$1"
    else
        echo $(($1 - 1))
    fi
}

remove_decimal() {
    echo "$(echo $1 | sed 's/\..*//')"
}

reserved_normal_ram=$(max $min_normal_ram $min_normal_percentage_ram)
reserved_normal_ram=$(min $reserved_normal_ram $max_normal_ram)
echo "- Reserved RAM: $reserved_normal_ram MiB"

# The huge pages RAM should still be usable for normal pages in most cases.
hugepages_ram=$(($available_ram - $reserved_normal_ram))
hugepages_ram=$(remove_decimal $hugepages_ram)
hugepages_ram=$(ensure_even $hugepages_ram)
echo "- RAM for hugepages: $hugepages_ram MiB"

hugepage_size_in_mib=2
echo "- Huge page size: $hugepage_size_in_mib MiB"
hugepages=$(($hugepages_ram / $hugepage_size_in_mib))

# No base preallocation here: pinning pages up front exists to beat memory
# fragmentation on long-lived hosts, and this script only serves CI, whose
# freshly booted VMs never live long enough to fragment. Everything goes
# through the overcommit pool instead — allocated on demand, returned when
# freed — so the pages don't sit carved out of RAM while packages build and
# services start. If hugepage-allocation flakes ever appear, a base reserve
# comes back per-run via BASE_HUGEPAGES_PERCENTAGE.
base_hugepages_percentage="${BASE_HUGEPAGES_PERCENTAGE:-0}"
base_hugepages=$(($hugepages * $base_hugepages_percentage / 100))
base_hugepages=$(remove_decimal $base_hugepages)
echo "- Allocating $base_hugepages huge pages ($base_hugepages_percentage%) for base usage"
echo $base_hugepages >/proc/sys/vm/nr_hugepages

overcommitment_hugepages_percentage=$((100 - $base_hugepages_percentage))
overcommitment_hugepages=$(($hugepages * $overcommitment_hugepages_percentage / 100))
overcommitment_hugepages=$(remove_decimal $overcommitment_hugepages)
echo "- Allocating $overcommitment_hugepages huge pages ($overcommitment_hugepages_percentage%) for overcommitment"
echo $overcommitment_hugepages >/proc/sys/vm/nr_overcommit_hugepages
