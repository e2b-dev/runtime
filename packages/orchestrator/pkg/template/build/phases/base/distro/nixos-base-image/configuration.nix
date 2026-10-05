# E2B premade NixOS sandbox base.
# Everything provision.sh does imperatively on other distros is declared here;
# the orchestrator's nixos profile only verifies and boots.
{ config, pkgs, lib, ... }:
{
  # Boot: the E2B microVM supplies its own kernel and mounts the rootfs rw
  # (root=/dev/vda rw), then runs this system's stage-2 init directly — no
  # initrd, no bootloader.
  # The initrd/kernel in the closure are unused dead weight (the microVM
  # boots E2B's kernel with init= pointing at this system's stage-2 init),
  # but NixOS's module system requires them to evaluate; only grub is off.
  boot.loader.grub.enable = false;
  fileSystems."/" = { device = "/dev/vda"; fsType = "ext4"; };

  # eth0 is configured by the kernel command line (ip=...); nothing to manage.
  networking.useDHCP = false;
  networking.resolvconf.enable = false;
  # NixOS is the only family that enables a firewall by default — provision.sh
  # installs iptables/nftables for user workloads but never filters. Leaving it on
  # would drop the orchestrator's connection to envd (TCP 49983) and every
  # customer-exposed sandbox port; isolation is enforced by the E2B network layer.
  networking.firewall.enable = false;
  # The E2B rootfs layer bakes an immutable /etc/resolv.conf; NixOS must not
  # try to regenerate it at activation.
  environment.etc."resolv.conf".enable = false;
  # Same for the baked /etc/hostname and /etc/hosts (both carry e2b.local, which
  # every other family keeps). An empty hostName also stops activation calling
  # `hostname` and overriding the running name.
  networking.hostName = "";
  environment.etc."hostname".enable = false;
  environment.etc."hosts".enable = false;

  # The env daemon: E2B bakes the static binary at /usr/bin/envd as an OCI
  # layer on top of this image. Mirrors envd.service.tpl (systemd family).
  systemd.services.envd = {
    description = "E2B env daemon";
    wantedBy = [ "multi-user.target" ];
    unitConfig.StartLimitIntervalSec = 0;
    # e2b-seed-certs (baked at /usr/local/bin by the E2B layer) bind-mounts a
    # tmpfs over /etc/ssl/certs seeded with DEREFERENCED copies of the trust
    # bundle: envd APPENDS the egress-proxy CA to ca-certificates.crt at
    # sandbox /init, which must not hit a symlink into the read-only store.
    # socat and iptables are executed by name: envd spawns socat to forward
    # exposed ports and shells out to iptables to pin the MMDS route. There is no
    # FHS bin dir to find them in, so they must be on the unit's PATH.
    #
    # envd hands its OWN environment's PATH to every process it spawns, so this
    # list is also the PATH of anything started through the exec API that does
    # not go through a login shell (`sh -c` — what the orchestrator's pre-pause
    # reclaim/sync use — or a bare argv[0]). Without the system profile that PATH
    # has no `sh`, `curl`, `git`, `jq`: on the FHS families the same unit inherits
    # systemd's default /usr/bin:/bin, i.e. the whole userland, so append NixOS's
    # equivalent. /run/wrappers first, like the login PATH, so setuid wrappers win
    # over the plain store copies; the explicit packages stay ahead of the profile
    # so envd's own helpers always resolve inside this closure.
    path = [
      "/run/wrappers"
      pkgs.coreutils pkgs.util-linux pkgs.gnutar pkgs.socat pkgs.iptables
      "/run/current-system/sw"
    ];
    serviceConfig = {
      Type = "simple";
      Restart = "always";
      ExecStartPre = "/usr/local/bin/e2b-seed-certs";
      ExecStart = "/usr/bin/envd";
      LimitCORE = "infinity";
      # envd.service.tpl templates GOMEMLIMIT per sandbox as min(MemoryMB/2, 512)MiB;
      # a premade image can't know the sandbox size, so pin the 512 MiB ceiling —
      # envd must still GC under a cap, not grow unbounded.
      Environment = [ "GOTRACEBACK=all" "GOMEMLIMIT=512MiB" ];
      # Priority/scheduling parity with envd.service.tpl (ionice 1:4 = realtime,4).
      Nice = -20;
      IOSchedulingClass = "realtime";
      IOSchedulingPriority = 4;
      OOMPolicy = "continue";
      OOMScoreAdjust = -1000;
      # Resource-control parity: reserve envd's memory and win CPU/IO contention.
      # These are the unit's long-standing values, not the request the template
      # build can render into envd.service. That request comes with a
      # system.slice drop-in that makes it effective, and neither reaches this
      # image: provisioning removes the baked /etc/systemd/system so activation
      # can own it (InitNixOS in distro/init.go), so the slice request would
      # have to be declared here. Until it is, system.slice requests nothing
      # and envd's request is granted nothing.
      Delegate = true;
      MemoryMin = "50M";
      MemoryLow = "100M";
      CPUAccounting = true;
      CPUWeight = 1000;
      IOAccounting = true;
      IOWeight = 10000;
    };
  };

  # Default sandbox user (matches configure.sh on the other families).
  # Match useradd semantics on the other families: a per-user group named
  # after the user (configure.sh chowns /home/user to user:user).
  users.groups.user = {};
  users.users.user = {
    isNormalUser = true;
    group = "user";
    extraGroups = [ "wheel" ];
    initialHashedPassword = "";
  };
  users.users.root.initialHashedPassword = "";
  security.sudo.wheelNeedsPassword = false;
  # The DEFAULT USER build step greps for this exact line before appending to
  # /etc/sudoers (which is a read-only store symlink on NixOS) — declaring it
  # makes that step a clean no-op.
  security.sudo.extraConfig = "user ALL=(ALL:ALL) NOPASSWD: ALL";

  services.openssh = {
    enable = true;
    settings.PermitRootLogin = "yes";
    settings.PermitEmptyPasswords = true;
  };
  security.pam.services.sshd.allowNullPassword = true;

  # Time-sync parity with provision.sh: prefer the hypervisor PHC refclock
  # (kvm-ptp — no network, tracks the host) when /dev/ptp0 is present, else the
  # NTP pool. A refclock line for a missing PHC is FATAL to chronyd, and a premade
  # image can't probe the device at build time, so the source line is written at
  # boot by e2b-chrony-source.service and pulled in via this include.
  services.chrony = {
    enable = true;
    servers = [ ];
    extraConfig = ''
      include /run/chrony-e2b/source.conf
      makestep 1.0 3
    '';
  };

  systemd.services.e2b-chrony-source = {
    description = "E2B: select chrony time source (PHC refclock if /dev/ptp0, else NTP)";
    before = [ "chronyd.service" ];
    requiredBy = [ "chronyd.service" ];
    path = [ pkgs.coreutils ];
    serviceConfig = {
      Type = "oneshot";
      RemainAfterExit = true;
    };
    script = ''
      mkdir -p /run/chrony-e2b
      if [ -e /dev/ptp0 ]; then
        echo "refclock PHC /dev/ptp0 poll 2 dpoll 2" > /run/chrony-e2b/source.conf
      else
        echo "pool pool.ntp.org iburst maxsources 3" > /run/chrony-e2b/source.conf
      fi
    '';
  };

  # Journald must not watchdog-reboot when the microVM is paused for
  # snapshots (mirrors the systemd-family override baked into other images).
  systemd.services.systemd-journald.serviceConfig.WatchdogSec = 0;

  # No serial getty fighting the console; keep the closure lean.
  systemd.services."serial-getty@ttyS0".enable = false;
  documentation.enable = false;

  # Kernel tunables provision.sh writes to /etc/sysctl.conf on other
  # families (NixOS reads sysctl.d from its own config instead).
  boot.kernel.sysctl."fs.inotify.max_user_watches" = 65536;
  boot.kernel.sysctl."vm.compaction_proactiveness" = 0;

  # E2B build steps and customer commands are executed via /bin/bash (the
  # orchestrator invokes it explicitly, like on every FHS distro) — provide it.
  system.activationScripts.e2bBinBash = "mkdir -m 0755 -p /bin && ln -sfn ${pkgs.bash}/bin/bash /bin/bash";

  # Load the store registration build.sh packed, once, so the nix tooling sees
  # the closure as valid. Never fail activation over it: without the DB the nix
  # commands are broken, but the sandbox itself is fine — which is the status quo
  # this repairs, not a regression it could introduce.
  system.activationScripts.e2bNixDb = ''
    if [ -f /nix/var/nix/db-registration ] && [ ! -e /nix/var/nix/db/db.sqlite ]; then
      ${pkgs.nix}/bin/nix-store --load-db < /nix/var/nix/db-registration || true
    fi
  '';

  # Parity with the package set provision.sh installs on the other families, so
  # a sandbox exposes the same userland whichever base image it was built from.
  # (openssh, sudo, chrony and bash are declared as services/programs above.)
  # shadow is not optional: finalize's configure.sh runs useradd, usermod and
  # passwd by name, the same way it does on the families that install
  # shadow/shadow-utils via Packages.
  environment.systemPackages = with pkgs; [
    shadow socat curl git jq less fuse3 iptables nftables iputils iproute2 nfs-utils
  ];

  # Tracks the nixpkgs pin in build.sh, and is meant to. The usual "never
  # change stateVersion" rule protects persistent state across an upgrade of a
  # running system; this image has none — every publish builds it from scratch
  # and every sandbox boots it fresh — so leaving it behind the pin would just
  # opt the image into compatibility shims for a release it never ran.
  system.stateVersion = "26.05";
}
