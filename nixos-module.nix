# NixOS module for jmpchat-matrix-bridge (see flake.nix).
#
# What it does:
#   - builds the bridge with the same CGO configuration as the upstream Docker
#     image (E2BE via libolm + SIP call support via libopus)
#   - a one-shot init unit that, on every boot:
#       1. installs config.yaml generated from `services.jmpchat-matrix-bridge.settings`
#          (first boot) or deep-merges the template into the existing state copy
#          (later boots: settings changes take effect; generated secrets like
#          as_token/hs_token are preserved — see mergePy for the merge rules)
#       2. generates the appservice registration with the bridge's own `-g`
#          (only if missing — `-g` rotates both tokens on every run, so it must
#          not be re-run while the registration is installed on the homeserver)
#   - a hardened systemd daemon unit running as a dedicated non-root user
#   - opens the firewall for SIP signalling (UDP+TCP 5060-5069) and RTP media
#     (UDP 40000-40100) when network.calls is enabled
#   - orders matrix-synapse after the init unit, so the registration exists
#     before Synapse reads app_service_config_files
{
  config,
  lib,
  pkgs,
  ...
}:
with lib; let
  cfg = config.services.jmpchat-matrix-bridge;
  stateDir = "/var/lib/jmpchat-matrix-bridge";
  synapseDir = "/var/lib/matrix-synapse";
  registrationFile = "jabber-registration.yaml";

  yamlGenerator = pkgs.formats.yaml {};
  baseConfig = yamlGenerator.generate "jmpchat-matrix-bridge-config.yaml" cfg.settings;

  # Deep-merges the Nix template into the state config on every boot, so that
  # settings changes in the NixOS config take effect after the first install.
  # Rules:
  #   - template values win (deep for dicts, whole-value for lists/scalars)
  #   - keys only present in the state config (generated secrets like
  #     appservice.as_token/hs_token) are kept
  #   - a template value of exactly "generate" is a generation placeholder:
  #     it only fills a missing key, never overwrites a generated one
  #   - the file is only rewritten when the merge actually changed something
  mergePy = pkgs.writeText "jmpchat-matrix-bridge-merge-config.py" ''
    import sys
    import yaml

    state_path, template_path = sys.argv[1], sys.argv[2]

    def deep_merge(dst, src):
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                deep_merge(dst[k], v)
            elif v == "generate" and k in dst:
                pass
            else:
                dst[k] = v

    with open(template_path) as f:
        template = yaml.safe_load(f) or {}
    with open(state_path) as f:
        state = yaml.safe_load(f) or {}

    deep_merge(state, template)

    new = yaml.safe_dump(state, sort_keys=False, default_flow_style=False)
    with open(state_path) as f:
        old = f.read()
    if old != new:
        with open(state_path, "w") as f:
            f.write(new)
        print(f"jmpchat-matrix-bridge: merged Nix template into {state_path}")
    else:
        print("jmpchat-matrix-bridge: state config up to date")
  '';

  pythonEnv = pkgs.python3.withPackages (p: [ p.pyyaml ]);

  initScript = pkgs.writeShellScript "jmpchat-matrix-bridge-init" ''
    set -euo pipefail

    # 1. Install the config on first boot; on later boots merge the Nix template
    #    into the state copy so settings changes take effect (secrets preserved).
    if [ ! -f ${stateDir}/config.yaml ]; then
      install -D -m 0600 -o ${cfg.user} -g ${cfg.group} ${baseConfig} ${stateDir}/config.yaml
    else
      ${pythonEnv}/bin/python ${mergePy} ${stateDir}/config.yaml ${baseConfig}
    fi

    # 2. Generate the registration + upsert its tokens into the state config.
    #    ONLY run this once: `-g` rotates both tokens every run, which would
    #    break the already-registered appservice. Delete both files to re-bootstrap.
    if [ ! -f ${stateDir}/registration.yaml ]; then
      ${cfg.package}/bin/jmpchat-matrix-bridge -c ${stateDir}/config.yaml -r ${stateDir}/registration.yaml -g
      chown ${cfg.user}:${cfg.group} ${stateDir}/config.yaml ${stateDir}/registration.yaml
      chmod 0600 ${stateDir}/config.yaml ${stateDir}/registration.yaml
    fi

    # 3. Expose the registration for Synapse's app_service_config_files.
    mkdir -p ${synapseDir}
    install -m 0640 -o matrix-synapse -g matrix-synapse \
      ${stateDir}/registration.yaml ${synapseDir}/${registrationFile}
  '';
in {
  options.services.jmpchat-matrix-bridge = {
    enable = mkEnableOption "jmpchat-matrix-bridge (a Matrix<->JMP.chat/XMPP bridge, mautrix-style)";

    package = mkOption {
      type = types.package;
      default = pkgs.callPackage ./package.nix {};
      defaultText = "jmpchat-matrix-bridge from this flake";
      description = "The jmpchat-matrix-bridge package to run.";
    };

    settings = mkOption {
      type = yamlGenerator.type;
      default = {};
      description = ''
        The bridge config.yaml. as_token/hs_token are generated once on first boot
        (via the bridge's own `-g`), so leave them unset here.
      '';
    };

    user = mkOption {
      type = types.str;
      default = "jmpchat";
    };
    group = mkOption {
      type = types.str;
      default = "jmpchat";
    };

    environmentFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Optional env file (e.g. OTEL_* OTLP endpoint). Telemetry is off unless set.";
    };
  };

  config = mkIf cfg.enable {
    # libolm is currently marked insecure in nixpkgs (unmaintained upstream);
    # the bridge only needs it for E2BE.
    nixpkgs.config.permittedInsecurePackages = mkAfter ["olm-3.2.16"];

    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      home = stateDir;
    };
    users.groups.${cfg.group} = {};

    systemd.services.jmpchat-matrix-bridge-init = {
      description = "Bootstrap jmpchat-matrix-bridge config + registration";
      after = ["local-fs.target"];
      before = ["jmpchat-matrix-bridge.service" "matrix-synapse.service"];
      requiredBy = ["jmpchat-matrix-bridge.service"];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = initScript;
      };
    };

    systemd.services.jmpchat-matrix-bridge = {
      description = "jmpchat-matrix-bridge appservice";
      after = ["network-online.target" "jmpchat-matrix-bridge-init.service"];
      wants = ["network-online.target"];
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        # Relative paths in the config (e.g. zeroconfig's default file writer "./logs/")
        # resolve inside the state dir instead of the read-only cwd (/).
        WorkingDirectory = stateDir;
        StateDirectory = "jmpchat-matrix-bridge";
        ExecStart = escapeShellArgs [
          "${cfg.package}/bin/jmpchat-matrix-bridge"
          "-c" "${stateDir}/config.yaml"
          "-r" "${stateDir}/registration.yaml"
        ];
        Restart = "on-failure";
        RestartSec = 10;
        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectHome = true;
        ProtectSystem = "strict";
        ReadWritePaths = [stateDir];
        # AF_NETLINK: the SIP auto-detect enumerates local IPs via netlink (net.Dialer ip+net).
        RestrictAddressFamilies = ["AF_INET" "AF_INET6" "AF_UNIX" "AF_NETLINK"];
        AmbientCapabilities = "";
        CapabilityBoundingSet = "";
      };
      # Telemetry via OTEL_* stays opt-in: if environmentFile exports OTEL_*_ENDPOINT
      # the bridge picks that up; otherwise it installs nothing (verified in
      # pkg/telemetry: everything is off unless an OTLP endpoint is configured).
    };
    systemd.services.jmpchat-matrix-bridge.serviceConfig.EnvironmentFile =
      mkIf (cfg.environmentFile != null)
      (mkDefault cfg.environmentFile);

    # Synapse must not start before the registration exists (it fails on a
    # missing app service config file).
    systemd.services.matrix-synapse.after = ["jmpchat-matrix-bridge-init.service"];
    systemd.services.matrix-synapse.wants = ["jmpchat-matrix-bridge.service"];

    # SIP signalling and RTP media must be reachable by JMP's servers (see
    # network.calls.sip_port_min..max / rtp_port_min..max in the bridge config).
    networking.firewall.allowedTCPPortRanges = [{from = 5060; to = 5069;}];
    networking.firewall.allowedUDPPortRanges = [
      {from = 5060; to = 5069;}
      {from = 40000; to = 40100;}
    ];
  };
}
