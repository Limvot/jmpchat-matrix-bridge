{
  description = "jmpchat-matrix-bridge — a Matrix <-> JMP.chat bridge (XMPP/SMS/MMS, SIP calls, CardDAV contacts)";

  inputs.nixpkgs.url = "nixpkgs/nixos-unstable";

  outputs = {
    self,
    nixpkgs,
  }: let
    # nixpkgs-unstable is needed for Go 1.26 (go.mod requires it).
    system = "x86_64-linux";
    pkgs = import (nixpkgs) {
      inherit system;
      config.permittedInsecurePackages = ["olm-3.2.16"];
    };
    jmpchat-matrix-bridge = pkgs.callPackage ./package.nix {jmpchatSrc = self;};
  in {
    packages.${system} = {
      jmpchat-matrix-bridge = jmpchat-matrix-bridge;
      default = jmpchat-matrix-bridge;
    };

    # If you use the overlay instead of the module/flake package, remember that
    # libolm is currently marked insecure upstream: allow "olm-3.2.16" in your
    # nixpkgs config (the NixOS module below does this for you).
    overlays.default = final: prev: {
      jmpchat-matrix-bridge = final.callPackage ./package.nix {jmpchatSrc = self;};
    };

    # NixOS module: services.jmpchat-matrix-bridge.enable = true; (see README "Nix")
    nixosModules.default = import ./nixos-module.nix;

    checks.${system}.default = jmpchat-matrix-bridge;
  };
}
