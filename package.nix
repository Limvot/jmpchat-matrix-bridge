# Packaging for jmpchat-matrix-bridge.
#
# Same build the upstream Dockerfile performs (CGO + libolm for E2BE, libopus for
# calls, bundled sqlite3, build tags `sipcalls nolibopusfile`).
#
# `src` defaults to this repo's checkout (flakes pass their own clean source);
# buildGoModule needs network access for the `go mod` fixed-output derivation.
{
  lib,
  buildGo126Module,
  pkg-config,
  olm,
  opus,
  version ? "0.8.0",
  jmpchatSrc ? lib.cleanSourceWith {
    src = lib.cleanSource ./.;
    filter = name: type: type != "symlink" || baseNameOf name != "result";
  },
}:
buildGo126Module rec {
  pname = "jmpchat-matrix-bridge";
  inherit version;
  src = jmpchatSrc;

  subPackages = ["cmd/jmpchat-matrix-bridge"];
  tags = ["sipcalls" "nolibopusfile"];
  env.CGO_ENABLED = "1";
  # libopus.a pulls in libm symbols; some toolchains need them explicit at link time.
  env.CGO_LDFLAGS = "-lm";
  buildInputs = [olm opus];
  nativeBuildInputs = [pkg-config];
  ldflags = ["-s" "-w" "-X main.Tag=v${version}"];
  vendorHash = "sha256-Fty8LNWh6MkNlYPo30PK14TJBX84nfXg6xd2NYEJBWM=";

  meta = with lib; {
    description = "Matrix <-> JMP.chat bridge: XMPP/SMS/MMS, SIP calls, CardDAV contacts";
    homepage = "https://github.com/williamkray/jmpchat-matrix-bridge";
    # No LICENSE file in the repo upstream yet; leave the license unset
    # rather than mislabel it.
    maintainers = [];
    mainProgram = "jmpchat-matrix-bridge";
    platforms = platforms.linux;
  };
}
