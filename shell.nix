/* Version from Robert (rycee): */
{ pkgs ? import <nixpkgs> {
  overlays = [(
    self: super: {
      dotnet-sdk-specific = super.dotnet-sdk.overrideAttrs (
        old: rec {
          version = "6.0.400";
          src = super.fetchurl {
            url = "https://dotnetcli.azureedge.net/dotnet/Sdk/${version}/dotnet-sdk-${version}-linux-x64.tar.gz";
            sha256 = "c9507e9d3fe0a0d3e18277d15606f27bd134c8541b26682a20b55e45fd7bc17b";
          };
        }
      );
    }
  )];
}}:

with pkgs;

mkShell {
  buildInputs = [
    ncurses # for `clear`
    emacs
    openssl
    zlib
    dotnet-sdk-specific
  ];
  shellHook = ''
    export CLI_DEBUG=1
    export DOTNET_CLI_TELEMETRY_OPTOUT="1"
    export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
    export LD_LIBRARY_PATH=${openssl.out}/lib/:${zlib.out}/lib/
    # export LD_DEBUG="all"
    #
    # Issue with: FAKE + FSAC and dotnet 6.0.x
    # - https://github.com/fsprojects/fantomas/issues/412#issuecomment-798774514
    export DOTNET_ROOT="/nix/store/lh13bkffks62x8sls4kvvqb83hbnvai7-dotnet-sdk-6.0.400"
  '';  
}

# References:
#
# Download .NET (specific):
#
# - https://dotnet.microsoft.com/download/dotnet
#
# - https://dotnet.microsoft.com/download/dotnet/6.0