{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      with pkgs;
      {
        devShell = mkShell rec {
          nativeBuildInputs = [
            metals
          ];
          buildInputs = [
            spark
            sbt
          ];
          LD_LIBRARY_PATH = lib.makeLibraryPath buildInputs;
        };
      }
    );
}

