{
  description = "Rain local db remote utilities.";

  inputs = {
    rainix.url = "github:rainprotocol/rainix";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, flake-utils, rainix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = rainix.pkgs.${system};
      in rec {
        packages = rainix.packages.${system};

        devShells.default = pkgs.mkShell {
          shellHook = rainix.devShells.${system}.default.shellHook;
          inputsFrom = [ rainix.devShells.${system}.default ];
        };
      });
}
