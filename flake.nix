{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/master";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils }:
    let
      out = system:
        let pkgs = nixpkgs.legacyPackages."${system}";
        in {

          devShell = pkgs.mkShell {
            buildInputs = with pkgs; [
              (python314.withPackages (ps: with ps; [ pip uv ty pytest ]))
              ruff
              socat
            ];

            shellHook = ''
              if [ -d "$PWD/tests/bats/bin" ]; then
                export PATH="$PWD/tests/bats/bin:$PATH"
              fi
            '';
          };

          # defaultPackage = with pkgs.poetry2nix; mkPoetryApplication {
          #     projectDir = ./.;
          #     preferWheels = true;
          # };

          # defaultApp = utils.lib.mkApp {
          #     drv = self.defaultPackage."${system}";
          # };

        };
    in with utils.lib; eachSystem defaultSystems out;

}
