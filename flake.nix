{
  description = "Nix development environment for Kafka, Airflow, and Python";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }: 
  let
    supportedSystems = [ "x86_64-darwin" "aarch64-darwin" "x86_64-linux" "aarch64-linux" ];
    forAllSystems = f: builtins.listToAttrs (map (system: { name = system; value = f system; }) supportedSystems);
  in {
    devShells = forAllSystems (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in {
        default = pkgs.mkShell {
          buildInputs = with pkgs; [
            docker-compose
            python311
            python311Packages.virtualenv
            python311Packages.pip
            apacheKafka
            postgresql
            curl
            netcat
            zsh
          ];

          shellHook = ''
            zsh
            echo "Setting up Python virtual environment..."
            python -m venv venv
            source venv/bin/activate
            echo "Installing Python dependencies..."
            pip install --upgrade pip
            pip install -r requirements.txt
            echo "Development environment is ready!"
          '';
        };
      }
    );
  };
}
