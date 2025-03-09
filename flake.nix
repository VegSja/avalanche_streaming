{
  description = "Nix development environment for Kafka, Airflow, and Python";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }: 
  let
    pkgs = import nixpkgs { system = "x86_64-darwin"; };  # Change to "x86_64-linux" if on Linux
  in {
    devShells.x86_64-darwin.default = pkgs.mkShell {
      buildInputs = with pkgs; [
        docker-compose
        python311
        python311Packages.virtualenv
        python311Packages.pip
        kafka
        postgresql
        curl
        netcat
      ];

      shellHook = ''
        echo "Setting up Python virtual environment..."
        python -m venv venv
        source venv/bin/activate
        echo "Installing Python dependencies..."
        pip install --upgrade pip
        pip install -r requirements.txt
        echo "Development environment is ready!"
      '';
    };
  };
}
