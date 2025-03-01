let
  pkgs = import ./nix;
in
pkgs.mkShell.override {
  stdenv = pkgs.clangStdenv;
} {
  name = "sakuhiki";
  buildInputs = with pkgs; [
    niv
    pkg-config

    (fenix.combine (with fenix; [
      minimal.cargo
      minimal.rustc
      complete.rust-src
      rust-analyzer
      targets.wasm32-unknown-unknown.latest.rust-std
    ]))
  ];
  LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
}
