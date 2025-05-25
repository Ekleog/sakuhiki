all: fmt clippy test doc

fmt:
    cargo fmt

doc:
    cargo doc --workspace --all-features

clippy:
    cargo clippy --workspace --all-features -- -D warnings

test:
    cargo nextest run --workspace --all-features
