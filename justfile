toolchain := "nightly-2026-01-05"
fmt:
  rustup toolchain install {{toolchain}} > /dev/null 2>&1 && \
  cargo +{{toolchain}} fmt

fmt-check:
  rustup toolchain install {{toolchain}} > /dev/null 2>&1 && \
  cargo +{{toolchain}} fmt --check

clippy:
	cargo clippy --all-features --no-deps --all-targets -- \
		-D warnings

clippy-fix:
	cargo clippy --fix --allow-dirty --all-features --all-targets  --no-deps -- \
		-D warnings

test:
  cargo test --lib --tests --bins

machete:
  cargo install cargo-machete && \
  cargo machete && \
  ./check_workspace_deps.sh

typos:
  cargo install typos-cli --locked && \
  typos

fix-typos:
  cargo install typos-cli --locked --force && \
  typos -w

# run-connector
rc path:
  cargo run --profile release-with-debug --features test_validator -- {{path}}

# build-connector
bc:
  cargo build --profile release-prod

lint: fmt clippy typos
  cargo install cargo-machete
  cargo machete
  cargo test
