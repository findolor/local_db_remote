# Repository Guidelines

## Project Structure & Module Organization
Rust sources live in `src/`, with `sync.rs` orchestrating remote orderbook syncs into SQLite. The `bin/` directory stores the unpacked `rain-orderbook-cli` binary downloaded at runtime, while `data/` holds compressed snapshots per network (`*.db.tar.gz`). `Cargo.toml` and `flake.nix` anchor the build configuration and Nix development environment.

## Build, Test, and Development Commands
Always run commands inside the Nix dev shell to keep the Rust toolchain on PATH. Use `nix develop` for an interactive session, then run `cargo fmt`, `cargo test`, or `cargo run --release`. The release run fetches orderbook data and refreshes the local dumps under `data/`.

## Sync Workflow & Database Dumps
Each sync hydrates the working database from the existing tarball, runs the CLI, then re-archives the result before deleting the temporary `.db`. Only the compressed dump remains. Keep the directory writeable and avoid manual edits to the live `.db`—modify the tarball if you need seeded data. If a dump is missing, the script initializes a fresh database and archives it on completion.

## Coding Style & Naming Conventions
Use Rust's standard formatting (`cargo fmt`) with 4-space indentation and idiomatic ownership patterns. Keep modules focused and prefer pure helpers where practical. Exported APIs should carry explicit types, and fallible paths must return `anyhow::Result` with context.

## Testing Guidelines
Unit tests live alongside the modules they exercise under `src/`. Run `cargo test` inside the dev shell to validate behaviour, covering filesystem orchestration, CLI invocation, and network helpers via mocks or stubs.

## Commit & Pull Request Guidelines
With no prior history, adopt Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) to keep logs searchable. Pull requests should summarize intent, list affected networks or data directories, and mention required environment variables (`HYPERRPC_API_TOKEN`, `RAIN_ORDERBOOK_API_TOKEN`, etc.). Attach relevant sync logs when they demonstrate behavioural changes.

## Environment Notes
Consult `src/sync.rs` for the authoritative list of tokens and the default CLI commit hash. Keep secrets outside the repo and export them into your Nix shell before syncing. Avoid committing local SQLite artifacts; tarball dumps in `data/` remain the canonical outputs.
