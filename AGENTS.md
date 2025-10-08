# Repository Guidelines

## Project Structure & Module Organization
Rust sources live in `src/`. `sync.rs` orchestrates remote orderbook syncs, `manifest.rs` owns the manifest format, and `src/workflows/` contains the automation entrypoints that compile to the `bump-schema-version` and `bump-seed-generation` binaries. The `bin/` directory stores the extracted `rain-orderbook-cli` binary (and its downloaded archive) used during syncs, while `data/` holds compressed SQL snapshots per network (`*.sql.gz`) plus the authoritative `manifest.yaml`. `Cargo.toml` and `flake.nix` anchor the Rust workspace and Nix development environment.

## Build, Test, and Development Commands
Always work inside `nix develop` so the Rust toolchain and CLI dependencies are on PATH. Run `cargo fmt`, `cargo test`, or `cargo run --release` from that shell. The release run downloads the CLI if needed, executes a sync, updates `data/*.sql.gz`, and refreshes `data/manifest.yaml`. Maintenance helpers ship as bins: use `cargo run --quiet --bin bump-schema-version` (optionally passing custom manifest and source paths) and `cargo run --quiet --bin bump-seed-generation -- <chain-id> [manifest-path]` to update the manifest without touching the full sync.

## Sync Workflow & Database Dumps
Each sync inflates the existing gzipped dump (or bootstraps a fresh database), runs the CLI, exports a new dump, then deletes the temporary `.db`. `SyncConfig` defaults to Arbitrum (`chain_id` 42161), writing dumps under `data/` and choosing that directory even if a parent copy exists. After finalizing a dump the code rewrites `data/manifest.yaml`, preserving the current seed generation and stamping the download URL derived from `RELEASE_DOWNLOAD_URL_TEMPLATE`. Keep `data/` writeable and avoid manual edits to the transient database; update the compressed dump or manifest if you need to seed changes.

## Manifest Management Helpers
`data/manifest.yaml` tracks the global `schema_version` and per-network dump metadata. The schema constant (`Manifest::CURRENT_SCHEMA_VERSION` in `src/manifest.rs`) must stay in sync with the manifest file. Use `cargo run --quiet --bin bump-schema-version` to bump both in one step. When a specific network needs a seed reset, run `cargo run --quiet --bin bump-seed-generation -- <chain-id>`; it increments the manifest entry and logs the `previous=`/`next=` values consumed by CI.

## Coding Style & Naming Conventions
Format with `cargo fmt` (4-space indentation) and keep module responsibilities narrow. Exported APIs should expose explicit types, and fallible paths must return `anyhow::Result` carrying context. Add comments sparingly to clarify non-obvious control flow or invariants.

## Testing Guidelines
Unit tests live alongside their modules in `src/`, including the workflow binaries. Run `cargo test` inside the dev shell to cover filesystem orchestration, manifest helpers, CLI invocation, and HTTP/network stubs.

## Commit & Pull Request Guidelines
Use Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) for manual changes. PRs should summarize their intent, list affected networks or data directories, and call out required environment variables (`COMMIT_HASH`, `HYPERRPC_API_TOKEN`, etc.). Attach sync logs when they demonstrate behavioural changes. Automation currently commits with messages like `update orderbook dump` or `bump manifest schema version to ...`; keep those as-is unless the workflow changes.

## Environment Notes
`COMMIT_HASH` must point to a valid `rain.orderbook` commit and is surfaced as a GitHub repository variable (`vars.COMMIT_HASH`) in CI; export it locally before running `cargo run --release`. API tokens can be supplied via any key in `API_TOKEN_ENV_VARS` (`RAIN_API_TOKEN`, `RAIN_ORDERBOOK_API_TOKEN`, `HYPERRPC_API_TOKEN`). Secrets should remain outside the repo and be injected into the Nix shell or CI environment. Avoid committing temporary SQLite databases—only the gzipped dumps and manifest are canonical.

## CI Automation
Three manual GitHub Actions live in `.github/workflows/`:
- `Remote Sync` runs `nix develop --command cargo run --release`, using `vars.COMMIT_HASH` and `secrets.HYPERRPC_API_TOKEN`. Concurrency (`group: remote-sync`) prevents overlapping runs. On `main` it configures Git with `CI_GIT_USER`/`CI_GIT_EMAIL`, commits `update orderbook dump`, and pushes if changes appear under `data/`.
- `Bump Manifest Schema Version` calls the `bump-schema-version` binary, prints the diff, and, on `main`, commits the updated manifest and `src/manifest.rs` using `bump manifest schema version to <next>`.
- `Bump Seed Generation` requires a `chain_id` input, invokes the corresponding helper, and commits `bump seed generation for chain <id> to <next>` when run on `main`.
All jobs install Nix via Determinate Systems' action and use the FlakeHub cache. Provide `CI_GIT_USER`, `CI_GIT_EMAIL`, and the API token secrets so these workflows can commit and authenticate successfully.
