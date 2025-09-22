# Repository Guidelines

## Project Structure & Module Organization
TypeScript sources live in `src/`, with `sync.ts` orchestrating remote orderbook syncs into SQLite. The `bin/` directory stores the unpacked `rain-orderbook-cli` binary, while `data/` holds compressed snapshots per network (`*.db.tar.gz`). Root configs (`package.json`, `tsconfig.json`, `flake.nix`) define npm scripts, compiler settings, and the Nix development environment. Generated JavaScript lands in `dist/`; commit only when required.

## Build, Test, and Development Commands
Always run commands inside the Nix dev shell to keep the toolchain on PATH. Use `nix develop` for an interactive session, then run `npm install`, `npm run build`, or `npm run sync`. For one-off tasks, prefer `nix develop -c npm run build` or `nix develop -c npm run sync`. The sync command fetches orderbook data and refreshes the local dumps under `data/`.

## Sync Workflow & Database Dumps
Each sync hydrates the working database from the existing tarball, runs the CLI, then re-archives the result before deleting the temporary `.db`. Only the compressed dump remains. Keep the directory writeable and avoid manual edits to the live `.db`—modify the tarball if you need seeded data. If a dump is missing, the script initializes a fresh database and archives it on completion.

## Coding Style & Naming Conventions
TypeScript files use 2-space indentation, `camelCase` for values, and `PascalCase` for types or interfaces. Keep modules small and favor pure helpers where practical. The compiler runs with `strict` enabled, so annotate exported APIs and handle nullable branches explicitly. Reuse the ANSI logging helpers already in `sync.ts` for consistent console output.

## Testing Guidelines
There is no automated suite yet. Validate behaviour by running `nix develop -c npm run sync` against representative dumps and inspecting the console plan output. When adding tests, colocate them under `src/__tests__/`, name files `*.spec.ts`, and cover configuration loading, filesystem behaviour, and process orchestration via mocks.

## Commit & Pull Request Guidelines
With no prior history, adopt Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) to keep logs searchable. Pull requests should summarize intent, list affected networks or data directories, and mention required environment variables (`HYPERLANE_API_TOKEN`, `RAIN_ORDERBOOK_API_TOKEN`, etc.). Attach relevant sync logs when they demonstrate behavioural changes.

## Environment Notes
Consult `src/sync.ts` for the authoritative list of tokens and the default CLI commit hash. Keep secrets outside the repo and export them into your Nix shell before syncing. Avoid committing local SQLite artifacts; tarball dumps in `data/` are canonical outputs.
