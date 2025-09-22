# Repository Guidelines

## Project Structure & Module Organization
TypeScript sources live in `src/`, with `sync.ts` orchestrating remote orderbook syncs into SQLite. The unpacked `rain-orderbook-cli` binary sits in `bin/`, while `data/` holds network-specific databases. Root configs (`package.json`, `tsconfig.json`, `flake.nix`) define npm scripts, compiler settings, and the Nix developer environment. Generated JavaScript is emitted into `dist/`; only commit it when explicitly required.

## Build, Test, and Development Commands
Always run project commands inside the Nix dev shell so the expected toolchain is on PATH. Use `nix develop` to enter an interactive shell, then run `npm install`, `npm run build`, or `npm run sync -- --dry-run` from there. For one-off execution, prefer `nix develop -c npm run sync -- --dry-run`; drop `--dry-run` to perform a real sync. `npm run build` compiles TypeScript to `dist/`, and `npm run sync` drives the CLI to update local databases.

## Coding Style & Naming Conventions
TypeScript files use 2-space indentation, `camelCase` for values, and `PascalCase` for types or interfaces. Keep modules small and favor pure helpers where possible. The compiler runs with `strict` enabled, so annotate exported APIs and handle nullable branches explicitly. Reuse the ANSI logging helpers already in `sync.ts` for consistent console output.

## Testing Guidelines
There is no automated suite yet. Until one exists, rely on `npm run sync -- --dry-run` inside the Nix shell to validate argument parsing and plan generation without mutating databases. When adding tests, colocate them under `src/__tests__/`, name files `*.spec.ts`, and cover configuration loading, filesystem behaviour, and process orchestration via mocks.

## Commit & Pull Request Guidelines
With no prior history, adopt Conventional Commit prefixes (`feat:`, `fix:`, `chore:`) to keep logs searchable. Pull requests should summarize intent, list affected networks or data directories, and mention required environment variables (`HYPERLANE_API_TOKEN`, `RAIN_ORDERBOOK_API_TOKEN`, etc.). Include dry-run snippets when they demonstrate behaviour changes.

## Environment Notes
Consult `src/sync.ts` for the authoritative list of tokens and the default CLI commit hash. Keep secrets outside the repo and export them into your Nix shell before syncing. Avoid committing local SQLite artifacts unless explicitly requested.
