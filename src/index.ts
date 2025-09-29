import { promises as fs } from "fs";
import { resolve } from "path";

import { downloadCliArchive, extractCliBinary } from "./archive";
import { runCliSync } from "./cli";
import { API_TOKEN_ENV_VARS, CLI_ARCHIVE_NAME, DEFAULT_COMMIT_HASH } from "./constants";
import { defaultHttpClient, type HttpClient } from "./http";
import { logPlan } from "./logging";
import { parseArgs, printUsage } from "./options";
import { finalizeDatabase, planSync, prepareDatabase } from "./database";

export interface SyncRuntime {
  argv: string[];
  env: NodeJS.ProcessEnv;
  cwd: () => string;
  exit: (code: number) => void;
  http: HttpClient;
}

const defaultRuntime: SyncRuntime = {
  argv: process.argv.slice(2),
  env: process.env,
  cwd: () => process.cwd(),
  exit: (code: number) => process.exit(code),
  http: defaultHttpClient,
};

export async function runSync(partialRuntime: Partial<SyncRuntime> = {}): Promise<void> {
  const runtime: SyncRuntime = { ...defaultRuntime, ...partialRuntime };
  const { options, helpRequested } = parseArgs(runtime.argv);

  if (helpRequested) {
    printUsage();
    runtime.exit(0);
    return;
  }

  const startTime = new Date();
  console.log(`Sync started at ${startTime.toISOString()}`);

  const commitHash = (runtime.env.COMMIT_HASH ?? DEFAULT_COMMIT_HASH).trim();
  if (commitHash.length === 0) {
    throw new Error("COMMIT_HASH must be set to a valid rain.orderbook commit hash");
  }
  console.log(`Using commit hash ${commitHash}`);

  const archivePath = resolve(runtime.cwd(), CLI_ARCHIVE_NAME);
  await downloadCliArchive(runtime.http, commitHash, archivePath);
  const cliDir = resolve(runtime.cwd(), options.cliDir);
  const cliBinary = await extractCliBinary(archivePath, cliDir);

  if (!options.keepArchive) {
    await fs.unlink(archivePath).catch(() => undefined);
  }

  const apiToken = resolveApiToken(runtime.env);
  if (!apiToken) {
    throw new Error(`Missing API token. Set one of: ${API_TOKEN_ENV_VARS.join(", ")}.`);
  }
  console.log("Using API token sourced from environment.");

  const dbDir = resolve(runtime.cwd(), options.dbDir);
  await fs.mkdir(dbDir, { recursive: true });

  const networks = await resolveNetworks(options.networks, dbDir);
  if (networks.length === 0) {
    console.log("No networks selected. Provide --networks or ensure dumps exist in the data directory.");
    return;
  }

  for (const network of networks) {
    const { dbPath, dumpPath } = await prepareDatabase(network, dbDir);
    try {
      const plan = await planSync(dbPath, dumpPath);
      logPlan(network, plan);
      await runCliSync({
        cliBinary,
        network,
        dbPath,
        apiToken,
        configCommit: commitHash,
        startBlock: plan.nextStartBlock ?? undefined,
      });
      await finalizeDatabase(network, dbPath, dumpPath);
    } finally {
      await fs.unlink(dbPath).catch(() => undefined);
    }
  }

  const endTime = new Date();
  const elapsedMs = endTime.getTime() - startTime.getTime();
  console.log(
    `Sync completed at ${endTime.toISOString()} (duration: ${(elapsedMs / 1000).toFixed(1)}s)`,
  );
}

export async function main(partialRuntime: Partial<SyncRuntime> = {}): Promise<void> {
  try {
    await runSync(partialRuntime);
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    throw error;
  }
}

function resolveApiToken(env: NodeJS.ProcessEnv): string | null {
  for (const name of API_TOKEN_ENV_VARS) {
    const value = env[name];
    if (value && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

async function resolveNetworks(selected: string[], dbDir: string): Promise<string[]> {
  if (selected.length > 0) {
    return Array.from(
      new Set(
        selected
          .map((name) => name.trim())
          .filter((name) => name.length > 0),
      ),
    );
  }

  const entries = await fs.readdir(dbDir).catch(() => [] as string[]);
  return Array.from(
    new Set(
      entries
        .filter((name) => name.endsWith(".db.tar.gz"))
        .map((name) => name.replace(/\.db\.tar\.gz$/, ""))
        .filter((name) => name.length > 0),
    ),
  );
}

if (require.main === module) {
  main().catch(() => {
    process.exit(1);
  });
}
