import { promises as fs } from "fs";
import { resolve } from "path";

import { downloadCliArchive, extractCliBinary } from "./archive";
import { runCliSync } from "./cli";
import {
  API_TOKEN_ENV_VARS,
  CLI_ARCHIVE_NAME,
  CONSTANTS_URL_TEMPLATE,
  DEFAULT_COMMIT_HASH,
} from "./constants";
import { defaultHttpClient, type HttpClient } from "./http";
import { logPlan } from "./logging";
import { parseArgs, printUsage } from "./options";
import { buildOrderbookConfigs, extractSettingsUrl, parseSettingsYaml } from "./settings";
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

  const constantsUrl = CONSTANTS_URL_TEMPLATE.replace("{commit}", commitHash);
  console.log(`Fetching constants from ${constantsUrl}`);
  const constantsSource = await runtime.http.fetchText(constantsUrl);
  const settingsUrl = extractSettingsUrl(constantsSource);
  console.log(`Fetching settings from ${settingsUrl}`);
  const settingsYaml = await runtime.http.fetchText(settingsUrl);

  const archivePath = resolve(runtime.cwd(), CLI_ARCHIVE_NAME);
  await downloadCliArchive(runtime.http, commitHash, archivePath);
  const cliDir = resolve(runtime.cwd(), options.cliDir);
  const cliBinary = await extractCliBinary(archivePath, cliDir);

  if (!options.keepArchive) {
    await fs.unlink(archivePath).catch(() => undefined);
  }

  const parsedSettings = parseSettingsYaml(settingsYaml);
  const configs = buildOrderbookConfigs(parsedSettings, options.networks);
  if (configs.length === 0) {
    console.log("No orderbook configurations matched the selection.");
    return;
  }

  const apiToken = resolveApiToken(runtime.env);
  if (!apiToken) {
    throw new Error(`Missing API token. Set one of: ${API_TOKEN_ENV_VARS.join(", ")}.`);
  }
  console.log("Using API token sourced from environment.");

  const dbDir = resolve(runtime.cwd(), options.dbDir);
  await fs.mkdir(dbDir, { recursive: true });

  for (const config of configs) {
    const { dbPath, dumpPath } = await prepareDatabase(config.network, dbDir);
    try {
      const plan = await planSync(config, dbPath, dumpPath);
      logPlan(config, plan);
      await runCliSync(cliBinary, config, dbPath, apiToken);
      await finalizeDatabase(config.network, dbPath, dumpPath);
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

if (require.main === module) {
  main().catch(() => {
    process.exit(1);
  });
}
