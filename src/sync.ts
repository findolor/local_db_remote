import https from "https";
import { spawnSync } from "child_process";
import { promises as fs } from "fs";
import { resolve, join } from "path";

interface CliOptions {
  networks: string[];
  dbDir: string;
  cliDir: string;
  keepArchive: boolean;
  dryRun: boolean;
}

interface NetworkSettings {
  chainId?: number;
  rpcs: string[];
}

interface OrderbookSettings {
  address?: string;
  deploymentBlock?: number;
}

interface ParsedSettings {
  networks: Record<string, NetworkSettings>;
  orderbooks: Record<string, OrderbookSettings>;
}

interface OrderbookConfig {
  network: string;
  chainId: number;
  orderbookAddress: string;
  deploymentBlock: number;
  rpcs: string[];
}

interface SyncPlan {
  dbPath: string;
  lastSyncedBlock: number | null;
  startBlock: number;
}

interface LogEntry {
  label: string;
  value: string;
}

const NUMBER_FORMATTER = new Intl.NumberFormat("en-US");

const DEFAULT_COMMIT_HASH = "3355912bf0052a7514ffb462e4a6655afb94347f";
const CLI_ARCHIVE_NAME = "rain-orderbook-cli.tar.gz";
const CONSTANTS_URL_TEMPLATE =
  "https://raw.githubusercontent.com/rainlanguage/rain.orderbook/{commit}/packages/webapp/src/lib/constants.ts";
const CLI_ARCHIVE_URL_TEMPLATE =
  "https://raw.githubusercontent.com/rainlanguage/rain.orderbook/{commit}/crates/cli/bin/rain-orderbook-cli.tar.gz";
const API_TOKEN_ENV_VARS = [
  "HYPERLANE_API_TOKEN",
  "RAIN_API_TOKEN",
  "RAIN_ORDERBOOK_API_TOKEN",
  "HYPERRPC_API_TOKEN",
] as const;

let sqliteWarningEmitted = false;

const ANSI = {
  reset: "\u001B[0m",
  bold: "\u001B[1m",
  dim: "\u001B[2m",
  cyan: "\u001B[36m",
  green: "\u001B[32m",
  magenta: "\u001B[35m",
  gray: "\u001B[90m",
} as const;

function colorText(text: string, ...codes: string[]): string {
  if (codes.length === 0) {
    return text;
  }
  return `${codes.join("")}${text}${ANSI.reset}`;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    networks: [],
    dbDir: "data",
    cliDir: "bin",
    keepArchive: false,
    dryRun: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case "--networks": {
        const values: string[] = [];
        let j = i + 1;
        while (j < argv.length && !argv[j].startsWith("--")) {
          values.push(argv[j]);
          j += 1;
        }
        if (values.length === 0) {
          throw new Error("--networks requires at least one value");
        }
        options.networks = values;
        i = j - 1;
        break;
      }
      case "--db-dir": {
        const value = argv[i + 1];
        if (!value) {
          throw new Error("--db-dir requires a path argument");
        }
        options.dbDir = value;
        i += 1;
        break;
      }
      case "--cli-dir": {
        const value = argv[i + 1];
        if (!value) {
          throw new Error("--cli-dir requires a path argument");
        }
        options.cliDir = value;
        i += 1;
        break;
      }
      case "--keep-archive": {
        options.keepArchive = true;
        break;
      }
      case "--dry-run": {
        options.dryRun = true;
        break;
      }
      case "--help":
      case "-h": {
        printUsage();
        process.exit(0);
      }
      default: {
        throw new Error(`Unknown argument: ${arg}`);
      }
    }
  }

  return options;
}

function printUsage(): void {
  console.log("Usage: ts-node src/sync.ts [options]");
  console.log("Options:");
  console.log(
    "  --networks <name...>   Limit sync to the provided network names",
  );
  console.log(
    "  --db-dir <path>        Directory to store SQLite databases (default: data)",
  );
  console.log(
    "  --cli-dir <path>       Directory to extract CLI binary (default: bin)",
  );
  console.log("  --keep-archive         Keep the downloaded CLI archive");
  console.log("  --dry-run              Print commands without executing them");
}

function fetchText(url: string): Promise<string> {
  return new Promise((resolvePromise, rejectPromise) => {
    const request = https.get(
      url,
      {
        headers: {
          "User-Agent": "rain-local-db-sync/1.0",
        },
      },
      (response) => {
        if (response.statusCode !== 200) {
          response.resume();
          rejectPromise(
            new Error(
              `Request to ${url} failed with status ${response.statusCode ?? "unknown"}`,
            ),
          );
          return;
        }

        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => {
          chunks.push(chunk);
        });
        response.on("end", () => {
          resolvePromise(Buffer.concat(chunks).toString("utf-8"));
        });
      },
    );

    request.on("error", (error) => {
      rejectPromise(error);
    });
  });
}

function fetchBinary(url: string): Promise<Buffer> {
  return new Promise((resolvePromise, rejectPromise) => {
    const request = https.get(
      url,
      {
        headers: {
          "User-Agent": "rain-local-db-sync/1.0",
        },
      },
      (response) => {
        if (response.statusCode !== 200) {
          response.resume();
          rejectPromise(
            new Error(
              `Request to ${url} failed with status ${response.statusCode ?? "unknown"}`,
            ),
          );
          return;
        }

        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => {
          chunks.push(chunk);
        });
        response.on("end", () => {
          resolvePromise(Buffer.concat(chunks));
        });
      },
    );

    request.on("error", (error) => {
      rejectPromise(error);
    });
  });
}

function extractSettingsUrl(constantsSource: string): string {
  const match = constantsSource.match(
    /REMOTE_SETTINGS_URL\s*=\s*['"]([^'"]+)['"]/,
  );
  if (!match) {
    throw new Error("Unable to locate REMOTE_SETTINGS_URL in constants source");
  }
  return match[1];
}

async function downloadCliArchive(
  commitHash: string,
  destination: string,
): Promise<string> {
  const url = CLI_ARCHIVE_URL_TEMPLATE.replace("{commit}", commitHash);
  const archiveBytes = await fetchBinary(url);
  await fs.writeFile(destination, archiveBytes);
  console.log(
    `Downloaded CLI archive to ${destination} (${archiveBytes.length} bytes)`,
  );
  return destination;
}

async function extractCliBinary(
  archivePath: string,
  outputDir: string,
): Promise<string> {
  await fs.mkdir(outputDir, { recursive: true });
  const extract = spawnSync("tar", ["-xzf", archivePath, "-C", outputDir], {
    stdio: "inherit",
  });
  if (extract.status !== 0) {
    throw new Error(
      `Failed to extract CLI archive (exit code ${extract.status ?? "unknown"})`,
    );
  }

  const candidate = await findBinary(outputDir);
  if (!candidate) {
    throw new Error(
      `Unable to locate rain-orderbook-cli binary under ${outputDir}`,
    );
  }

  await fs.chmod(candidate, 0o755);
  console.log(`Extracted CLI binary to ${candidate}`);
  return candidate;
}

async function findBinary(root: string): Promise<string | null> {
  const entries = await fs.readdir(root, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(root, entry.name);
    if (entry.isDirectory()) {
      const nested = await findBinary(fullPath);
      if (nested) {
        return nested;
      }
    } else if (entry.isFile() && entry.name === "rain-orderbook-cli") {
      return fullPath;
    }
  }
  return null;
}

function parseSettingsYaml(yamlText: string): ParsedSettings {
  const networks: Record<string, NetworkSettings> = {};
  const orderbooks: Record<string, OrderbookSettings> = {};

  let section: "networks" | "orderbooks" | null = null;
  let current: string | null = null;
  let listKey: string | null = null;

  const lines = yamlText.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.replace(/\s+$/, "");
    const trimmed = line.trim();

    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    if (trimmed === "networks:") {
      section = "networks";
      current = null;
      listKey = null;
      continue;
    }

    if (trimmed === "orderbooks:") {
      section = "orderbooks";
      current = null;
      listKey = null;
      continue;
    }

    const indent = line.search(/\S/);
    if (indent === 0) {
      section = null;
      current = null;
      listKey = null;
      continue;
    }

    if (
      (section === "networks" || section === "orderbooks") &&
      indent === 2 &&
      trimmed.endsWith(":")
    ) {
      current = trimmed.slice(0, -1).trim();
      listKey = null;
      if (section === "networks" && !networks[current]) {
        networks[current] = { rpcs: [] };
      }
      if (section === "orderbooks" && !orderbooks[current]) {
        orderbooks[current] = {};
      }
      continue;
    }

    if (!section || !current) {
      continue;
    }

    if (section === "networks") {
      if (indent === 4 && trimmed.endsWith(":")) {
        const key = trimmed.slice(0, -1).trim();
        if (key === "rpcs") {
          listKey = key;
          networks[current].rpcs = [];
        } else {
          listKey = null;
        }
        continue;
      }

      if (listKey === "rpcs" && indent >= 6 && trimmed.startsWith("- ")) {
        networks[current].rpcs.push(trimmed.slice(2).trim());
        continue;
      }

      const kvMatch = trimmed.match(/^([^:]+):\s*(.+)$/);
      if (!kvMatch) {
        continue;
      }

      const key = kvMatch[1].trim();
      const value = kvMatch[2].trim();

      if (key === "chain-id") {
        const parsed = Number(value);
        if (!Number.isNaN(parsed)) {
          networks[current].chainId = parsed;
        }
      }
    } else if (section === "orderbooks") {
      const kvMatch = trimmed.match(/^([^:]+):\s*(.+)$/);
      if (!kvMatch) {
        continue;
      }

      const key = kvMatch[1].trim();
      const value = kvMatch[2].trim();

      if (key === "address") {
        orderbooks[current].address = value;
      } else if (key === "deployment-block") {
        const parsed = Number(value);
        if (!Number.isNaN(parsed)) {
          orderbooks[current].deploymentBlock = parsed;
        }
      }
    }
  }

  return { networks, orderbooks };
}

function buildOrderbookConfigs(
  settings: ParsedSettings,
  selectedNetworks: string[],
): OrderbookConfig[] {
  const selected = new Set(selectedNetworks.map((name) => name.toLowerCase()));
  const configs: OrderbookConfig[] = [];

  for (const [network, orderbook] of Object.entries(settings.orderbooks)) {
    if (selected.size > 0 && !selected.has(network.toLowerCase())) {
      continue;
    }

    const networkInfo = settings.networks[network];
    if (!networkInfo) {
      console.log(`Skipping ${network}: missing network configuration`);
      continue;
    }
    if (networkInfo.chainId === undefined) {
      console.log(`Skipping ${network}: chain-id not defined`);
      continue;
    }
    if (!orderbook.address || orderbook.deploymentBlock === undefined) {
      console.log(`Skipping ${network}: orderbook data incomplete`);
      continue;
    }

    const normalizedRpcs = Array.from(
      new Set(
        networkInfo.rpcs
          .map((rpc) => rpc.trim())
          .filter((rpc) => rpc.length > 0),
      ),
    );

    if (normalizedRpcs.length === 0) {
      console.log(`Skipping ${network}: no RPC endpoints configured`);
      continue;
    }

    configs.push({
      network,
      chainId: networkInfo.chainId,
      orderbookAddress: orderbook.address,
      deploymentBlock: orderbook.deploymentBlock,
      rpcs: normalizedRpcs,
    });
  }

  return configs;
}

function resolveApiToken(): string | null {
  for (const name of API_TOKEN_ENV_VARS) {
    const value = process.env[name];
    if (value && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

function formatNumber(value: number): string {
  return NUMBER_FORMATTER.format(value);
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await fs.access(path);
    return true;
  } catch {
    return false;
  }
}

function warnIfSqliteMissing(error: Error | undefined): void {
  if (!error || sqliteWarningEmitted) {
    return;
  }

  const nodeError = error as NodeJS.ErrnoException;
  if (nodeError.code === "ENOENT") {
    console.log(
      "⚠️  sqlite3 CLI not found; skipping local sync-status inspection.",
    );
    sqliteWarningEmitted = true;
  }
}

async function getLastSyncedBlock(dbPath: string): Promise<number | null> {
  if (!(await pathExists(dbPath))) {
    return null;
  }

  const tableResult = spawnSync(
    "sqlite3",
    [
      "-readonly",
      dbPath,
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_status' LIMIT 1;",
    ],
    { encoding: "utf8" },
  );
  warnIfSqliteMissing(tableResult.error);
  if (
    tableResult.error ||
    tableResult.status !== 0 ||
    tableResult.stdout.trim() !== "1"
  ) {
    return null;
  }

  const pragmaResult = spawnSync(
    "sqlite3",
    [
      "-readonly",
      "-separator",
      "|",
      dbPath,
      "PRAGMA table_info('sync_status');",
    ],
    { encoding: "utf8" },
  );
  warnIfSqliteMissing(pragmaResult.error);
  if (pragmaResult.error || pragmaResult.status !== 0) {
    return null;
  }

  const columnName = pragmaResult.stdout
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.split("|")[1])
    .find(
      (name) =>
        typeof name === "string" && name.toLowerCase().includes("block"),
    );

  if (!columnName) {
    return null;
  }

  const queryResult = spawnSync(
    "sqlite3",
    [
      "-readonly",
      dbPath,
      `SELECT ${quoteIdentifier(columnName)} FROM sync_status ORDER BY ${quoteIdentifier(columnName)} DESC LIMIT 1;`,
    ],
    { encoding: "utf8" },
  );
  warnIfSqliteMissing(queryResult.error);

  if (queryResult.error || queryResult.status !== 0) {
    return null;
  }

  const value = Number(queryResult.stdout.trim());
  return Number.isNaN(value) ? null : value;
}

async function planSync(
  config: OrderbookConfig,
  dbDir: string,
): Promise<SyncPlan> {
  const dbPath = join(dbDir, `${config.network}.db`);
  const lastSyncedBlock = await getLastSyncedBlock(dbPath);
  const startCandidate =
    lastSyncedBlock !== null ? lastSyncedBlock + 1 : config.deploymentBlock;
  const startBlock = Math.max(config.deploymentBlock, startCandidate);

  return {
    dbPath,
    lastSyncedBlock,
    startBlock,
  };
}

function logBlock(title: string, entries: LogEntry[]): void {
  const safeEntries = entries.length > 0 ? entries : [{ label: "Info", value: "(no data)" }];
  const labelWidth = Math.max(...safeEntries.map((entry) => entry.label.length));
  const valueWidth = Math.max(...safeEntries.map((entry) => entry.value.length));
  const contentWidth = Math.max(title.length, labelWidth + 3 + valueWidth);
  const horizontal = "─".repeat(contentWidth + 2);

  console.log(`\n┌${horizontal}┐`);
  const coloredTitle = colorText(title.padEnd(contentWidth), ANSI.bold, ANSI.green);
  console.log(`│ ${coloredTitle} │`);
  console.log(`├${horizontal}┤`);

  for (const entry of safeEntries) {
    const paddedLabel = entry.label.padEnd(labelWidth);
    const coloredLabel = colorText(paddedLabel, ANSI.bold, ANSI.cyan);
    const separator = colorText(" : ", ANSI.bold, ANSI.gray);
    const coloredValue = colorText(entry.value, ANSI.magenta);
    const lineLength = labelWidth + 3 + entry.value.length;
    const padding = " ".repeat(contentWidth - lineLength);
    console.log(`│ ${coloredLabel}${separator}${coloredValue}${padding} │`);
  }

  console.log(`└${horizontal}┘`);
}

function logPlan(config: OrderbookConfig, plan: SyncPlan): void {
  const entries: LogEntry[] = [
    { label: "Database path", value: plan.dbPath },
    { label: "Orderbook", value: config.orderbookAddress },
    { label: "Chain ID", value: String(config.chainId) },
    { label: "Deployment block", value: formatNumber(config.deploymentBlock) },
    {
      label: "Last synced block",
      value:
        plan.lastSyncedBlock !== null
          ? formatNumber(plan.lastSyncedBlock)
          : "none",
    },
    { label: "Start block", value: formatNumber(plan.startBlock) },
    { label: "Blocks to fetch", value: "determined by CLI" },
    { label: "RPC endpoints", value: String(config.rpcs.length) },
  ];

  config.rpcs.forEach((rpc, index) => {
    entries.push({ label: `RPC[${index + 1}]`, value: rpc });
  });

  logBlock(`Plan for ${config.network}`, entries);
}

async function runCliSync(
  cliBinary: string,
  config: OrderbookConfig,
  dbDir: string,
  apiToken: string | null,
  dryRun: boolean,
): Promise<void> {
  await fs.mkdir(dbDir, { recursive: true });
  const dbPath = join(dbDir, `${config.network}.db`);
  const args = [
    "local-db",
    "sync",
    "--db-path",
    dbPath,
    "--chain-id",
    String(config.chainId),
    "--orderbook-address",
    config.orderbookAddress,
    "--deployment-block",
    String(config.deploymentBlock),
  ];

  if (!apiToken) {
    throw new Error(
      `No API token provided for ${config.network}. Set one of: ${API_TOKEN_ENV_VARS.join(
        ", ",
      )}`,
    );
  }

  if (config.rpcs.length === 0) {
    throw new Error(
      `No RPC URLs configured for ${config.network}. Update settings.yaml or provide overrides.`,
    );
  }

  args.push("--api-token", apiToken);

  const seen = new Set<string>();
  for (const rpc of config.rpcs) {
    if (seen.has(rpc)) {
      continue;
    }
    args.push("--rpc", rpc);
    seen.add(rpc);
  }

  console.log("Running:", [cliBinary, ...args].join(" "));
  if (dryRun) {
    console.log("Dry-run mode enabled; CLI command skipped.");
    return;
  }

  const result = spawnSync(cliBinary, args, { stdio: "inherit" });
  if (result.status !== 0) {
    throw new Error(
      `CLI sync failed for ${config.network} (exit code ${result.status ?? "unknown"})`,
    );
  }
}

async function main(): Promise<void> {
  const startTime = new Date();
  console.log(`Sync started at ${startTime.toISOString()}`);

  const options = parseArgs(process.argv.slice(2));
  const commitHash = (process.env.COMMIT_HASH ?? DEFAULT_COMMIT_HASH).trim();
  if (commitHash.length === 0) {
    throw new Error(
      "COMMIT_HASH must be set to a valid rain.orderbook commit hash",
    );
  }
  console.log(`Using commit hash ${commitHash}`);

  const constantsUrl = CONSTANTS_URL_TEMPLATE.replace("{commit}", commitHash);
  console.log(`Fetching constants from ${constantsUrl}`);
  const constantsSource = await fetchText(constantsUrl);
  const settingsUrl = extractSettingsUrl(constantsSource);
  console.log(`Fetching settings from ${settingsUrl}`);
  const settingsYaml = await fetchText(settingsUrl);

  const archivePath = resolve(process.cwd(), CLI_ARCHIVE_NAME);
  await downloadCliArchive(commitHash, archivePath);
  const cliDir = resolve(process.cwd(), options.cliDir);
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

  const apiToken = resolveApiToken();
  if (!apiToken) {
    throw new Error(
      `Missing API token. Set one of: ${API_TOKEN_ENV_VARS.join(", ")}.`,
    );
  }
  console.log("Using API token sourced from environment.");

  const dbDir = resolve(process.cwd(), options.dbDir);
  await fs.mkdir(dbDir, { recursive: true });

  for (const config of configs) {
    const plan = await planSync(config, dbDir);
    logPlan(config, plan);
    await runCliSync(cliBinary, config, dbDir, apiToken, options.dryRun);
  }

  const endTime = new Date();
  const elapsedMs = endTime.getTime() - startTime.getTime();
  console.log(
    `Sync completed at ${endTime.toISOString()} (duration: ${(elapsedMs / 1000).toFixed(1)}s)`,
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
