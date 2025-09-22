import { spawnSync } from "child_process";
import { promises as fs } from "fs";
import { basename, dirname, join } from "path";

import type { OrderbookConfig } from "./settings";

let sqliteWarningEmitted = false;

export interface SyncPlan {
  dbPath: string;
  dumpPath: string;
  lastSyncedBlock: number | null;
  startBlock: number;
}

export async function prepareDatabase(
  network: string,
  dbDir: string,
): Promise<{ dbPath: string; dumpPath: string }> {
  const dbPath = join(dbDir, `${network}.db`);
  const dumpPath = join(dbDir, `${network}.db.tar.gz`);

  await fs.mkdir(dbDir, { recursive: true });

  if (await pathExists(dbPath)) {
    await fs.unlink(dbPath);
  }

  if (await pathExists(dumpPath)) {
    console.log(`Extracting dump for ${network} from ${dumpPath}`);
    const extract = spawnSync("tar", ["-xzf", dumpPath, "-C", dbDir], {
      stdio: "inherit",
    });
    if (extract.status !== 0) {
      throw new Error(
        `Failed to extract dump for ${network} (exit code ${extract.status ?? "unknown"})`,
      );
    }
  } else {
    console.log(`No existing dump for ${network}; CLI will initialize a new database.`);
  }

  return { dbPath, dumpPath };
}

export async function finalizeDatabase(
  network: string,
  dbPath: string,
  dumpPath: string,
): Promise<void> {
  const dbExists = await pathExists(dbPath);

  if (!dbExists) {
    console.log(`No database file produced for ${network}; skipping archive.`);
    return;
  }

  const tempDumpPath = `${dumpPath}.tmp`;
  console.log(`Archiving database for ${network} to ${dumpPath}`);
  const pack = spawnSync(
    "tar",
    [
      "-czf",
      tempDumpPath,
      "-C",
      dirname(dbPath),
      basename(dbPath),
    ],
    { stdio: "inherit" },
  );
  if (pack.status !== 0) {
    await fs.unlink(tempDumpPath).catch(() => undefined);
    throw new Error(
      `Failed to archive database for ${network} (exit code ${pack.status ?? "unknown"})`,
    );
  }

  await fs.unlink(dumpPath).catch(() => undefined);
  await fs.rename(tempDumpPath, dumpPath);
  await fs.unlink(dbPath).catch(() => undefined);
}

export async function planSync(
  config: OrderbookConfig,
  dbPath: string,
  dumpPath: string,
): Promise<SyncPlan> {
  const lastSyncedBlock = await getLastSyncedBlock(dbPath);
  const startCandidate =
    lastSyncedBlock !== null ? lastSyncedBlock + 1 : config.deploymentBlock;
  const startBlock = Math.max(config.deploymentBlock, startCandidate);

  return {
    dbPath,
    dumpPath,
    lastSyncedBlock,
    startBlock,
  };
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

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}
