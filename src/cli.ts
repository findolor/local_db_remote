import { spawnSync } from "child_process";
import { promises as fs } from "fs";
import { dirname } from "path";

import { API_TOKEN_ENV_VARS } from "./constants";

export interface RunCliSyncOptions {
  cliBinary: string;
  network: string;
  dbPath: string;
  apiToken: string | null;
  configCommit: string;
  startBlock?: number | null;
  endBlock?: number | null;
}

export async function runCliSync(options: RunCliSyncOptions): Promise<void> {
  const { cliBinary, network, dbPath, apiToken, configCommit, startBlock, endBlock } = options;
  await fs.mkdir(dirname(dbPath), { recursive: true });
  const args = [
    "local-db",
    "sync",
    "--db-path",
    dbPath,
    "--network",
    network,
    "--config-commit",
    configCommit,
  ];

  if (!apiToken) {
    throw new Error(
      `No API token provided for ${network}. Set one of: ${API_TOKEN_ENV_VARS.join(", ")}`,
    );
  }

  args.push("--api-token", apiToken);

  if (startBlock !== undefined && startBlock !== null) {
    args.push("--start-block", String(startBlock));
  }

  if (endBlock !== undefined && endBlock !== null) {
    args.push("--end-block", String(endBlock));
  }

  console.log("Running:", [cliBinary, ...args].join(" "));
  const result = spawnSync(cliBinary, args, { stdio: "inherit" });
  if (result.status !== 0) {
    throw new Error(
      `CLI sync failed for ${network} (exit code ${result.status ?? "unknown"})`,
    );
  }
}
