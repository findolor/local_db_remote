import { spawnSync } from "child_process";
import { promises as fs } from "fs";
import { dirname } from "path";

import { API_TOKEN_ENV_VARS } from "./constants";
import type { OrderbookConfig } from "./settings";

export async function runCliSync(
  cliBinary: string,
  config: OrderbookConfig,
  dbPath: string,
  apiToken: string | null,
): Promise<void> {
  await fs.mkdir(dirname(dbPath), { recursive: true });
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
      `No API token provided for ${config.network}. Set one of: ${API_TOKEN_ENV_VARS.join(", ")}`,
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
  const result = spawnSync(cliBinary, args, { stdio: "inherit" });
  if (result.status !== 0) {
    throw new Error(
      `CLI sync failed for ${config.network} (exit code ${result.status ?? "unknown"})`,
    );
  }
}
