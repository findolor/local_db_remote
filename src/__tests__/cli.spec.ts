import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "fs";

vi.mock("child_process", () => ({
  spawnSync: vi.fn(),
}));

import { spawnSync } from "child_process";

import { runCliSync } from "../cli";
import type { OrderbookConfig } from "../settings";

const spawnSyncMock = vi.mocked(spawnSync);

const baseConfig: OrderbookConfig = {
  network: "optimism",
  chainId: 10,
  orderbookAddress: "0xorderbook",
  deploymentBlock: 1000,
  rpcs: ["https://rpc.optimism.io", "https://rpc.optimism.io"],
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("runCliSync", () => {
  it("spawns the CLI with deduplicated RPCs", async () => {
    const mkdirSpy = vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 0 } as unknown as ReturnType<typeof spawnSync>);

    await runCliSync("/bin/rain", baseConfig, "/tmp/db.sqlite", "token-123");

    expect(mkdirSpy).toHaveBeenCalledWith("/tmp", { recursive: true });
    expect(spawnSyncMock).toHaveBeenCalledWith(
      "/bin/rain",
      [
        "local-db",
        "sync",
        "--db-path",
        "/tmp/db.sqlite",
        "--chain-id",
        "10",
        "--orderbook-address",
        "0xorderbook",
        "--deployment-block",
        "1000",
        "--api-token",
        "token-123",
        "--rpc",
        "https://rpc.optimism.io",
      ],
      { stdio: "inherit" },
    );

    mkdirSpy.mockRestore();
  });

  it("throws when API token is missing", async () => {
    await expect(runCliSync("/bin/rain", baseConfig, "/tmp/db.sqlite", null)).rejects.toThrow(
      "No API token provided",
    );
  });

  it("throws when spawn returns a non-zero status", async () => {
    vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 1 } as unknown as ReturnType<typeof spawnSync>);

    await expect(
      runCliSync("/bin/rain", baseConfig, "/tmp/db.sqlite", "token-123"),
    ).rejects.toThrow("CLI sync failed");
  });
});
