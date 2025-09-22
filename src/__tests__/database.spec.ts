import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "fs";

vi.mock("child_process", () => ({
  spawnSync: vi.fn(),
}));

import { spawnSync } from "child_process";

import { planSync } from "../database";
import type { OrderbookConfig } from "../settings";

const spawnSyncMock = vi.mocked(spawnSync);

function makeConfig(overrides: Partial<OrderbookConfig> = {}): OrderbookConfig {
  return {
    network: "optimism",
    chainId: 10,
    orderbookAddress: "0xorderbook",
    deploymentBlock: 1200,
    rpcs: ["https://rpc.optimism.io"],
    ...overrides,
  };
}

function spawnResult(stdout: string, status = 0) {
  return {
    pid: 0,
    output: [stdout],
    stdout,
    stderr: "",
    status,
    signal: null,
  } as ReturnType<typeof spawnSync>;
}

beforeEach(() => {
  spawnSyncMock.mockReset();
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("planSync", () => {
  it("starts from deployment block when database is absent", async () => {
    const accessSpy = vi.spyOn(fs, "access").mockRejectedValue(new Error("ENOENT"));

    const plan = await planSync(makeConfig(), "/tmp/optimism.db", "/tmp/optimism.db.tar.gz");

    expect(plan.lastSyncedBlock).toBeNull();
    expect(plan.startBlock).toBe(1200);
    expect(spawnSyncMock).not.toHaveBeenCalled();
    accessSpy.mockRestore();
  });

  it("continues from the next block after the last synced block", async () => {
    const accessSpy = vi.spyOn(fs, "access").mockResolvedValue(undefined);
    spawnSyncMock
      .mockReturnValueOnce(spawnResult("1\n"))
      .mockReturnValueOnce(
        spawnResult("0|id|INTEGER\n1|last_block|INTEGER\n")
      )
      .mockReturnValueOnce(spawnResult("2500\n"));

    const config = makeConfig({ deploymentBlock: 2400 });
    const plan = await planSync(config, "/tmp/optimism.db", "/tmp/optimism.db.tar.gz");

    expect(plan.lastSyncedBlock).toBe(2500);
    expect(plan.startBlock).toBe(2501);
    expect(spawnSyncMock).toHaveBeenCalledTimes(3);
    accessSpy.mockRestore();
  });

  it("never starts before the deployment block", async () => {
    const accessSpy = vi.spyOn(fs, "access").mockResolvedValue(undefined);
    spawnSyncMock
      .mockReturnValueOnce(spawnResult("1\n"))
      .mockReturnValueOnce(
        spawnResult("0|id|INTEGER\n1|last_block|INTEGER\n")
      )
      .mockReturnValueOnce(spawnResult("50\n"));

    const config = makeConfig({ deploymentBlock: 100 });
    const plan = await planSync(config, "/tmp/optimism.db", "/tmp/optimism.db.tar.gz");

    expect(plan.lastSyncedBlock).toBe(50);
    expect(plan.startBlock).toBe(100);
    accessSpy.mockRestore();
  });
});
