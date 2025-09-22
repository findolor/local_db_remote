import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "fs";

vi.mock("child_process", () => ({
  spawnSync: vi.fn(),
}));

import { spawnSync } from "child_process";

import { finalizeDatabase, planSync, prepareDatabase } from "../database";
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

describe("prepareDatabase", () => {
  it("removes existing db, extracts archive when available", async () => {
    const mkdirSpy = vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    const unlinkSpy = vi.spyOn(fs, "unlink").mockResolvedValue(undefined);
    const accessSpy = vi.spyOn(fs, "access").mockImplementation(async (path) => {
      if (typeof path === "string" && path.endsWith("optimism.db")) {
        return undefined;
      }
      if (typeof path === "string" && path.endsWith("optimism.db.tar.gz")) {
        return undefined;
      }
      throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
    });
    spawnSyncMock.mockReturnValue({ status: 0 } as unknown as ReturnType<typeof spawnSync>);

    const result = await prepareDatabase("optimism", "/tmp");

    expect(result).toEqual({
      dbPath: "/tmp/optimism.db",
      dumpPath: "/tmp/optimism.db.tar.gz",
    });
    expect(unlinkSpy).toHaveBeenCalledWith("/tmp/optimism.db");
    expect(spawnSyncMock).toHaveBeenCalledWith("tar", ["-xzf", "/tmp/optimism.db.tar.gz", "-C", "/tmp"], {
      stdio: "inherit",
    });

    mkdirSpy.mockRestore();
    unlinkSpy.mockRestore();
    accessSpy.mockRestore();
  });

  it("throws when tar extraction fails", async () => {
    vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    vi.spyOn(fs, "access").mockImplementation(async (path) => {
      if (typeof path === "string" && path.endsWith(".tar.gz")) {
        return undefined;
      }
      throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
    });
    spawnSyncMock.mockReturnValue({ status: 2 } as unknown as ReturnType<typeof spawnSync>);

    await expect(prepareDatabase("optimism", "/tmp")).rejects.toThrow(
      "Failed to extract dump",
    );
  });
});

describe("finalizeDatabase", () => {
  it("archives the sqlite db and cleans up temp files", async () => {
    const accessSpy = vi.spyOn(fs, "access").mockResolvedValue(undefined);
    const unlinkSpy = vi.spyOn(fs, "unlink").mockResolvedValue(undefined);
    const renameSpy = vi.spyOn(fs, "rename").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 0 } as unknown as ReturnType<typeof spawnSync>);

    await finalizeDatabase("optimism", "/tmp/optimism.db", "/tmp/optimism.db.tar.gz");

    expect(spawnSyncMock).toHaveBeenCalledWith(
      "tar",
      [
        "-czf",
        "/tmp/optimism.db.tar.gz.tmp",
        "-C",
        "/tmp",
        "optimism.db",
      ],
      { stdio: "inherit" },
    );
    expect(renameSpy).toHaveBeenCalledWith(
      "/tmp/optimism.db.tar.gz.tmp",
      "/tmp/optimism.db.tar.gz",
    );
    expect(unlinkSpy).toHaveBeenCalledWith("/tmp/optimism.db");

    accessSpy.mockRestore();
    unlinkSpy.mockRestore();
    renameSpy.mockRestore();
  });

  it("skips archiving when database does not exist", async () => {
    const accessSpy = vi.spyOn(fs, "access").mockRejectedValue(Object.assign(new Error("ENOENT"), { code: "ENOENT" }));
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

    await finalizeDatabase("optimism", "/tmp/optimism.db", "/tmp/optimism.db.tar.gz");

    expect(spawnSyncMock).not.toHaveBeenCalled();
    expect(logSpy).toHaveBeenCalledWith(
      "No database file produced for optimism; skipping archive.",
    );

    accessSpy.mockRestore();
    logSpy.mockRestore();
  });

  it("cleans up temp archive when tar fails", async () => {
    vi.spyOn(fs, "access").mockResolvedValue(undefined);
    const unlinkSpy = vi.spyOn(fs, "unlink").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 1 } as unknown as ReturnType<typeof spawnSync>);

    await expect(
      finalizeDatabase("optimism", "/tmp/optimism.db", "/tmp/optimism.db.tar.gz"),
    ).rejects.toThrow("Failed to archive database for optimism");

    expect(unlinkSpy).toHaveBeenCalledWith("/tmp/optimism.db.tar.gz.tmp");

    unlinkSpy.mockRestore();
  });
});
