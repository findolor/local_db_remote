import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "fs";

vi.mock("child_process", () => ({
  spawnSync: vi.fn(),
}));

import { spawnSync } from "child_process";

import { runCliSync, type RunCliSyncOptions } from "../cli";

const spawnSyncMock = vi.mocked(spawnSync);

const baseOptions: RunCliSyncOptions = {
  cliBinary: "/bin/rain",
  network: "optimism",
  dbPath: "/tmp/db.sqlite",
  apiToken: "token-123",
  configCommit: "deadbeef",
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("runCliSync", () => {
  it("spawns the CLI with config commit and network", async () => {
    const mkdirSpy = vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 0 } as unknown as ReturnType<typeof spawnSync>);

    await runCliSync(baseOptions);

    expect(mkdirSpy).toHaveBeenCalledWith("/tmp", { recursive: true });
    expect(spawnSyncMock).toHaveBeenCalledWith(
      "/bin/rain",
      [
        "local-db",
        "sync",
        "--db-path",
        "/tmp/db.sqlite",
        "--network",
        "optimism",
        "--config-commit",
        "deadbeef",
        "--api-token",
        "token-123",
      ],
      { stdio: "inherit" },
    );

    mkdirSpy.mockRestore();
  });

  it("includes optional start and end blocks when provided", async () => {
    vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 0 } as unknown as ReturnType<typeof spawnSync>);

    await runCliSync({ ...baseOptions, startBlock: 100, endBlock: 200 });

    expect(spawnSyncMock).toHaveBeenCalledWith(
      "/bin/rain",
      expect.arrayContaining([
        "--start-block",
        "100",
        "--end-block",
        "200",
      ]),
      { stdio: "inherit" },
    );
  });

  it("throws when API token is missing", async () => {
    await expect(runCliSync({ ...baseOptions, apiToken: null })).rejects.toThrow(
      "No API token provided",
    );
  });

  it("throws when spawn returns a non-zero status", async () => {
    vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 1 } as unknown as ReturnType<typeof spawnSync>);

    await expect(runCliSync(baseOptions)).rejects.toThrow("CLI sync failed");
  });
});
