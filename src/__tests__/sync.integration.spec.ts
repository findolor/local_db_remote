import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, rm, writeFile, access, stat, mkdir } from "fs/promises";
import { tmpdir } from "os";
import { basename, dirname, join } from "path";
import * as fs from "fs";

type ChildProcessModule = typeof import("child_process");
type SpawnSyncResult = ReturnType<ChildProcessModule["spawnSync"]>;
type SpawnSyncMockFn = (command: unknown, args?: unknown) => SpawnSyncResult;

const { spawnSyncMock } = vi.hoisted(() => ({
  spawnSyncMock: vi.fn<SpawnSyncMockFn>(),
}));

vi.mock("child_process", () => {
  const actual = require("child_process") as typeof import("child_process");
  return {
    ...actual,
    spawnSync: spawnSyncMock,
  };
});

import { runSync } from "../index";
import type { HttpClient } from "../http";
import { CLI_ARCHIVE_NAME } from "../constants";

describe("runSync integration", () => {
  let tempDir: string;
  let sqliteResponses: Array<{ stdout: string; status: number }>;

  const makeSpawnResult = (stdout = "", status = 0): SpawnSyncResult =>
    ({
      pid: 0,
      output: [],
      stdout,
      stderr: "",
      status,
      signal: null,
    }) as SpawnSyncResult;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "rain-sync-int-"));
    spawnSyncMock.mockReset();
    sqliteResponses = [
      { stdout: "1\n", status: 0 },
      { stdout: "0|id|INTEGER\n1|last_block|INTEGER\n", status: 0 },
      { stdout: "2500\n", status: 0 },
    ];
  });

  afterEach(async () => {
    vi.clearAllMocks();
    spawnSyncMock.mockReset();
    await rm(tempDir, { recursive: true, force: true });
  });

  it("coordinates sync with stubbed external tools", async () => {
    const http: HttpClient = {
      fetchText: vi.fn(async () => {
        throw new Error("unexpected fetchText call");
      }),
      fetchBinary: vi.fn(async () => Buffer.from("fake-tar")),
    };

    const dataDir = join(tempDir, "data");
    await mkdir(dataDir, { recursive: true });

    const existingDump = join(dataDir, "Optimism.db.tar.gz");
    await writeFile(existingDump, "old-dump");

    spawnSyncMock.mockImplementation((command, args) => {
      if (command === "tar" && Array.isArray(args) && args[0] === "-xzf") {
        const archive = String(args[1]);
        const destIndex = args.indexOf("-C");
        const destination = destIndex >= 0 ? String(args[destIndex + 1]) : tempDir;
        if (destination.endsWith("bin")) {
          fs.mkdirSync(destination, { recursive: true });
          const cliPath = join(destination, "rain-orderbook-cli");
          fs.writeFileSync(cliPath, "#!/bin/sh\necho stub\n");
          fs.chmodSync(cliPath, 0o755);
        } else {
          fs.mkdirSync(destination, { recursive: true });
          const dbFile = basename(archive).replace(/\.tar\.gz$/, "");
          fs.writeFileSync(join(destination, dbFile), "extracted-db");
        }
        return makeSpawnResult();
      }

      if (command === "tar" && Array.isArray(args) && args[0] === "-czf") {
        const tempArchive = String(args[1]);
        fs.mkdirSync(dirname(tempArchive), { recursive: true });
        fs.writeFileSync(tempArchive, "new-archive");
        return makeSpawnResult();
      }

      if (typeof command === "string" && command.includes("rain-orderbook-cli")) {
        const dbFlag = Array.isArray(args) ? args.indexOf("--db-path") : -1;
        if (Array.isArray(args) && dbFlag >= 0) {
          const dbPath = String(args[dbFlag + 1]);
          fs.mkdirSync(dirname(dbPath), { recursive: true });
          fs.writeFileSync(dbPath, "cli-output-db");
        }
        return makeSpawnResult();
      }

      if (command === "sqlite3") {
        const response = sqliteResponses.shift() ?? { stdout: "", status: 1 };
        return makeSpawnResult(response.stdout, response.status);
      }

      return makeSpawnResult();
    });

    await runSync({
      argv: [],
      env: {
        COMMIT_HASH: "feedface",
        RAIN_API_TOKEN: "token",
      },
      cwd: () => tempDir,
      exit: vi.fn(),
      http,
    });

    await expect(access(join(tempDir, CLI_ARCHIVE_NAME))).rejects.toThrow();

    const cliBinaryPath = join(tempDir, "bin", "rain-orderbook-cli");
    await expect(access(cliBinaryPath)).resolves.toBeUndefined();

    const finalDump = join(tempDir, "data", "Optimism.db.tar.gz");
    await expect(stat(finalDump)).resolves.toMatchObject({ size: expect.any(Number) });
    await expect(access(join(tempDir, "data", "Optimism.db"))).rejects.toThrow();

    expect(http.fetchText).not.toHaveBeenCalled();
    expect(http.fetchBinary).toHaveBeenCalledWith(
      "https://raw.githubusercontent.com/rainlanguage/rain.orderbook/feedface/crates/cli/bin/rain-orderbook-cli.tar.gz",
    );

    const cliCall = spawnSyncMock.mock.calls.find(
      ([command]) => typeof command === "string" && command.includes("rain-orderbook-cli"),
    );
    expect(cliCall).toBeTruthy();
    const cliArgs = cliCall?.[1] as string[];
    expect(cliArgs).toEqual(
      expect.arrayContaining([
        "--db-path",
        join(tempDir, "data", "Optimism.db"),
        "--network",
        "Optimism",
        "--config-commit",
        "feedface",
        "--api-token",
        "token",
      ]),
    );
    expect(sqliteResponses).toHaveLength(0);
  });
});
