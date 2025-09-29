import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "fs";

const parseArgsMock = vi.fn();
const printUsageMock = vi.fn();
const downloadCliArchiveMock = vi.fn();
const extractCliBinaryMock = vi.fn();
const prepareDatabaseMock = vi.fn();
const planSyncMock = vi.fn();
const finalizeDatabaseMock = vi.fn();
const runCliSyncMock = vi.fn();
const logPlanMock = vi.fn();

vi.mock("../options", () => ({
  parseArgs: parseArgsMock,
  printUsage: printUsageMock,
}));

vi.mock("../archive", () => ({
  downloadCliArchive: downloadCliArchiveMock,
  extractCliBinary: extractCliBinaryMock,
}));

vi.mock("../database", () => ({
  prepareDatabase: prepareDatabaseMock,
  planSync: planSyncMock,
  finalizeDatabase: finalizeDatabaseMock,
}));

vi.mock("../cli", () => ({
  runCliSync: runCliSyncMock,
}));

vi.mock("../logging", () => ({
  logPlan: logPlanMock,
}));

const { runSync } = await import("../index");

const fakeHttp = {
  fetchText: vi.fn<(url: string) => Promise<string>>(),
  fetchBinary: vi.fn(),
};

beforeEach(() => {
  [
    parseArgsMock,
    printUsageMock,
    downloadCliArchiveMock,
    extractCliBinaryMock,
    prepareDatabaseMock,
    planSyncMock,
    finalizeDatabaseMock,
    runCliSyncMock,
    logPlanMock,
  ].forEach((mockFn) => mockFn.mockReset());
  fakeHttp.fetchText.mockReset();
  fakeHttp.fetchBinary.mockReset();
  vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
  vi.spyOn(fs, "unlink").mockResolvedValue(undefined);
  vi.spyOn(fs, "readdir").mockResolvedValue([]);
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("runSync", () => {
  it("prints help and exits early when requested", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: true,
    });

    const exitMock = vi.fn();

    await runSync({
      argv: ["--help"],
      env: {},
      cwd: () => "/workspace",
      exit: exitMock,
      http: fakeHttp,
    });

    expect(printUsageMock).toHaveBeenCalled();
    expect(exitMock).toHaveBeenCalledWith(0);
    expect(downloadCliArchiveMock).not.toHaveBeenCalled();
  });

  it("runs the full sync pipeline for the provided networks", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: ["optimism"],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    prepareDatabaseMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
    });
    planSyncMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
      lastSyncedBlock: null,
      nextStartBlock: null,
    });
    runCliSyncMock.mockResolvedValue(undefined);
    finalizeDatabaseMock.mockResolvedValue(undefined);

    downloadCliArchiveMock.mockResolvedValue(undefined);
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    const unlinkSpy = vi.spyOn(fs, "unlink");

    await runSync({
      argv: [],
      env: { COMMIT_HASH: "deadbeef", RAIN_API_TOKEN: "token" },
      cwd: () => "/workspace",
      exit: vi.fn(),
      http: fakeHttp,
    });

    expect(downloadCliArchiveMock).toHaveBeenCalledWith(
      fakeHttp,
      "deadbeef",
      "/workspace/rain-orderbook-cli.tar.gz",
    );
    expect(extractCliBinaryMock).toHaveBeenCalledWith(
      "/workspace/rain-orderbook-cli.tar.gz",
      "/workspace/bin",
    );
    expect(prepareDatabaseMock).toHaveBeenCalledWith("optimism", "/workspace/data");
    expect(planSyncMock).toHaveBeenCalledWith(
      "/workspace/data/optimism.db",
      "/workspace/data/optimism.db.tar.gz",
    );
    expect(logPlanMock).toHaveBeenCalledWith(
      "optimism",
      expect.objectContaining({ dbPath: "/workspace/data/optimism.db" }),
    );
    expect(runCliSyncMock).toHaveBeenCalledWith(
      expect.objectContaining({
        cliBinary: "/workspace/bin/rain-orderbook-cli",
        network: "optimism",
        dbPath: "/workspace/data/optimism.db",
        apiToken: "token",
        configCommit: "deadbeef",
      }),
    );
    expect(finalizeDatabaseMock).toHaveBeenCalledWith(
      "optimism",
      "/workspace/data/optimism.db",
      "/workspace/data/optimism.db.tar.gz",
    );
    expect(unlinkSpy).toHaveBeenCalledWith("/workspace/data/optimism.db");
  });

  it("throws when no API token is present", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: ["optimism"],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    downloadCliArchiveMock.mockResolvedValue(undefined);
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    await expect(
      runSync({
        argv: [],
        env: {},
        cwd: () => "/workspace",
        exit: vi.fn(),
        http: fakeHttp,
      }),
    ).rejects.toThrow("Missing API token");
  });

  it("logs and exits when no networks are resolved", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

    await runSync({
      argv: [],
      env: { RAIN_API_TOKEN: "token" },
      cwd: () => "/workspace",
      exit: vi.fn(),
      http: fakeHttp,
    });

    expect(logSpy).toHaveBeenCalledWith(
      "No networks selected. Provide --networks or ensure dumps exist in the data directory.",
    );
    expect(runCliSyncMock).not.toHaveBeenCalled();

    logSpy.mockRestore();
  });

  it("derives networks from existing dumps when none are provided", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    vi.spyOn(fs, "readdir").mockResolvedValue([
      "optimism.db.tar.gz",
      "ignore.txt",
    ]);

    prepareDatabaseMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
    });
    planSyncMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
      lastSyncedBlock: null,
      nextStartBlock: null,
    });
    runCliSyncMock.mockResolvedValue(undefined);
    finalizeDatabaseMock.mockResolvedValue(undefined);

    downloadCliArchiveMock.mockResolvedValue(undefined);
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    await runSync({
      argv: [],
      env: { RAIN_API_TOKEN: "token" },
      cwd: () => "/workspace",
      exit: vi.fn(),
      http: fakeHttp,
    });

    expect(prepareDatabaseMock).toHaveBeenCalledWith("optimism", "/workspace/data");
  });

  it("cleans up database files when CLI execution fails", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: ["optimism"],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    prepareDatabaseMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
    });
    planSyncMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
      lastSyncedBlock: null,
      nextStartBlock: null,
    });
    runCliSyncMock.mockRejectedValue(new Error("boom"));
    finalizeDatabaseMock.mockResolvedValue(undefined);

    downloadCliArchiveMock.mockResolvedValue(undefined);
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    const unlinkSpy = vi.spyOn(fs, "unlink");

    await expect(
      runSync({
        argv: [],
        env: { RAIN_API_TOKEN: "token" },
        cwd: () => "/workspace",
        exit: vi.fn(),
        http: fakeHttp,
      }),
    ).rejects.toThrow("boom");

    const unlinkTargets = (unlinkSpy.mock.calls as Array<[string]>).map(([path]) => path);
    expect(unlinkTargets).toContain("/workspace/data/optimism.db");
    expect(finalizeDatabaseMock).not.toHaveBeenCalled();
  });
});
