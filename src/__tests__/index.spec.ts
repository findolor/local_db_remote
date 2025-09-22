import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "fs";

const parseArgsMock = vi.fn();
const printUsageMock = vi.fn();
const downloadCliArchiveMock = vi.fn();
const extractCliBinaryMock = vi.fn();
const buildOrderbookConfigsMock = vi.fn();
const extractSettingsUrlMock = vi.fn();
const parseSettingsYamlMock = vi.fn();
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

vi.mock("../settings", () => ({
  buildOrderbookConfigs: buildOrderbookConfigsMock,
  extractSettingsUrl: extractSettingsUrlMock,
  parseSettingsYaml: parseSettingsYamlMock,
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
    buildOrderbookConfigsMock,
    extractSettingsUrlMock,
    parseSettingsYamlMock,
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

  it("runs the full sync pipeline for each configuration", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    extractSettingsUrlMock.mockReturnValue("https://example.com/settings.yaml");
    parseSettingsYamlMock.mockReturnValue("parsed" as unknown as Record<string, unknown>);
    buildOrderbookConfigsMock.mockReturnValue([
      {
        network: "optimism",
        chainId: 10,
        orderbookAddress: "0xorderbook",
        deploymentBlock: 1000,
        rpcs: ["https://rpc.optimism.io"],
      },
    ]);

    prepareDatabaseMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
    });
    planSyncMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
      lastSyncedBlock: null,
      startBlock: 1000,
    });
    runCliSyncMock.mockResolvedValue(undefined);
    finalizeDatabaseMock.mockResolvedValue(undefined);

    fakeHttp.fetchText
      .mockResolvedValueOnce("const REMOTE_SETTINGS_URL = 'https://example.com/settings.yaml';")
      .mockResolvedValueOnce("yaml");
    downloadCliArchiveMock.mockResolvedValue("/workspace/rain-orderbook-cli.tar.gz");
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    const unlinkSpy = vi.spyOn(fs, "unlink");

    await runSync({
      argv: [],
      env: { COMMIT_HASH: "deadbeef", RAIN_API_TOKEN: "token" },
      cwd: () => "/workspace",
      exit: vi.fn(),
      http: fakeHttp,
    });

    expect(fakeHttp.fetchText).toHaveBeenNthCalledWith(
      1,
      "https://raw.githubusercontent.com/rainlanguage/rain.orderbook/deadbeef/packages/webapp/src/lib/constants.ts",
    );
    expect(downloadCliArchiveMock).toHaveBeenCalledWith(
      fakeHttp,
      "deadbeef",
      "/workspace/rain-orderbook-cli.tar.gz",
    );
    expect(extractCliBinaryMock).toHaveBeenCalledWith(
      "/workspace/rain-orderbook-cli.tar.gz",
      "/workspace/bin",
    );
    expect(buildOrderbookConfigsMock).toHaveBeenCalledWith("parsed", []);
    expect(prepareDatabaseMock).toHaveBeenCalledWith("optimism", "/workspace/data");
    expect(planSyncMock).toHaveBeenCalled();
    expect(logPlanMock).toHaveBeenCalled();
    expect(runCliSyncMock).toHaveBeenCalledWith(
      "/workspace/bin/rain-orderbook-cli",
      expect.objectContaining({ network: "optimism" }),
      "/workspace/data/optimism.db",
      "token",
    );
    expect(finalizeDatabaseMock).toHaveBeenCalled();
    expect(unlinkSpy).toHaveBeenCalledWith("/workspace/data/optimism.db");
  });

  it("throws when no API token is present", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    extractSettingsUrlMock.mockReturnValue("https://example.com/settings.yaml");
    parseSettingsYamlMock.mockReturnValue("parsed" as unknown as Record<string, unknown>);
    buildOrderbookConfigsMock.mockReturnValue([
      {
        network: "optimism",
        chainId: 10,
        orderbookAddress: "0xorderbook",
        deploymentBlock: 1000,
        rpcs: ["https://rpc.optimism.io"],
      },
    ]);

    fakeHttp.fetchText
      .mockResolvedValueOnce("const REMOTE_SETTINGS_URL = 'https://example.com/settings.yaml';")
      .mockResolvedValueOnce("yaml");
    downloadCliArchiveMock.mockResolvedValue("/workspace/rain-orderbook-cli.tar.gz");
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

  it("logs and exits when no configs match", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: ["non-existent"],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    extractSettingsUrlMock.mockReturnValue("https://example.com/settings.yaml");
    parseSettingsYamlMock.mockReturnValue("parsed" as unknown as Record<string, unknown>);
    buildOrderbookConfigsMock.mockReturnValue([]);
    fakeHttp.fetchText
      .mockResolvedValueOnce("const REMOTE_SETTINGS_URL = 'https://example.com/settings.yaml';")
      .mockResolvedValueOnce("yaml");
    downloadCliArchiveMock.mockResolvedValue("/workspace/rain-orderbook-cli.tar.gz");
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

    await runSync({
      argv: [],
      env: { RAIN_API_TOKEN: "token" },
      cwd: () => "/workspace",
      exit: vi.fn(),
      http: fakeHttp,
    });

    expect(logSpy).toHaveBeenCalledWith("No orderbook configurations matched the selection.");
    expect(runCliSyncMock).not.toHaveBeenCalled();

    logSpy.mockRestore();
  });

  it("processes multiple configs, trims API tokens, and keeps archives when requested", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: true,
      },
      helpRequested: false,
    });

    extractSettingsUrlMock.mockReturnValue("https://example.com/settings.yaml");
    parseSettingsYamlMock.mockReturnValue("parsed" as unknown as Record<string, unknown>);
    buildOrderbookConfigsMock.mockReturnValue([
      {
        network: "Optimism",
        chainId: 10,
        orderbookAddress: "0xoptimism",
        deploymentBlock: 1000,
        rpcs: ["https://rpc.optimism.io"],
      },
      {
        network: "Arbitrum",
        chainId: 42161,
        orderbookAddress: "0xarbitrum",
        deploymentBlock: 1200,
        rpcs: ["https://arb1.arbitrum.io/rpc"],
      },
    ]);

    prepareDatabaseMock
      .mockResolvedValueOnce({
        dbPath: "/workspace/data/Optimism.db",
        dumpPath: "/workspace/data/Optimism.db.tar.gz",
      })
      .mockResolvedValueOnce({
        dbPath: "/workspace/data/Arbitrum.db",
        dumpPath: "/workspace/data/Arbitrum.db.tar.gz",
      });

    planSyncMock
      .mockResolvedValueOnce({
        dbPath: "/workspace/data/Optimism.db",
        dumpPath: "/workspace/data/Optimism.db.tar.gz",
        lastSyncedBlock: 900,
        startBlock: 901,
      })
      .mockResolvedValueOnce({
        dbPath: "/workspace/data/Arbitrum.db",
        dumpPath: "/workspace/data/Arbitrum.db.tar.gz",
        lastSyncedBlock: null,
        startBlock: 1200,
      });

    runCliSyncMock.mockResolvedValue(undefined);
    finalizeDatabaseMock.mockResolvedValue(undefined);

    fakeHttp.fetchText
      .mockResolvedValueOnce("const REMOTE_SETTINGS_URL = 'https://example.com/settings.yaml';")
      .mockResolvedValueOnce("yaml");
    downloadCliArchiveMock.mockResolvedValue("/workspace/rain-orderbook-cli.tar.gz");
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    const unlinkSpy = vi.spyOn(fs, "unlink");

    await runSync({
      argv: [],
      env: {
        COMMIT_HASH: "cafebabe",
        RAIN_API_TOKEN: " token-value ",
      },
      cwd: () => "/workspace",
      exit: vi.fn(),
      http: fakeHttp,
    });

    expect(prepareDatabaseMock).toHaveBeenNthCalledWith(1, "Optimism", "/workspace/data");
    expect(prepareDatabaseMock).toHaveBeenNthCalledWith(2, "Arbitrum", "/workspace/data");
    expect(finalizeDatabaseMock).toHaveBeenCalledTimes(2);
    expect(runCliSyncMock).toHaveBeenNthCalledWith(
      1,
      "/workspace/bin/rain-orderbook-cli",
      expect.objectContaining({ network: "Optimism" }),
      "/workspace/data/Optimism.db",
      "token-value",
    );
    expect(runCliSyncMock).toHaveBeenNthCalledWith(
      2,
      "/workspace/bin/rain-orderbook-cli",
      expect.objectContaining({ network: "Arbitrum" }),
      "/workspace/data/Arbitrum.db",
      "token-value",
    );
    expect(downloadCliArchiveMock).toHaveBeenCalledTimes(1);
    expect(extractCliBinaryMock).toHaveBeenCalledTimes(1);

    const archivePath = "/workspace/rain-orderbook-cli.tar.gz";
    const unlinkCalls = (unlinkSpy.mock.calls as Array<[string]>).map(([path]) => path);
    expect(unlinkCalls).not.toContain(archivePath);
  });

  it("rejects when COMMIT_HASH is blank after trimming", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    await expect(
      runSync({
        argv: [],
        env: { COMMIT_HASH: "   " },
        cwd: () => "/workspace",
        exit: vi.fn(),
        http: fakeHttp,
      }),
    ).rejects.toThrow("COMMIT_HASH must be set to a valid rain.orderbook commit hash");

    expect(fakeHttp.fetchText).not.toHaveBeenCalled();
    expect(downloadCliArchiveMock).not.toHaveBeenCalled();
  });

  it("cleans up database files when CLI execution fails", async () => {
    parseArgsMock.mockReturnValue({
      options: {
        networks: [],
        dbDir: "data",
        cliDir: "bin",
        keepArchive: false,
      },
      helpRequested: false,
    });

    extractSettingsUrlMock.mockReturnValue("https://example.com/settings.yaml");
    parseSettingsYamlMock.mockReturnValue("parsed" as unknown as Record<string, unknown>);
    buildOrderbookConfigsMock.mockReturnValue([
      {
        network: "optimism",
        chainId: 10,
        orderbookAddress: "0xorderbook",
        deploymentBlock: 1000,
        rpcs: ["https://rpc.optimism.io"],
      },
    ]);

    prepareDatabaseMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
    });
    planSyncMock.mockResolvedValue({
      dbPath: "/workspace/data/optimism.db",
      dumpPath: "/workspace/data/optimism.db.tar.gz",
      lastSyncedBlock: null,
      startBlock: 1000,
    });

    runCliSyncMock.mockRejectedValue(new Error("boom"));
    finalizeDatabaseMock.mockResolvedValue(undefined);

    fakeHttp.fetchText
      .mockResolvedValueOnce("const REMOTE_SETTINGS_URL = 'https://example.com/settings.yaml';")
      .mockResolvedValueOnce("yaml");
    downloadCliArchiveMock.mockResolvedValue("/workspace/rain-orderbook-cli.tar.gz");
    extractCliBinaryMock.mockResolvedValue("/workspace/bin/rain-orderbook-cli");

    const unlinkSpy = vi.spyOn(fs, "unlink");

    await expect(
      runSync({
        argv: [],
        env: { COMMIT_HASH: "deadbeef", RAIN_API_TOKEN: "token" },
        cwd: () => "/workspace",
        exit: vi.fn(),
        http: fakeHttp,
      }),
    ).rejects.toThrow("boom");

    expect(runCliSyncMock).toHaveBeenCalledTimes(1);
    expect(finalizeDatabaseMock).not.toHaveBeenCalled();

    const unlinkTargets = (unlinkSpy.mock.calls as Array<[string]>).map(([path]) => path);
    expect(unlinkTargets).toContain("/workspace/data/optimism.db");
  });
});
