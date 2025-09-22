import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "fs";

vi.mock("child_process", () => ({
  spawnSync: vi.fn(),
}));

import { spawnSync } from "child_process";

import { downloadCliArchive, extractCliBinary } from "../archive";
import type { HttpClient } from "../http";

const spawnSyncMock = vi.mocked(spawnSync);

const fakeHttp: HttpClient = {
  fetchBinary: vi.fn(),
  fetchText: vi.fn(),
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("downloadCliArchive", () => {
  it("writes the downloaded archive to disk", async () => {
    const writeSpy = vi.spyOn(fs, "writeFile").mockResolvedValue(undefined);
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    vi.mocked(fakeHttp.fetchBinary).mockResolvedValue(Buffer.from("archive"));

    const destination = await downloadCliArchive(fakeHttp, "deadbeef", "/tmp/cli.tar.gz");

    expect(destination).toBe("/tmp/cli.tar.gz");
    expect(fakeHttp.fetchBinary).toHaveBeenCalledWith(
      "https://raw.githubusercontent.com/rainlanguage/rain.orderbook/deadbeef/crates/cli/bin/rain-orderbook-cli.tar.gz",
    );
    expect(writeSpy).toHaveBeenCalledWith("/tmp/cli.tar.gz", Buffer.from("archive"));

    writeSpy.mockRestore();
    logSpy.mockRestore();
  });
});

describe("extractCliBinary", () => {
  it("extracts the archive and returns the binary path", async () => {
    const mkdirSpy = vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    const chmodSpy = vi.spyOn(fs, "chmod").mockResolvedValue(undefined);
    const readdirSpy = vi.spyOn(fs, "readdir").mockResolvedValue([
      {
        name: "rain-orderbook-cli",
        isDirectory: () => false,
        isFile: () => true,
      },
    ] as unknown as ReturnType<typeof fs.readdir>);
    spawnSyncMock.mockReturnValue({ status: 0 } as unknown as ReturnType<typeof spawnSync>);

    const result = await extractCliBinary("/tmp/cli.tar.gz", "/tmp/bin");

    expect(result).toBe("/tmp/bin/rain-orderbook-cli");
    expect(spawnSyncMock).toHaveBeenCalledWith("tar", ["-xzf", "/tmp/cli.tar.gz", "-C", "/tmp/bin"], {
      stdio: "inherit",
    });
    expect(mkdirSpy).toHaveBeenCalledWith("/tmp/bin", { recursive: true });
    expect(chmodSpy).toHaveBeenCalledWith("/tmp/bin/rain-orderbook-cli", 0o755);

    mkdirSpy.mockRestore();
    chmodSpy.mockRestore();
    readdirSpy.mockRestore();
  });

  it("throws if extraction fails", async () => {
    vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    spawnSyncMock.mockReturnValue({ status: 2 } as unknown as ReturnType<typeof spawnSync>);

    await expect(extractCliBinary("/tmp/cli.tar.gz", "/tmp/bin")).rejects.toThrow(
      "Failed to extract CLI archive",
    );
  });

  it("throws if binary is not found", async () => {
    vi.spyOn(fs, "mkdir").mockResolvedValue(undefined);
    const readdirSpy = vi.spyOn(fs, "readdir").mockResolvedValue([] as unknown as ReturnType<typeof fs.readdir>);
    spawnSyncMock.mockReturnValue({ status: 0 } as unknown as ReturnType<typeof spawnSync>);

    await expect(extractCliBinary("/tmp/cli.tar.gz", "/tmp/bin")).rejects.toThrow(
      "Unable to locate rain-orderbook-cli binary",
    );

    readdirSpy.mockRestore();
  });
});
