import { describe, expect, it } from "vitest";

import { parseArgs } from "../options";

describe("parseArgs", () => {
  it("returns defaults when no arguments provided", () => {
    const result = parseArgs([]);
    expect(result.helpRequested).toBe(false);
    expect(result.options).toEqual({
      networks: [],
      dbDir: "data",
      cliDir: "bin",
      keepArchive: false,
    });
  });

  it("parses network list and directory overrides", () => {
    const result = parseArgs([
      "--networks",
      "Mainnet",
      "Optimism",
      "--db-dir",
      "custom-data",
      "--cli-dir",
      "custom-bin",
      "--keep-archive",
    ]);

    expect(result.helpRequested).toBe(false);
    expect(result.options).toEqual({
      networks: ["Mainnet", "Optimism"],
      dbDir: "custom-data",
      cliDir: "custom-bin",
      keepArchive: true,
    });
  });

  it("flags help requests", () => {
    const result = parseArgs(["--help"]);
    expect(result.helpRequested).toBe(true);
    expect(result.options).toEqual({
      networks: [],
      dbDir: "data",
      cliDir: "bin",
      keepArchive: false,
    });
  });

  it("throws when required option values are missing", () => {
    expect(() => parseArgs(["--db-dir"])).toThrow("--db-dir requires a path argument");
  });

  it("throws on unknown arguments", () => {
    expect(() => parseArgs(["--unknown"])).toThrow("Unknown argument: --unknown");
  });
});
