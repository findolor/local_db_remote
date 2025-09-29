import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { colorText, logBlock, logPlan } from "../logging";
import type { SyncPlan } from "../database";

beforeEach(() => {
  vi.spyOn(console, "log").mockImplementation(() => undefined);
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("colorText", () => {
  it("wraps text with ANSI codes", () => {
    expect(colorText("hello", "\u001B[1m"))
      .toBe("\u001B[1mhello\u001B[0m");
  });
});

describe("logBlock", () => {
  it("prints a formatted table", () => {
    logBlock("Example", [
      { label: "Foo", value: "123" },
      { label: "Bar", value: "456" },
    ]);

    const logs = (console.log as unknown as vi.Mock).mock.calls.map(([line]) => String(line));
    expect(logs[0]).toContain("┌");
    expect(logs[1]).toContain("Example");
    expect(logs.some((line) => line.includes("Foo"))).toBe(true);
    expect(logs.some((line) => line.includes("Bar"))).toBe(true);
  });
});

describe("logPlan", () => {
  it("logs the basic plan details", () => {
    const plan: SyncPlan = {
      dbPath: "/tmp/optimism.db",
      dumpPath: "/tmp/optimism.db.tar.gz",
      lastSyncedBlock: 900,
      nextStartBlock: 901,
    };

    logPlan("optimism", plan);

    const logs = (console.log as unknown as vi.Mock).mock.calls
      .flat()
      .map((line) => String(line));

    expect(logs.some((line) => line.includes("Plan for optimism"))).toBe(true);
    expect(logs.some((line) => line.includes("Next start block"))).toBe(true);
  });
});
