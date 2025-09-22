import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { colorText, logBlock, logPlan } from "../logging";
import type { OrderbookConfig } from "../settings";
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
  it("logs key plan details including RPC endpoints", () => {
    const config: OrderbookConfig = {
      network: "optimism",
      chainId: 10,
      orderbookAddress: "0xorderbook",
      deploymentBlock: 1000,
      rpcs: ["https://rpc.optimism.io"],
    };
    const plan: SyncPlan = {
      dbPath: "/tmp/optimism.db",
      dumpPath: "/tmp/optimism.db.tar.gz",
      lastSyncedBlock: 900,
      startBlock: 901,
    };

    logPlan(config, plan);

    const logs = (console.log as unknown as vi.Mock).mock.calls
      .flat()
      .map((line) => String(line));

    expect(logs.some((line) => line.includes("Plan for optimism"))).toBe(true);
    expect(logs.some((line) => line.includes("0xorderbook"))).toBe(true);
    expect(logs.some((line) => line.includes("RPC[1]"))).toBe(true);
  });
});
