import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildOrderbookConfigs,
  extractSettingsUrl,
  parseSettingsYaml,
  type ParsedSettings,
} from "../settings";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("extractSettingsUrl", () => {
  it("returns the remote settings URL from constants source", () => {
    const source = "const REMOTE_SETTINGS_URL = 'https://example.com/settings.yaml';";
    expect(extractSettingsUrl(source)).toBe("https://example.com/settings.yaml");
  });

  it("throws when REMOTE_SETTINGS_URL is missing", () => {
    expect(() => extractSettingsUrl("const other = 'value';")).toThrow(
      "Unable to locate REMOTE_SETTINGS_URL in constants source",
    );
  });
});

describe("parseSettingsYaml", () => {
  it("parses networks and orderbooks sections", () => {
    const yaml = `
networks:
  Optimism:
    chain-id: 10
    rpcs:
      - https://rpc.optimism.io
      - https://rpc.optimism.io
      - https://another-rpc.optimism.io
  Mainnet:
    chain-id: 1
    rpcs:
      - https://mainnet.rpc
orderbooks:
  Optimism:
    address: 0x1234
    deployment-block: 9000
  Mainnet:
    address: 0xabcd
    deployment-block: 100
`;

    const parsed = parseSettingsYaml(yaml);
    expect(parsed.networks.Optimism).toEqual({
      chainId: 10,
      rpcs: [
        "https://rpc.optimism.io",
        "https://rpc.optimism.io",
        "https://another-rpc.optimism.io",
      ],
    });
    expect(parsed.orderbooks.Mainnet).toEqual({
      address: "0xabcd",
      deploymentBlock: 100,
    });
  });

  it("ignores malformed entries and whitespace", () => {
    const yaml = `
networks:
  Invalid:
    chain-id: not-a-number
    rpcs:
      -    
      - https://rpc.invalid
orderbooks:
  Invalid:
    address:
    deployment-block: not-a-number
`;

    const parsed = parseSettingsYaml(yaml);
    expect(parsed.networks.Invalid.chainId).toBeUndefined();
    expect(parsed.networks.Invalid.rpcs).toEqual(["https://rpc.invalid"]);
    expect(parsed.orderbooks.Invalid.address).toBeUndefined();
    expect(parsed.orderbooks.Invalid.deploymentBlock).toBeUndefined();
  });
});

describe("buildOrderbookConfigs", () => {
  const baseSettings: ParsedSettings = {
    networks: {
      Optimism: {
        chainId: 10,
        rpcs: ["https://rpc.optimism.io", "https://rpc.optimism.io"],
      },
      Arbitrum: {
        chainId: 42161,
        rpcs: ["https://arb1.arbitrum.io/rpc", "   ", "https://arb1.arbitrum.io/rpc"],
      },
    },
    orderbooks: {
      Optimism: {
        address: "0xoptimism",
        deploymentBlock: 9000,
      },
      Arbitrum: {
        address: "0xarbitrum",
        deploymentBlock: 1200,
      },
    },
  };

  it("builds configs with normalized RPCs", () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const configs = buildOrderbookConfigs(baseSettings, []);

    expect(configs).toEqual([
      {
        network: "Optimism",
        chainId: 10,
        orderbookAddress: "0xoptimism",
        deploymentBlock: 9000,
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

    logSpy.mockRestore();
  });

  it("filters configs by selected networks case-insensitively", () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const configs = buildOrderbookConfigs(baseSettings, ["optimism"]);
    expect(configs).toHaveLength(1);
    expect(configs[0]?.network).toBe("Optimism");
    logSpy.mockRestore();
  });

  it("skips orderbooks with missing network configuration", () => {
    const settings: ParsedSettings = {
      networks: {},
      orderbooks: {
        Missing: {
          address: "0x0",
          deploymentBlock: 1,
        },
      },
    };
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const configs = buildOrderbookConfigs(settings, []);
    expect(configs).toHaveLength(0);
    expect(logSpy).toHaveBeenCalledWith(
      "Skipping Missing: missing network configuration",
    );
    logSpy.mockRestore();
  });

  it("skips networks without chain IDs", () => {
    const settings: ParsedSettings = {
      networks: {
        NoChain: {
          rpcs: ["https://example.com"],
        },
      },
      orderbooks: {
        NoChain: {
          address: "0xdead",
          deploymentBlock: 42,
        },
      },
    };

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const configs = buildOrderbookConfigs(settings, []);

    expect(configs).toHaveLength(0);
    expect(logSpy).toHaveBeenCalledWith("Skipping NoChain: chain-id not defined");

    logSpy.mockRestore();
  });
});
