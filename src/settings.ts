export interface NetworkSettings {
  chainId?: number;
  rpcs: string[];
}

export interface OrderbookSettings {
  address?: string;
  deploymentBlock?: number;
}

export interface ParsedSettings {
  networks: Record<string, NetworkSettings>;
  orderbooks: Record<string, OrderbookSettings>;
}

export interface OrderbookConfig {
  network: string;
  chainId: number;
  orderbookAddress: string;
  deploymentBlock: number;
  rpcs: string[];
}

export function extractSettingsUrl(constantsSource: string): string {
  const match = constantsSource.match(
    /REMOTE_SETTINGS_URL\s*=\s*['"]([^'"]+)['"]/,
  );
  if (!match) {
    throw new Error("Unable to locate REMOTE_SETTINGS_URL in constants source");
  }
  return match[1];
}

export function parseSettingsYaml(yamlText: string): ParsedSettings {
  const networks: Record<string, NetworkSettings> = {};
  const orderbooks: Record<string, OrderbookSettings> = {};

  let section: "networks" | "orderbooks" | null = null;
  let current: string | null = null;
  let listKey: string | null = null;

  const lines = yamlText.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.replace(/\s+$/, "");
    const trimmed = line.trim();

    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    if (trimmed === "networks:") {
      section = "networks";
      current = null;
      listKey = null;
      continue;
    }

    if (trimmed === "orderbooks:") {
      section = "orderbooks";
      current = null;
      listKey = null;
      continue;
    }

    const indent = line.search(/\S/);
    if (indent === 0) {
      section = null;
      current = null;
      listKey = null;
      continue;
    }

    if (
      (section === "networks" || section === "orderbooks") &&
      indent === 2 &&
      trimmed.endsWith(":")
    ) {
      current = trimmed.slice(0, -1).trim();
      listKey = null;
      if (section === "networks" && !networks[current]) {
        networks[current] = { rpcs: [] };
      }
      if (section === "orderbooks" && !orderbooks[current]) {
        orderbooks[current] = {};
      }
      continue;
    }

    if (!section || !current) {
      continue;
    }

    if (section === "networks") {
      if (indent === 4 && trimmed.endsWith(":")) {
        const key = trimmed.slice(0, -1).trim();
        if (key === "rpcs") {
          listKey = key;
          networks[current].rpcs = [];
        } else {
          listKey = null;
        }
        continue;
      }

      if (listKey === "rpcs" && indent >= 6 && trimmed.startsWith("- ")) {
        networks[current].rpcs.push(trimmed.slice(2).trim());
        continue;
      }

      const kvMatch = trimmed.match(/^([^:]+):\s*(.+)$/);
      if (!kvMatch) {
        continue;
      }

      const key = kvMatch[1].trim();
      const value = kvMatch[2].trim();

      if (key === "chain-id") {
        const parsed = Number(value);
        if (!Number.isNaN(parsed)) {
          networks[current].chainId = parsed;
        }
      }
    } else if (section === "orderbooks") {
      const kvMatch = trimmed.match(/^([^:]+):\s*(.+)$/);
      if (!kvMatch) {
        continue;
      }

      const key = kvMatch[1].trim();
      const value = kvMatch[2].trim();

      if (key === "address") {
        orderbooks[current].address = value;
      } else if (key === "deployment-block") {
        const parsed = Number(value);
        if (!Number.isNaN(parsed)) {
          orderbooks[current].deploymentBlock = parsed;
        }
      }
    }
  }

  return { networks, orderbooks };
}

export function buildOrderbookConfigs(
  settings: ParsedSettings,
  selectedNetworks: string[],
): OrderbookConfig[] {
  const selected = new Set(selectedNetworks.map((name) => name.toLowerCase()));
  const configs: OrderbookConfig[] = [];

  for (const [network, orderbook] of Object.entries(settings.orderbooks)) {
    if (selected.size > 0 && !selected.has(network.toLowerCase())) {
      continue;
    }

    const networkInfo = settings.networks[network];
    if (!networkInfo) {
      console.log(`Skipping ${network}: missing network configuration`);
      continue;
    }
    if (networkInfo.chainId === undefined) {
      console.log(`Skipping ${network}: chain-id not defined`);
      continue;
    }
    if (!orderbook.address || orderbook.deploymentBlock === undefined) {
      console.log(`Skipping ${network}: orderbook data incomplete`);
      continue;
    }

    const normalizedRpcs = Array.from(
      new Set(
        networkInfo.rpcs
          .map((rpc) => rpc.trim())
          .filter((rpc) => rpc.length > 0),
      ),
    );

    if (normalizedRpcs.length === 0) {
      console.log(`Skipping ${network}: no RPC endpoints configured`);
      continue;
    }

    configs.push({
      network,
      chainId: networkInfo.chainId,
      orderbookAddress: orderbook.address,
      deploymentBlock: orderbook.deploymentBlock,
      rpcs: normalizedRpcs,
    });
  }

  return configs;
}
