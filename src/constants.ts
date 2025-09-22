export const DEFAULT_COMMIT_HASH = "3355912bf0052a7514ffb462e4a6655afb94347f";
export const CLI_ARCHIVE_NAME = "rain-orderbook-cli.tar.gz";
export const CONSTANTS_URL_TEMPLATE =
  "https://raw.githubusercontent.com/rainlanguage/rain.orderbook/{commit}/packages/webapp/src/lib/constants.ts";
export const CLI_ARCHIVE_URL_TEMPLATE =
  "https://raw.githubusercontent.com/rainlanguage/rain.orderbook/{commit}/crates/cli/bin/rain-orderbook-cli.tar.gz";

export const API_TOKEN_ENV_VARS = [
  "HYPERLANE_API_TOKEN",
  "RAIN_API_TOKEN",
  "RAIN_ORDERBOOK_API_TOKEN",
  "HYPERRPC_API_TOKEN",
] as const;

const NUMBER_FORMATTER = new Intl.NumberFormat("en-US");

export function formatNumber(value: number): string {
  return NUMBER_FORMATTER.format(value);
}

export const ANSI = {
  reset: "\u001B[0m",
  bold: "\u001B[1m",
  dim: "\u001B[2m",
  cyan: "\u001B[36m",
  green: "\u001B[32m",
  magenta: "\u001B[35m",
  gray: "\u001B[90m",
} as const;
