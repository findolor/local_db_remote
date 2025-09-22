import { ANSI, formatNumber } from "./constants";
import type { SyncPlan } from "./database";
import type { OrderbookConfig } from "./settings";

export interface LogEntry {
  label: string;
  value: string;
}

export function colorText(text: string, ...codes: string[]): string {
  if (codes.length === 0) {
    return text;
  }
  return `${codes.join("")}${text}${ANSI.reset}`;
}

export function logBlock(title: string, entries: LogEntry[]): void {
  const safeEntries = entries.length > 0 ? entries : [{ label: "Info", value: "(no data)" }];
  const labelWidth = Math.max(...safeEntries.map((entry) => entry.label.length));
  const valueWidth = Math.max(...safeEntries.map((entry) => entry.value.length));
  const contentWidth = Math.max(title.length, labelWidth + 3 + valueWidth);
  const horizontal = "─".repeat(contentWidth + 2);

  console.log(`\n┌${horizontal}┐`);
  const coloredTitle = colorText(title.padEnd(contentWidth), ANSI.bold, ANSI.green);
  console.log(`│ ${coloredTitle} │`);
  console.log(`├${horizontal}┤`);

  for (const entry of safeEntries) {
    const paddedLabel = entry.label.padEnd(labelWidth);
    const coloredLabel = colorText(paddedLabel, ANSI.bold, ANSI.cyan);
    const separator = colorText(" : ", ANSI.bold, ANSI.gray);
    const coloredValue = colorText(entry.value, ANSI.magenta);
    const lineLength = labelWidth + 3 + entry.value.length;
    const padding = " ".repeat(contentWidth - lineLength);
    console.log(`│ ${coloredLabel}${separator}${coloredValue}${padding} │`);
  }

  console.log(`└${horizontal}┘`);
}

export function logPlan(config: OrderbookConfig, plan: SyncPlan): void {
  const entries: LogEntry[] = [
    { label: "Database path", value: plan.dbPath },
    { label: "Dump path", value: plan.dumpPath },
    { label: "Orderbook", value: config.orderbookAddress },
    { label: "Chain ID", value: String(config.chainId) },
    { label: "Deployment block", value: formatNumber(config.deploymentBlock) },
    {
      label: "Last synced block",
      value:
        plan.lastSyncedBlock !== null
          ? formatNumber(plan.lastSyncedBlock)
          : "none",
    },
    { label: "Start block", value: formatNumber(plan.startBlock) },
    { label: "Blocks to fetch", value: "determined by CLI" },
    { label: "RPC endpoints", value: String(config.rpcs.length) },
  ];

  config.rpcs.forEach((rpc, index) => {
    entries.push({ label: `RPC[${index + 1}]`, value: rpc });
  });

  logBlock(`Plan for ${config.network}`, entries);
}
