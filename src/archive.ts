import { spawnSync } from "child_process";
import { promises as fs } from "fs";
import { join } from "path";

import { CLI_ARCHIVE_URL_TEMPLATE } from "./constants";
import type { HttpClient } from "./http";

export async function downloadCliArchive(
  http: HttpClient,
  commitHash: string,
  destination: string,
): Promise<string> {
  const url = CLI_ARCHIVE_URL_TEMPLATE.replace("{commit}", commitHash);
  const archiveBytes = await http.fetchBinary(url);
  await fs.writeFile(destination, archiveBytes);
  console.log(
    `Downloaded CLI archive to ${destination} (${archiveBytes.length} bytes)`,
  );
  return destination;
}

export async function extractCliBinary(
  archivePath: string,
  outputDir: string,
): Promise<string> {
  await fs.mkdir(outputDir, { recursive: true });
  const extract = spawnSync("tar", ["-xzf", archivePath, "-C", outputDir], {
    stdio: "inherit",
  });
  if (extract.status !== 0) {
    throw new Error(
      `Failed to extract CLI archive (exit code ${extract.status ?? "unknown"})`,
    );
  }

  const candidate = await findBinary(outputDir);
  if (!candidate) {
    throw new Error(
      `Unable to locate rain-orderbook-cli binary under ${outputDir}`,
    );
  }

  await fs.chmod(candidate, 0o755);
  console.log(`Extracted CLI binary to ${candidate}`);
  return candidate;
}

async function findBinary(root: string): Promise<string | null> {
  const entries = await fs.readdir(root, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(root, entry.name);
    if (entry.isDirectory()) {
      const nested = await findBinary(fullPath);
      if (nested) {
        return nested;
      }
    } else if (entry.isFile() && entry.name === "rain-orderbook-cli") {
      return fullPath;
    }
  }
  return null;
}
