export interface CliOptions {
  networks: string[];
  dbDir: string;
  cliDir: string;
  keepArchive: boolean;
}

export interface CliParseResult {
  options: CliOptions;
  helpRequested: boolean;
}

export function parseArgs(argv: string[]): CliParseResult {
  const options: CliOptions = {
    networks: [],
    dbDir: "data",
    cliDir: "bin",
    keepArchive: false,
  };

  let helpRequested = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case "--networks": {
        const values: string[] = [];
        let j = i + 1;
        while (j < argv.length && !argv[j].startsWith("--")) {
          values.push(argv[j]);
          j += 1;
        }
        if (values.length === 0) {
          throw new Error("--networks requires at least one value");
        }
        options.networks = values;
        i = j - 1;
        break;
      }
      case "--db-dir": {
        const value = argv[i + 1];
        if (!value) {
          throw new Error("--db-dir requires a path argument");
        }
        options.dbDir = value;
        i += 1;
        break;
      }
      case "--cli-dir": {
        const value = argv[i + 1];
        if (!value) {
          throw new Error("--cli-dir requires a path argument");
        }
        options.cliDir = value;
        i += 1;
        break;
      }
      case "--keep-archive": {
        options.keepArchive = true;
        break;
      }
      case "--help":
      case "-h": {
        helpRequested = true;
        break;
      }
      default: {
        throw new Error(`Unknown argument: ${arg}`);
      }
    }
  }

  return { options, helpRequested };
}

export function printUsage(): void {
  console.log("Usage: ts-node src/sync.ts [options]");
  console.log("Options:");
  console.log(
    "  --networks <name...>   Limit sync to the provided network names",
  );
  console.log(
    "  --db-dir <path>        Directory to store SQLite databases (default: data)",
  );
  console.log(
    "  --cli-dir <path>       Directory to extract CLI binary (default: bin)",
  );
  console.log("  --keep-archive         Keep the downloaded CLI archive");
  console.log("  -h, --help             Show this help message");
}
