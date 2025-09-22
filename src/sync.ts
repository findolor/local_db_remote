import { main } from "./index";

if (require.main === module) {
  main().catch(() => {
    process.exit(1);
  });
}

export { main };
