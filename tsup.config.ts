import { copyFile, mkdir } from "node:fs/promises";
import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "postgres/index": "src/store/postgres/index.ts",
    "bullmq/index": "src/queue/bullmq/index.ts",
  },
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  target: "node20",
  platform: "node",
  // Shared chunks keep one copy of the core (errors, orchestrator) across
  // the three entry points, so `instanceof` and module state agree.
  splitting: true,
  treeshake: true,
  external: ["pg", "bullmq", "ioredis"],
  async onSuccess() {
    await mkdir("dist/postgres", { recursive: true });
    await copyFile("src/store/postgres/schema.sql", "dist/postgres/schema.sql");
  },
});
