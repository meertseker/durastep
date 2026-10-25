import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    {
      // Mirrors the tsup `.sql: text` loader so the Postgres adapter can
      // import its schema file as a string under test.
      name: "sql-as-text",
      transform(code, id) {
        if (id.endsWith(".sql")) {
          return { code: `export default ${JSON.stringify(code)};`, map: null };
        }
        return null;
      },
    },
  ],
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    testTimeout: 10_000,
  },
});
