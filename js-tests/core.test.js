import { describe, it, expect, beforeEach } from "vitest";
import { DataFusionContext } from "../pkg/datafusion_wasm.js";

describe("DataFusionContext integration tests", () => {
  let ctx;

  beforeEach(() => {
    ctx = DataFusionContext.new();
  });

  it("greets", () => {
    expect(DataFusionContext.greet()).toContain("datafusion-wasm");
  });

  it("SELECT literal arithmetic", async () => {
    const out = await ctx.execute_sql("SELECT 1 + 1 AS result");
    expect(out).toContain("result");
    expect(out).toContain("2");
  });

  it("SELECT with a VALUES-backed subquery", async () => {
    const out = await ctx.execute_sql(
      "SELECT id, name FROM (VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')) AS t(id, name) ORDER BY id"
    );
    expect(out).toContain("alice");
    expect(out).toContain("bob");
    expect(out).toContain("carol");
  });

  it("filters rows with WHERE", async () => {
    const out = await ctx.execute_sql(
      "SELECT name FROM (VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')) AS t(id, name) WHERE id = 2"
    );
    expect(out).toContain("bob");
    expect(out).not.toContain("alice");
    expect(out).not.toContain("carol");
  });

  it("aggregates with COUNT(*)", async () => {
    const out = await ctx.execute_sql(
      "SELECT COUNT(*) AS n FROM (VALUES (1), (2), (3)) AS t(id)"
    );
    expect(out).toContain("n");
    expect(out).toContain("3");
  });
});
