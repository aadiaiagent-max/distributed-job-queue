import { describe, expect, it } from "vitest";
import { computeDelay } from "../src/backoff.js";

describe("computeDelay", () => {
  it("returns a value in [0, baseMs] for attempt 0 with no growth", () => {
    const delay = computeDelay(0, 100, 10_000, () => 0.5);
    expect(delay).toBe(Math.floor(0.5 * (100 + 1)));
  });

  it("grows exponentially and respects maxMs", () => {
    // attempt 10 with base 100 would be huge; capped at maxMs=500
    const delay = computeDelay(10, 100, 500, () => 1 - Number.EPSILON);
    expect(delay).toBeLessThanOrEqual(500);
    expect(delay).toBeGreaterThanOrEqual(0);
  });

  it("is deterministic given a fixed RNG", () => {
    const a = computeDelay(2, 50, 1000, () => 0.25);
    const b = computeDelay(2, 50, 1000, () => 0.25);
    expect(a).toBe(b);
  });

  it("rejects negative attempt", () => {
    expect(() => computeDelay(-1, 10, 100)).toThrow(RangeError);
  });
});
