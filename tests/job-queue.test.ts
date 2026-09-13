import { describe, expect, it } from "vitest";
import { JobQueue } from "../src/queue/JobQueue.js";

function createQueue(opts?: {
  now?: () => number;
  random?: () => number;
}): { queue: JobQueue; setNow: (t: number) => void } {
  let clock = 1_000_000;
  const setNow = (t: number) => {
    clock = t;
  };
  const queue = new JobQueue({
    now: () => clock,
    random: opts?.random ?? (() => 0), // zero jitter for predictable backoff
    ...opts,
  });
  return { queue, setNow };
}

describe("JobQueue", () => {
  describe("enqueue / claim / complete", () => {
    it("enqueues, claims, and completes a job", () => {
      const { queue } = createQueue();
      const job = queue.enqueue("send-email", { to: "a@b.co" });
      expect(job.status).toBe("pending");
      expect(job.attempts).toBe(0);

      const claimed = queue.claim("worker-1", 5_000);
      expect(claimed).toBeDefined();
      expect(claimed!.id).toBe(job.id);
      expect(claimed!.status).toBe("leased");
      expect(claimed!.leasedBy).toBe("worker-1");
      expect(claimed!.leaseExpiresAt).toBe(1_000_000 + 5_000);

      const done = queue.complete(claimed!.id);
      expect(done.status).toBe("succeeded");
      expect(done.leasedBy).toBeUndefined();
      expect(queue.claim("worker-1", 5_000)).toBeUndefined();
    });

    it("returns undefined when the queue is empty", () => {
      const { queue } = createQueue();
      expect(queue.claim("w", 1000)).toBeUndefined();
    });

    it("does not claim a job that is still delayed", () => {
      const { queue, setNow } = createQueue();
      queue.enqueue("later", {}, { delayMs: 10_000 });
      expect(queue.claim("w", 1000)).toBeUndefined();
      setNow(1_000_000 + 10_000);
      expect(queue.claim("w", 1000)?.name).toBe("later");
    });
  });

  describe("retry then dead-letter", () => {
    it("retries with backoff then moves to dead after maxAttempts", () => {
      const { queue, setNow } = createQueue();
      const job = queue.enqueue(
        "flaky",
        { n: 1 },
        { maxAttempts: 3, backoffBaseMs: 100, backoffMaxMs: 10_000 },
      );

      // attempt 1 fails → failed, available after backoff (attempt index 0 → delay 0 with random=0)
      let claimed = queue.claim("w", 1000)!;
      let result = queue.fail(claimed.id, new Error("boom-1"));
      expect(result.status).toBe("failed");
      expect(result.attempts).toBe(1);
      expect(result.lastError).toBe("boom-1");
      expect(result.availableAt).toBe(1_000_000); // delay 0 with random=0

      // attempt 2
      claimed = queue.claim("w", 1000)!;
      result = queue.fail(claimed.id, new Error("boom-2"));
      expect(result.status).toBe("failed");
      expect(result.attempts).toBe(2);

      // attempt 3 → dead
      claimed = queue.claim("w", 1000)!;
      result = queue.fail(claimed.id, new Error("boom-3"));
      expect(result.status).toBe("dead");
      expect(result.attempts).toBe(3);

      const dead = queue.listDead();
      expect(dead).toHaveLength(1);
      expect(dead[0]!.id).toBe(job.id);
      expect(queue.claim("w", 1000)).toBeUndefined();

      // advance clock — still dead, not reclaimable
      setNow(1_000_000 + 60_000);
      expect(queue.claim("w", 1000)).toBeUndefined();
    });

    it("respects backoff before reclaim after fail", () => {
      const { queue, setNow } = createQueue({
        random: () => 1 - Number.EPSILON, // max jitter
      });
      queue.enqueue("slow-retry", {}, { maxAttempts: 5, backoffBaseMs: 200 });

      const claimed = queue.claim("w", 1000)!;
      const failed = queue.fail(claimed.id, "temp");
      expect(failed.status).toBe("failed");
      // attempt index 0, base 200, random ~1 → delay ≈ 200
      expect(failed.availableAt).toBeGreaterThan(1_000_000);

      expect(queue.claim("w", 1000)).toBeUndefined();
      setNow(failed.availableAt);
      expect(queue.claim("w", 1000)?.id).toBe(claimed.id);
    });
  });

  describe("idempotency", () => {
    it("returns the existing job when idempotencyKey matches", () => {
      const { queue } = createQueue();
      const a = queue.enqueue(
        "charge",
        { amount: 10 },
        { idempotencyKey: "txn-42" },
      );
      const b = queue.enqueue(
        "charge",
        { amount: 999 },
        { idempotencyKey: "txn-42" },
      );
      expect(b.id).toBe(a.id);
      expect(b.payload).toEqual({ amount: 10 });
      expect(queue.list()).toHaveLength(1);
    });

    it("creates distinct jobs when keys differ or are absent", () => {
      const { queue } = createQueue();
      const a = queue.enqueue("x", {}, { idempotencyKey: "a" });
      const b = queue.enqueue("x", {}, { idempotencyKey: "b" });
      const c = queue.enqueue("x", {});
      expect(new Set([a.id, b.id, c.id]).size).toBe(3);
    });
  });

  describe("lease expiry reclaim", () => {
    it("allows another worker to reclaim after lease expiry", () => {
      const { queue, setNow } = createQueue();
      const job = queue.enqueue("work", { i: 1 });

      const first = queue.claim("worker-a", 5_000)!;
      expect(first.leasedBy).toBe("worker-a");

      // still leased — not claimable
      expect(queue.claim("worker-b", 5_000)).toBeUndefined();

      // lease expires
      setNow(1_000_000 + 5_000);
      const second = queue.claim("worker-b", 5_000)!;
      expect(second.id).toBe(job.id);
      expect(second.leasedBy).toBe("worker-b");
      expect(second.status).toBe("leased");
    });

    it("heartbeat extends the lease", () => {
      const { queue, setNow } = createQueue();
      queue.enqueue("long", {});
      const claimed = queue.claim("w", 1_000)!;
      expect(claimed.leaseExpiresAt).toBe(1_000_000 + 1_000);

      setNow(1_000_000 + 500);
      const hb = queue.heartbeat(claimed.id, 2_000);
      expect(hb.leaseExpiresAt).toBe(1_000_000 + 500 + 2_000);

      // original lease would have expired at +1000, but heartbeat extended it
      setNow(1_000_000 + 1_500);
      expect(queue.claim("other", 1000)).toBeUndefined();
    });

    it("rejects complete/fail on non-leased jobs", () => {
      const { queue } = createQueue();
      const job = queue.enqueue("x", {});
      expect(() => queue.complete(job.id)).toThrow(/not leased/);
      expect(() => queue.fail(job.id, "nope")).toThrow(/not leased/);
    });
  });
});
