/**
 * Basic demo: enqueue → claim → fail (retry) → claim → complete,
 * plus a dead-letter path and idempotent enqueue.
 *
 * Run: npm run example
 */
import { JobQueue } from "../src/index.js";

// Deterministic clock + zero jitter so the demo is readable without sleeps.
let now = Date.now();
const queue = new JobQueue({
  now: () => now,
  random: () => 0,
});

console.log("=== Enqueue ===");
const job = queue.enqueue(
  "send-welcome-email",
  { userId: "u_123", email: "ada@example.com" },
  { maxAttempts: 3, backoffBaseMs: 50, idempotencyKey: "welcome:u_123" },
);
console.log(`enqueued ${job.id} status=${job.status}`);

// Idempotent re-enqueue (safe client retry)
const again = queue.enqueue(
  "send-welcome-email",
  { userId: "u_123", email: "ada@example.com" },
  { idempotencyKey: "welcome:u_123" },
);
console.log(`idempotent enqueue returned same id? ${again.id === job.id}`);

console.log("\n=== Claim + fail (will retry) ===");
let claimed = queue.claim("worker-1", 5_000);
if (!claimed) throw new Error("expected a job");
console.log(`claimed by ${claimed.leasedBy}, attempts=${claimed.attempts}`);
const failed = queue.fail(claimed.id, new Error("SMTP 421 temporary"));
console.log(
  `after fail: status=${failed.status} attempts=${failed.attempts} availableAt=${failed.availableAt}`,
);

console.log("\n=== Reclaim + complete ===");
claimed = queue.claim("worker-1", 5_000);
if (!claimed) throw new Error("expected retryable job");
const done = queue.complete(claimed.id);
console.log(`completed: status=${done.status}`);

console.log("\n=== Dead-letter path ===");
const poison = queue.enqueue(
  "charge-card",
  { amount: 42 },
  { maxAttempts: 2, backoffBaseMs: 1, idempotencyKey: "charge:42" },
);
void poison;
for (let i = 0; i < 2; i++) {
  const c = queue.claim("worker-2", 1_000);
  if (!c) break;
  const r = queue.fail(c.id, new Error(`processor down #${i + 1}`));
  console.log(`  fail #${i + 1}: status=${r.status} attempts=${r.attempts}`);
}
console.log(
  "dead-letter:",
  queue.listDead().map((j) => ({ id: j.id, name: j.name, err: j.lastError })),
);

console.log("\n=== Lease expiry reclaim ===");
queue.enqueue("crashed-worker-task", { step: 1 });
const leased = queue.claim("worker-a", 1_000);
if (!leased) throw new Error("expected lease");
console.log(`leased by ${leased.leasedBy} until ${leased.leaseExpiresAt}`);
now += 1_000; // lease expires
const reclaimed = queue.claim("worker-b", 1_000);
console.log(
  `reclaimed by ${reclaimed?.leasedBy} (same id? ${reclaimed?.id === leased.id})`,
);
if (reclaimed) queue.complete(reclaimed.id);

console.log("\n=== Done ===");
console.log(`total jobs in store: ${queue.list().length}`);
