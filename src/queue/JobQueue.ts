import { randomUUID } from "node:crypto";
import { computeDelay } from "../backoff.js";
import { InMemoryJobStore } from "../store/InMemoryJobStore.js";
import type { EnqueueOptions, Job } from "../types.js";

export interface JobQueueOptions {
  /** Injectable clock for tests. Defaults to Date.now. */
  now?: () => number;
  /** Injectable RNG for backoff jitter. Defaults to Math.random. */
  random?: () => number;
  /** Shared or custom store. Defaults to a fresh InMemoryJobStore. */
  store?: InMemoryJobStore;
}

/**
 * Durable-style in-process job queue.
 *
 * Workers claim jobs under time-bounded leases. If a worker crashes, the
 * lease expires and another worker can reclaim the job. Failures retry with
 * exponential backoff + jitter until maxAttempts, then move to the
 * dead-letter queue. Optional idempotency keys make enqueue safe to retry.
 */
export class JobQueue {
  private readonly store: InMemoryJobStore;
  private readonly now: () => number;
  private readonly random: () => number;

  constructor(options: JobQueueOptions = {}) {
    this.store = options.store ?? new InMemoryJobStore();
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
  }

  /**
   * Enqueue a job. If `opts.idempotencyKey` is set and a job with that key
   * already exists, returns the existing job (no duplicate created).
   */
  enqueue<TPayload = unknown>(
    name: string,
    payload: TPayload,
    opts: EnqueueOptions = {},
  ): Job<TPayload> {
    if (opts.idempotencyKey !== undefined) {
      const existing = this.store.getByIdempotencyKey(opts.idempotencyKey);
      if (existing !== undefined) {
        return existing as Job<TPayload>;
      }
    }

    const ts = this.now();
    const delayMs = opts.delayMs ?? 0;
    const job: Job<TPayload> = {
      id: randomUUID(),
      name,
      payload,
      status: "pending",
      attempts: 0,
      maxAttempts: opts.maxAttempts ?? 3,
      backoffBaseMs: opts.backoffBaseMs ?? 100,
      backoffMaxMs: opts.backoffMaxMs ?? 30_000,
      availableAt: ts + delayMs,
      createdAt: ts,
      updatedAt: ts,
      ...(opts.idempotencyKey !== undefined
        ? { idempotencyKey: opts.idempotencyKey }
        : {}),
    };

    this.store.save(job as Job);
    return job;
  }

  /**
   * Claim the next eligible job for `workerId` with a lease of `leaseMs`.
   *
   * Eligible jobs are `pending`/`failed` past `availableAt`, or `leased`
   * jobs whose lease has expired (crash recovery / reclaim).
   *
   * Returns `undefined` if nothing is claimable.
   */
  claim(workerId: string, leaseMs: number): Job | undefined {
    if (leaseMs <= 0) {
      throw new RangeError("leaseMs must be > 0");
    }

    const ts = this.now();
    const candidates = this.store
      .list()
      .filter((j) => this.isClaimable(j, ts))
      .sort((a, b) => a.availableAt - b.availableAt || a.createdAt - b.createdAt);

    const next = candidates[0];
    if (next === undefined) {
      return undefined;
    }

    const leased: Job = {
      ...next,
      status: "leased",
      leasedBy: workerId,
      leaseExpiresAt: ts + leaseMs,
      updatedAt: ts,
    };
    this.store.update(leased);
    return leased;
  }

  /**
   * Mark a leased job as successfully completed.
   */
  complete(id: string): Job {
    const job = this.requireJob(id);
    this.assertLeased(job);
    const ts = this.now();
    const done: Job = {
      ...job,
      status: "succeeded",
      leasedBy: undefined,
      leaseExpiresAt: undefined,
      updatedAt: ts,
    };
    this.store.update(done);
    return done;
  }

  /**
   * Record a failure. Increments attempts. If attempts >= maxAttempts the
   * job moves to the dead-letter queue (`dead`); otherwise it becomes
   * `failed`/`pending`-eligible after exponential backoff + jitter.
   */
  fail(id: string, err: unknown): Job {
    const job = this.requireJob(id);
    this.assertLeased(job);

    const ts = this.now();
    const attempts = job.attempts + 1;
    const message = err instanceof Error ? err.message : String(err);

    if (attempts >= job.maxAttempts) {
      const dead: Job = {
        ...job,
        status: "dead",
        attempts,
        lastError: message,
        leasedBy: undefined,
        leaseExpiresAt: undefined,
        updatedAt: ts,
      };
      this.store.update(dead);
      return dead;
    }

    const delay = computeDelay(
      attempts - 1,
      job.backoffBaseMs,
      job.backoffMaxMs,
      this.random,
    );
    const retry: Job = {
      ...job,
      status: "failed",
      attempts,
      lastError: message,
      leasedBy: undefined,
      leaseExpiresAt: undefined,
      availableAt: ts + delay,
      updatedAt: ts,
    };
    this.store.update(retry);
    return retry;
  }

  /**
   * Extend the lease on a claimed job (keep-alive while work is in progress).
   */
  heartbeat(id: string, leaseMs: number): Job {
    if (leaseMs <= 0) {
      throw new RangeError("leaseMs must be > 0");
    }
    const job = this.requireJob(id);
    this.assertLeased(job);
    const ts = this.now();
    if (job.leaseExpiresAt !== undefined && job.leaseExpiresAt < ts) {
      throw new Error(`Lease expired for job: ${id}`);
    }
    const updated: Job = {
      ...job,
      leaseExpiresAt: ts + leaseMs,
      updatedAt: ts,
    };
    this.store.update(updated);
    return updated;
  }

  /** All jobs currently in the dead-letter queue. */
  listDead(): Job[] {
    return this.store.listByStatus("dead");
  }

  /** Read a job by id (or undefined). */
  get(id: string): Job | undefined {
    return this.store.get(id);
  }

  /** All jobs (snapshot). Useful for demos and introspection. */
  list(): Job[] {
    return this.store.list();
  }

  private isClaimable(job: Job, ts: number): boolean {
    if (job.status === "succeeded" || job.status === "dead") {
      return false;
    }
    if (job.status === "leased") {
      return job.leaseExpiresAt !== undefined && job.leaseExpiresAt <= ts;
    }
    // pending or failed — respect backoff / delay gate
    return job.availableAt <= ts;
  }

  private requireJob(id: string): Job {
    const job = this.store.get(id);
    if (job === undefined) {
      throw new Error(`Job not found: ${id}`);
    }
    return job;
  }

  private assertLeased(job: Job): void {
    if (job.status !== "leased") {
      throw new Error(
        `Job ${job.id} is not leased (status=${job.status}); cannot mutate`,
      );
    }
  }
}
