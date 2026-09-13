import type { Job } from "../types.js";

/**
 * In-memory persistence layer for jobs.
 *
 * Intentionally simple so the queue semantics (leases, retries, DLQ,
 * idempotency) are easy to reason about and unit-test without I/O.
 * A durable backend would implement the same surface area.
 */
export class InMemoryJobStore {
  private readonly jobs = new Map<string, Job>();
  private readonly byIdempotencyKey = new Map<string, string>();

  save(job: Job): void {
    this.jobs.set(job.id, structuredClone(job));
    if (job.idempotencyKey !== undefined) {
      this.byIdempotencyKey.set(job.idempotencyKey, job.id);
    }
  }

  get(id: string): Job | undefined {
    const job = this.jobs.get(id);
    return job === undefined ? undefined : structuredClone(job);
  }

  getByIdempotencyKey(key: string): Job | undefined {
    const id = this.byIdempotencyKey.get(key);
    if (id === undefined) {
      return undefined;
    }
    return this.get(id);
  }

  update(job: Job): void {
    if (!this.jobs.has(job.id)) {
      throw new Error(`Job not found: ${job.id}`);
    }
    this.jobs.set(job.id, structuredClone(job));
  }

  list(): Job[] {
    return Array.from(this.jobs.values()).map((j) => structuredClone(j));
  }

  listByStatus(status: Job["status"]): Job[] {
    return this.list().filter((j) => j.status === status);
  }

  clear(): void {
    this.jobs.clear();
    this.byIdempotencyKey.clear();
  }

  get size(): number {
    return this.jobs.size;
  }
}
