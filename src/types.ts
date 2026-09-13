/**
 * Lifecycle status of a job in the queue.
 *
 * - `pending`  — waiting to be claimed (or awaiting backoff after a failure)
 * - `leased`   — claimed by a worker; exclusive until lease expires or completes
 * - `succeeded`— terminal success
 * - `failed`   — non-terminal failure; will be retried after backoff
 * - `dead`     — terminal failure after exhausting maxAttempts (dead-letter)
 */
export type JobStatus =
  | "pending"
  | "leased"
  | "succeeded"
  | "failed"
  | "dead";

/**
 * Persisted job record.
 */
export interface Job<TPayload = unknown> {
  id: string;
  name: string;
  payload: TPayload;
  status: JobStatus;
  /** Completed (failed) attempts so far. Starts at 0. */
  attempts: number;
  maxAttempts: number;
  /** Base delay (ms) for exponential backoff. */
  backoffBaseMs: number;
  /** Cap on backoff delay (ms). */
  backoffMaxMs: number;
  /** Optional client-supplied key; enqueue is a no-op if already present. */
  idempotencyKey?: string;
  /** Worker currently holding the lease, if any. */
  leasedBy?: string;
  /** Absolute timestamp (ms since epoch) when the lease expires. */
  leaseExpiresAt?: number;
  /** Absolute timestamp when the job becomes eligible to claim again (backoff). */
  availableAt: number;
  /** Last error message (from fail). */
  lastError?: string;
  createdAt: number;
  updatedAt: number;
}

/**
 * Options accepted by {@link JobQueue.enqueue}.
 */
export interface EnqueueOptions {
  /** Maximum attempts before the job is moved to the dead-letter queue. Default: 3. */
  maxAttempts?: number;
  /** Base delay in ms for exponential backoff. Default: 100. */
  backoffBaseMs?: number;
  /** Maximum backoff delay in ms. Default: 30_000. */
  backoffMaxMs?: number;
  /**
   * Client-supplied idempotency key. If a job with this key already exists,
   * enqueue returns the existing job without creating a duplicate.
   */
  idempotencyKey?: string;
  /** Delay (ms) before the job first becomes claimable. Default: 0. */
  delayMs?: number;
}
