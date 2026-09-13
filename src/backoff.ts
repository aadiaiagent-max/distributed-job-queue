/**
 * Compute exponential backoff delay with full jitter.
 *
 * delay = min(maxMs, baseMs * 2^attempt) then uniform jitter in [0, delay].
 *
 * Full jitter (AWS architecture blog style) reduces thundering herds when
 * many workers retry after a correlated failure.
 *
 * @param attempt - Zero-based attempt count after a failure (0 = first retry)
 * @param baseMs  - Base delay in milliseconds
 * @param maxMs   - Cap on the exponential component before jitter
 * @param random  - Injectable RNG in [0, 1) for deterministic tests
 */
export function computeDelay(
  attempt: number,
  baseMs: number,
  maxMs: number,
  random: () => number = Math.random,
): number {
  if (attempt < 0) {
    throw new RangeError("attempt must be >= 0");
  }
  if (baseMs < 0 || maxMs < 0) {
    throw new RangeError("baseMs and maxMs must be >= 0");
  }
  const exp = Math.min(maxMs, baseMs * 2 ** attempt);
  return Math.floor(random() * (exp + 1));
}
