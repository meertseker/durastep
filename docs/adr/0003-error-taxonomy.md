# ADR 0003: An error taxonomy instead of "retry everything"

- Status: accepted
- Date: 2026-10-25

## Context

Queues retry what throws. In the production pipeline that meant schema
violations, exhausted quotas and refused prompts were retried with
exponential backoff, burning tokens and scraping credits on work that would
deterministically fail again, while the run stayed in limbo for minutes.
Conversely, treating every failure as final lost runs to transient network
errors and provider 5xx responses.

There was also a third case that is neither: a worker with no capacity right
now (all browser slots taken). Retrying it spends attempts; failing it is
wrong.

## Decision

Three classes, each with one handling rule in the executor and dispatchers:

| class                    | meaning                                                  | executor                                   | dispatcher                                             |
| ------------------------ | -------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------ |
| `RetryableError`         | transient                                                | run FAILED (marked retry-pending), rethrow | retry with backoff; last attempt finalises             |
| `PermanentError(reason)` | will never succeed (`schema`, `input`, `quota`, `other`) | run FAILED, advance the session            | do not retry (BullMQ `UnrecoverableError`)             |
| `DeferError(delayMs)`    | not now                                                  | run back to PENDING, `deferCount + 1`      | re-deliver after `delayMs` without spending an attempt |

Everything else goes through `classifyError(err, matchers?)`: user matchers
first, then generic defaults. Retryable rules are checked before permanent
ones (timeouts, aborts, network codes, pool back-pressure, HTTP 408/429/5xx),
then permanent ones (schema violations, invalid JSON, quota, HTTP
400/401/403/404/422). Unknown errors are retryable: retrying once is cheaper
than losing a run to a flaky dependency, and the retry budget bounds the cost.

The outer deadline is a `RetryableError` that also aborts `ctx.signal`, so
the work stops, not just the wait.

## Consequences

- Permanent failures cost one attempt, not three.
- Classification is heuristic for third-party errors; teams add matchers for
  their providers (`{ match: /content filtered/, retryable: false, reason: "input" }`).
- Error classes carry a string brand checked by `isRetryableError` /
  `isPermanentError` / `isDeferError`, so classification survives duplicated
  module copies (ESM and CJS loaded side by side).
