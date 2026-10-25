import type { RunFinishedEvent, Session } from "../../src/index";

/** One attempt as observed through the telemetry hook. */
export interface Attempt extends RunFinishedEvent {
  /** Epoch ms when the attempt finished. */
  endedAt: number;
}

/** Collects attempts from `onRunFinished`. */
export function recorder() {
  const attempts: Attempt[] = [];
  return {
    attempts,
    onRunFinished(event: RunFinishedEvent) {
      attempts.push({ ...event, endedAt: Date.now() });
    },
  };
}

const pad = (s: string, n: number) => s.padEnd(n);
const padL = (s: string, n: number) => s.padStart(n);

/** Renders one row per attempt with an ASCII bar on a shared time axis. */
export function renderTimeline(session: Session, attempts: Attempt[], width = 40): string {
  const t0 = Math.min(...attempts.map((a) => a.endedAt - a.durationMs));
  const t1 = Math.max(...attempts.map((a) => a.endedAt));
  const scale = width / Math.max(1, t1 - t0);
  const rows = [...attempts].sort((a, b) => a.endedAt - a.durationMs - (b.endedAt - b.durationMs));
  const lines = [
    `${pad("step", 11)} ${pad("worker", 11)} ${pad("try", 3)} ${pad("result", 9)} ${padL("start", 6)} ${padL("end", 6)}  timeline`,
    "-".repeat(11 + 1 + 11 + 1 + 3 + 1 + 9 + 1 + 6 + 1 + 6 + 2 + width),
  ];
  for (const a of rows) {
    const start = a.endedAt - a.durationMs - t0;
    const end = a.endedAt - t0;
    const from = Math.floor(start * scale);
    const len = Math.max(1, Math.round((end - start) * scale));
    const bar = " ".repeat(from) + (a.status === "SUCCEEDED" ? "#" : "x").repeat(len);
    const result = a.status === "SUCCEEDED" ? "ok" : a.willRetry ? "retry" : "failed";
    lines.push(
      `${pad(a.stepId, 11)} ${pad(a.worker, 11)} ${pad(String(a.attempt), 3)} ${pad(result, 9)} ${padL(`${start}ms`, 6)} ${padL(`${end}ms`, 6)}  ${bar}`,
    );
  }
  lines.push("");
  lines.push(`session ${session.status}`);
  for (const step of session.plan) lines.push(`  ${pad(step.stepId, 11)} ${step.status}`);
  return lines.join("\n");
}
