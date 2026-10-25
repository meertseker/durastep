/**
 * Structured metadata attached to a log line. Values must be JSON-friendly.
 */
export type LogMeta = Readonly<Record<string, unknown>>;

/**
 * The minimal logger durastep writes to. Any structured logger (pino,
 * winston, console) can be adapted to this shape in three lines.
 */
export interface Logger {
  /** Routine lifecycle events: a step scheduled, a run finished. */
  info(message: string, meta?: LogMeta): void;
  /** Something unexpected that durastep recovered from. */
  warn(message: string, meta?: LogMeta): void;
  /** Something that needs a human: a lost notify, a failed dispatch. */
  error(message: string, meta?: LogMeta): void;
}

/** A logger that drops everything. The default when none is supplied. */
export const noopLogger: Logger = {
  info() {},
  warn() {},
  error() {},
};

/**
 * A logger that writes one line per event to the console, with the
 * metadata serialised as JSON. Handy in development.
 */
export function consoleLogger(prefix = "durastep"): Logger {
  const line = (message: string, meta?: LogMeta) =>
    meta && Object.keys(meta).length > 0
      ? `[${prefix}] ${message} ${JSON.stringify(meta)}`
      : `[${prefix}] ${message}`;
  return {
    info: (message, meta) => console.info(line(message, meta)),
    warn: (message, meta) => console.warn(line(message, meta)),
    error: (message, meta) => console.error(line(message, meta)),
  };
}

/** Formats an unknown thrown value as a message string. */
export function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;
  try {
    return JSON.stringify(err) ?? String(err);
  } catch {
    return String(err);
  }
}
