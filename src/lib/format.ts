/**
 * Shared number / time formatting (F5.0): one place for the formats
 * the pages used to duplicate. Clock times are `HH:MM:SS`; a null
 * input is the empty value `—`, never a zero or a blank.
 */

/** `HH:MM:SS` (zh-CN, 24h); null → `—`. */
export function fmtClock(ms: number | null): string {
  if (ms == null) return "—";
  return new Date(ms).toLocaleTimeString("zh-CN", { hour12: false });
}
