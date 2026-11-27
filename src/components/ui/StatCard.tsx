import React from "react";

type Tone = "default" | "profit" | "loss";

/**
 * Overview stat card: title, value and helper line, all center-aligned.
 * Values always use tabular numerals so live updates do not jitter.
 */
export default function StatCard({
  title,
  value,
  sub,
  tone = "default",
  compact = false,
  change,
}: {
  title: string;
  value: string;
  sub?: string;
  tone?: Tone;
  /** Compact variant: tighter padding and smaller text (account overview). */
  compact?: boolean;
  /** Optional change line, e.g. "+2.4% 今日" — colored up / down. */
  change?: { text: string; tone: "up" | "down" };
}) {
  const valueColor =
    tone === "profit" ? "text-ds-up" : tone === "loss" ? "text-ds-down" : "text-ds-text-1";
  return (
    <div
      className={`bg-ds-panel border border-ds-line rounded-[10px] shadow-card flex flex-col items-center text-center ${
        compact ? "px-3 py-2" : "p-5"
      }`}
    >
      <div className="text-xs text-ds-text-2 text-center">{title}</div>
      <div className={`num font-semibold text-center ${compact ? "text-[15px] mt-1" : "text-base mt-1"} ${valueColor}`}>{value}</div>
      {change && (
        <div className={`num text-xs mt-1 ${change.tone === "up" ? "text-ds-up" : "text-ds-down"}`}>
          {change.text}
        </div>
      )}
      {sub && <div className="text-xs text-ds-text-3 mt-1 text-center">{sub}</div>}
    </div>
  );
}
