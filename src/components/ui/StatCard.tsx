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
}: {
  title: string;
  value: string;
  sub?: string;
  tone?: Tone;
  /** Compact variant: tighter padding and smaller text (account overview). */
  compact?: boolean;
}) {
  const valueColor =
    tone === "profit" ? "text-profit" : tone === "loss" ? "text-loss" : "text-t1";
  return (
    <div
      className={`bg-panel border border-line rounded-lg flex flex-col items-center text-center ${
        compact ? "px-3 py-2" : "p-5"
      }`}
    >
      <div className={`${compact ? "text-xs" : "text-sm"} text-t2 text-center`}>{title}</div>
      <div className={`num font-semibold text-center ${compact ? "text-xl mt-1" : "text-2xl mt-2"} ${valueColor}`}>{value}</div>
      {sub && <div className="text-[11px] text-t3 mt-1 text-center">{sub}</div>}
    </div>
  );
}
