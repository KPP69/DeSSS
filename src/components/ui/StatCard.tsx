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
}: {
  title: string;
  value: string;
  sub?: string;
  tone?: Tone;
}) {
  const valueColor =
    tone === "profit" ? "text-profit" : tone === "loss" ? "text-loss" : "text-t1";
  return (
    <div className="bg-panel border border-line rounded-lg p-5 flex flex-col items-center text-center">
      <div className="text-sm text-t2 text-center">{title}</div>
      <div className={`num text-2xl font-semibold mt-2 text-center ${valueColor}`}>{value}</div>
      {sub && <div className="text-[11px] text-t3 mt-1 text-center">{sub}</div>}
    </div>
  );
}
