import React from "react";

type Tone = "online" | "offline" | "mock" | "configured" | "warn" | "danger" | "neutral";

const tones: Record<Tone, string> = {
  online: "text-profit border-profit/40 bg-profit/10",
  configured: "text-profit border-profit/40 bg-profit/10",
  offline: "text-t3 border-line bg-panel",
  neutral: "text-t2 border-line bg-panel",
  mock: "text-warn border-warn/40 bg-warn/10",
  warn: "text-warn border-warn/40 bg-warn/10",
  danger: "text-loss border-loss/40 bg-loss/10",
};

export default function Badge({
  tone = "neutral",
  children,
}: {
  tone?: Tone;
  children: React.ReactNode;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1 text-[11px] leading-5 border rounded px-1.5 ${tones[tone]}`}
    >
      {children}
    </span>
  );
}
