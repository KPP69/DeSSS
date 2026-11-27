import React from "react";

type Tone =
  | "up"
  | "down"
  | "warn"
  | "info"
  | "neutral"
  | "online"
  | "offline"
  | "configured"
  | "danger";

// Badge pattern: 5px dot + text, 20px tall, 5px radius, 11px text.
// Semantic tones carry meaning through the dot + the colored label together
// (the text itself always says what the state is); neutral chrome is used
// for plain labels such as configured.
// The canonical tones are neutral / up / down / warn / info; online,
// offline, configured and danger are aliases kept for existing pages
// (F2 does not rewrite pages).
const tones: Record<Tone, { cls: string; dot: string }> = {
  up: { cls: "text-ds-up border-ds-up/30 bg-ds-up-soft/10", dot: "bg-ds-up" },
  online: { cls: "text-ds-up border-ds-up/30 bg-ds-up-soft/10", dot: "bg-ds-up" },
  down: { cls: "text-ds-down border-ds-down/30 bg-ds-down-soft/10", dot: "bg-ds-down" },
  danger: { cls: "text-ds-down border-ds-down/30 bg-ds-down-soft/10", dot: "bg-ds-down" },
  warn: { cls: "text-ds-warn border-ds-warn/30 bg-ds-warn-soft/10", dot: "bg-ds-warn" },
  info: { cls: "text-ds-info border-ds-info/30 bg-ds-info-soft/10", dot: "bg-ds-info" },
  neutral: { cls: "text-ds-text-2 border-ds-line bg-ds-panel-2", dot: "bg-ds-text-3" },
  offline: { cls: "text-ds-text-2 border-ds-line bg-ds-panel-2", dot: "bg-ds-text-3" },
  configured: { cls: "text-ds-text-2 border-ds-line bg-ds-panel-2", dot: "bg-ds-text-3" },
};

export default function Badge({
  tone = "neutral",
  size = "sm",
  children,
}: {
  tone?: Tone;
  /** sm is the default (h-5, 11px); md is h-6, 12px. */
  size?: "sm" | "md";
  children: React.ReactNode;
}) {
  const t = tones[tone];
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-[5px] border leading-none ${
        size === "md" ? "h-6 px-2 text-xs" : "h-5 px-1.5 text-[11px]"
      } ${t.cls}`}
    >
      <span className={`inline-block w-[5px] h-[5px] rounded-full shrink-0 ${t.dot}`} />
      {children}
    </span>
  );
}
