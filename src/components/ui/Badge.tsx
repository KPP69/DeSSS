import React from "react";

type Tone = "online" | "offline" | "mock" | "configured" | "warn" | "danger" | "neutral";

// 90/10 rule: badges are always neutral chrome. The tone prop stays for
// API compatibility; meaning comes from the text (and the 6px status dot
// the caller may put inside), never from a colored border or label.
const tones: Record<Tone, string> = {
  online: "text-t2 border-line bg-panel",
  configured: "text-t2 border-line bg-panel",
  offline: "text-t2 border-line bg-panel",
  neutral: "text-t2 border-line bg-panel",
  mock: "text-t2 border-line bg-panel",
  warn: "text-t2 border-line bg-panel",
  danger: "text-t2 border-line bg-panel",
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
      className={`inline-flex items-center gap-1 text-xs leading-5 border rounded px-1.5 ${tones[tone]}`}
    >
      {children}
    </span>
  );
}
