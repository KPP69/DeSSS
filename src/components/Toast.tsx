import React from "react";
import { useToastStore } from "../store/useToastStore";

/** Toast stack, top-right (mounted once in AppShell). Click to dismiss. */
export default function Toast() {
  const toasts = useToastStore((s) => s.toasts);
  const dismiss = useToastStore((s) => s.dismiss);
  if (toasts.length === 0) return null;
  return (
    <div className="fixed top-3 right-4 z-[9999] flex flex-col gap-2 w-80 pointer-events-none">
      {toasts.map((t) => (
        <button
          key={t.id}
          type="button"
          onClick={() => dismiss(t.id)}
          className={`pointer-events-auto text-left rounded-lg border px-3 py-2 text-[13px] text-ds-text-1 shadow-card transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50 ${
            t.kind === "error"
              ? "border-ds-down/60 bg-ds-down-soft/15"
              : "border-ds-up/60 bg-ds-up-soft/15"
          }`}
        >
          {t.message}
        </button>
      ))}
    </div>
  );
}
