import React, { useRef, useState } from "react";

const HOVER_DELAY_MS = 400;

/**
 * Tooltip for icon explanations and truncated text only — never for
 * information the user must have. Hover shows after 400ms; keyboard
 * focus shows immediately (focus/blur bubble from any focusable child,
 * so the trigger itself needs no extra tab stop).
 */
export default function Tooltip({
  content,
  children,
}: {
  content: React.ReactNode;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const showSoon = () => {
    timer.current = setTimeout(() => setOpen(true), HOVER_DELAY_MS);
  };
  const hide = () => {
    if (timer.current) {
      clearTimeout(timer.current);
      timer.current = null;
    }
    setOpen(false);
  };
  return (
    <span
      className="relative inline-flex"
      onMouseEnter={showSoon}
      onMouseLeave={hide}
      onFocusCapture={() => setOpen(true)}
      onBlurCapture={hide}
    >
      {children}
      {open && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 z-[9999] max-w-56 whitespace-normal break-words rounded border border-ds-line bg-ds-panel-2 px-2 py-1 text-xs text-ds-text-1 shadow-card pointer-events-none">
          {content}
        </span>
      )}
    </span>
  );
}
