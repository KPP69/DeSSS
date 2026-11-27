import React, { useEffect, useRef } from "react";

/**
 * Unified modal: dimmed backdrop, panel-2 container, independently
 * scrolling content (usable at the 960x640 minimum window size).
 * Overlay ds-overlay/50. Esc closes, backdrop click closes. On open,
 * focus moves in and body scroll locks; on close, focus returns to
 * the opener. Sizes: sm (420px, confirms) / md (760px, forms).
 */
export default function Modal({
  open,
  title,
  onClose,
  children,
  footer,
  size = "md",
  widthClass,
}: {
  open: boolean;
  title: string;
  onClose: () => void;
  children: React.ReactNode;
  footer?: React.ReactNode;
  size?: "sm" | "md";
  /** Explicit width overrides `size` (kept for existing callers). */
  widthClass?: string;
}) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const prevFocus = useRef<Element | null>(null);

  useEffect(() => {
    if (!open) return;
    prevFocus.current = document.activeElement;
    dialogRef.current?.focus();
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
      (prevFocus.current as HTMLElement | null)?.focus?.();
    };
  }, [open, onClose]);

  if (!open) return null;

  const width = widthClass ?? (size === "sm" ? "w-[420px]" : "w-[760px]");
  return (
    <div
      className="fixed inset-0 z-40 flex items-center justify-center bg-ds-overlay/50 p-4"
      onClick={onClose}
    >
      <div
        ref={dialogRef}
        tabIndex={-1}
        className={`bg-ds-panel-2 border border-ds-line rounded-[10px] ${width} max-w-[94vw] max-h-[90vh] flex flex-col shadow-2xl focus:outline-none`}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-6 py-4 shrink-0">
          <h2 className="text-[15px] font-semibold text-ds-text-1">{title}</h2>
          <button
            onClick={onClose}
            className="text-ds-text-2 hover:text-ds-text-1 text-base leading-none px-1 rounded focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50"
            aria-label="关闭"
          >
            ×
          </button>
        </div>
        <div className="px-6 pb-4 overflow-y-auto min-h-0">{children}</div>
        {footer && <div className="px-6 py-4 shrink-0 flex justify-end gap-3">{footer}</div>}
      </div>
    </div>
  );
}
