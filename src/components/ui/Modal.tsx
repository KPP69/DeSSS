import React, { useEffect } from "react";

/**
 * Unified modal: dimmed backdrop, overlay-colored panel, and a content
 * region that scrolls independently so the dialog never stretches past
 * the window (usable at the 960x640 minimum window size).
 */
export default function Modal({
  open,
  title,
  onClose,
  children,
  footer,
  widthClass = "w-[760px]",
}: {
  open: boolean;
  title: string;
  onClose: () => void;
  children: React.ReactNode;
  footer?: React.ReactNode;
  widthClass?: string;
}) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-ds-overlay/60 p-4">
      <div
        className={`bg-ds-panel-2 border border-ds-line rounded-[10px] ${widthClass} max-w-[94vw] max-h-[90vh] flex flex-col shadow-2xl`}
        role="dialog"
        aria-modal="true"
        aria-label={title}
      >
        <div className="flex items-center justify-between px-6 py-4 shrink-0">
          <h2 className="text-[15px] font-semibold text-ds-text-1">{title}</h2>
          <button
            onClick={onClose}
            className="text-ds-text-2 hover:text-ds-text-1 text-base leading-none px-1"
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
