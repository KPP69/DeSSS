import React, { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

export type SelectOption = { value: string; label: string };

type Pos = { top: number; left: number; width: number };

/**
 * Dropdown rendered through a React Portal on document.body, positioned
 * with position:fixed from the trigger's bounding rect. The option list
 * scrolls independently (max-h-56 + overflow-y-auto) and can never stretch
 * or be clipped by an ancestor's overflow. Supports mouse, outside-click
 * close, Esc, and ArrowUp/ArrowDown + Enter keyboard selection. While
 * open, its position tracks window scroll/resize.
 *
 * When `disabled`, the trigger is greyed out and cannot open (used for the
 * locked exchange selector in edit mode).
 */
export default function SelectDropdown({
  options,
  value,
  onChange,
  disabled = false,
  placeholder = "请选择",
}: {
  options: SelectOption[];
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  placeholder?: string;
}) {
  const btnRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<Pos>({ top: 0, left: 0, width: 0 });
  const [highlight, setHighlight] = useState(0);

  const selected = options.find((o) => o.value === value);

  const updatePos = useCallback(() => {
    const rect = btnRef.current?.getBoundingClientRect();
    if (rect) setPos({ top: rect.bottom + 4, left: rect.left, width: rect.width });
  }, []);

  const openMenu = useCallback(() => {
    if (disabled) return;
    updatePos();
    const idx = options.findIndex((o) => o.value === value);
    setHighlight(idx >= 0 ? idx : 0);
    setOpen(true);
  }, [disabled, options, value, updatePos]);

  const close = useCallback(() => setOpen(false), []);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (e: PointerEvent) => {
      const t = e.target as Node;
      if (!btnRef.current?.contains(t) && !menuRef.current?.contains(t)) close();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    window.addEventListener("pointerdown", onPointerDown, true);
    window.addEventListener("keydown", onKey);
    window.addEventListener("resize", updatePos);
    window.addEventListener("scroll", updatePos, true);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown, true);
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", updatePos);
      window.removeEventListener("scroll", updatePos, true);
    };
  }, [open, close, updatePos]);

  const onTriggerKey = (e: React.KeyboardEvent) => {
    if (disabled) return;
    if (!open && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
      e.preventDefault();
      openMenu();
      return;
    }
    if (!open) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlight((h) => Math.min(h + 1, options.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlight((h) => Math.max(h - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      const opt = options[highlight];
      if (opt) {
        onChange(opt.value);
        close();
      }
    }
  };

  return (
    <>
      <button
        ref={btnRef}
        type="button"
        disabled={disabled}
        onClick={() => (open ? close() : openMenu())}
        onKeyDown={onTriggerKey}
        aria-haspopup="listbox"
        aria-expanded={open}
        className={`w-full text-left px-3 py-2 rounded border transition-colors focus:outline-none focus:ring-2 focus:ring-primary/50 ${
          disabled
            ? "bg-panel/50 border-line text-t3 cursor-not-allowed"
            : "bg-panel border-line text-t1 hover:border-t3"
        }`}
      >
        {selected?.label ?? placeholder}
        {!disabled && <span className="float-right text-t3">▾</span>}
      </button>
      {open &&
        !disabled &&
        createPortal(
          <div
            ref={menuRef}
            role="listbox"
            className="fixed z-[9999] max-h-56 overflow-y-auto bg-overlay border border-line rounded shadow-2xl py-1"
            style={{ top: pos.top, left: pos.left, width: pos.width }}
          >
            {options.map((o, i) => (
              <button
                key={o.value}
                type="button"
                role="option"
                aria-selected={o.value === value}
                onMouseEnter={() => setHighlight(i)}
                onClick={() => {
                  onChange(o.value);
                  close();
                }}
                className={`block w-full text-left px-3 py-2 ${
                  i === highlight ? "bg-white/5" : ""
                } ${o.value === value ? "text-t1 font-medium" : "text-t1"}`}
              >
                {o.label}
              </button>
            ))}
          </div>,
          document.body
        )}
    </>
  );
}
