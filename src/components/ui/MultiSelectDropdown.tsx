import React, { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { SelectOption } from "./SelectDropdown";

type Pos = { top: number; left: number; width: number };

/**
 * Multi-select dropdown on the same React Portal architecture as
 * `SelectDropdown`: the menu floats on document.body, positioned with
 * position:fixed from the trigger's bounding rect, and scrolls
 * independently (max-h-56 + overflow-y-auto), so it can hold hundreds of
 * options without ever stretching or being clipped by an ancestor's
 * overflow. The trigger shows a compact count (`交易所 (6/6) ▾`) and
 * never changes width as options are toggled. The menu stays open while
 * toggling; its header offers 全选 / 清空. Supports mouse, outside-click
 * close, Esc, and ArrowUp/ArrowDown + Enter/Space keyboard toggling.
 * While open, its position tracks window scroll/resize.
 */
export default function MultiSelectDropdown({
  options,
  values,
  onChange,
  label = "选择",
  disabled = false,
}: {
  options: SelectOption[];
  values: string[];
  onChange: (values: string[]) => void;
  label?: string;
  disabled?: boolean;
}) {
  const btnRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<Pos>({ top: 0, left: 0, width: 0 });
  const [highlight, setHighlight] = useState(0);

  const selected = new Set(values);
  const allSelected = options.length > 0 && values.length === options.length;

  const updatePos = useCallback(() => {
    const rect = btnRef.current?.getBoundingClientRect();
    if (rect) setPos({ top: rect.bottom + 4, left: rect.left, width: rect.width });
  }, []);

  const openMenu = useCallback(() => {
    if (disabled) return;
    updatePos();
    setHighlight(0);
    setOpen(true);
  }, [disabled, updatePos]);

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

  const toggle = (value: string) => {
    onChange(
      selected.has(value)
        ? values.filter((v) => v !== value)
        : [...values, value],
    );
  };

  const selectAll = () => onChange(options.map((o) => o.value));
  const clearAll = () => onChange([]);

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
    } else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      const opt = options[highlight];
      if (opt) toggle(opt.value);
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
        className={`w-full text-left h-8 px-3 rounded-lg border transition-colors focus:outline-none focus:ring-2 focus:ring-ds-ring/50 ${
          disabled
            ? "bg-ds-panel/50 border-ds-line text-ds-text-3 cursor-not-allowed"
            : "bg-ds-panel border-ds-line text-ds-text-1 hover:border-ds-text-3"
        }`}
      >
        <span className="whitespace-nowrap">
          {label} ({values.length}/{options.length})
        </span>
        {!disabled && <span className="float-right text-ds-text-3">▾</span>}
      </button>
      {open &&
        !disabled &&
        createPortal(
          <div
            ref={menuRef}
            role="listbox"
            aria-multiselectable
            className="fixed z-[9999] max-h-56 overflow-y-auto bg-ds-panel-2 border border-ds-line rounded shadow-2xl py-1"
            style={{ top: pos.top, left: pos.left, width: pos.width }}
          >
            <div className="flex items-center justify-between px-3 py-1.5 border-b border-ds-line">
              <button
                type="button"
                onClick={selectAll}
                disabled={allSelected}
                className="text-xs text-ds-accent hover:underline disabled:text-ds-text-3 disabled:no-underline"
              >
                全选
              </button>
              <button
                type="button"
                onClick={clearAll}
                disabled={values.length === 0}
                className="text-xs text-ds-text-2 hover:text-ds-text-1 disabled:text-ds-text-3"
              >
                清空
              </button>
            </div>
            {options.map((o, i) => (
              <label
                key={o.value}
                role="option"
                aria-selected={selected.has(o.value)}
                onMouseEnter={() => setHighlight(i)}
                className={`flex items-center gap-2 px-3 py-2 cursor-pointer text-ds-text-1 ${
                  i === highlight ? "bg-ds-accent-soft/10" : ""
                }`}
              >
                <input
                  type="checkbox"
                  checked={selected.has(o.value)}
                  onChange={() => toggle(o.value)}
                  className="accent-ds-accent"
                />
                <span className={selected.has(o.value) ? "font-medium" : ""}>{o.label}</span>
              </label>
            ))}
          </div>,
          document.body
        )}
    </>
  );
}
