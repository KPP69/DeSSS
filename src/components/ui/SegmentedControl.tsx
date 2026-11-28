import React, { useRef } from "react";

export type SegmentedOption = { value: string; label: string };

/**
 * Segmented control (F5.1a): 2+ options in one pill group, the one
 * place a view / segment switch lives (DataPage view toggle,
 * TradingPage 持仓/记录）. Roving tabindex — Tab lands on the
 * selected option; ←/→ moves focus (wrapping); Enter / Space
 * selects the focused option. The selected pill takes the accent
 * fill; focus uses the shared focus ring. Labels are 正文 (13px);
 * the group radius is the 控件档 (8px).
 */
export default function SegmentedControl({
  options,
  value,
  onChange,
  ariaLabel,
}: {
  options: SegmentedOption[];
  value: string;
  onChange: (value: string) => void;
  /** Accessible name for the group (e.g. "视图切换"). */
  ariaLabel: string;
}) {
  const btnRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const focusOption = (i: number) => {
    const n = options.length;
    btnRefs.current[(i + n) % n]?.focus();
  };
  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      className="flex rounded-lg border border-ds-line bg-ds-panel p-0.5"
    >
      {options.map((o, i) => {
        const selected = o.value === value;
        return (
          <button
            key={o.value}
            ref={(el) => {
              btnRefs.current[i] = el;
            }}
            role="radio"
            aria-checked={selected}
            tabIndex={selected ? 0 : -1}
            onClick={() => onChange(o.value)}
            onKeyDown={(e) => {
              if (e.key === "ArrowRight") {
                e.preventDefault();
                focusOption(i + 1);
              } else if (e.key === "ArrowLeft") {
                e.preventDefault();
                focusOption(i - 1);
              } else if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onChange(o.value);
              }
            }}
            className={`px-3 py-1 rounded-lg text-[13px] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50 ${
              selected
                ? "bg-ds-accent text-ds-accent-contrast"
                : "text-ds-text-2 hover:text-ds-text-1"
            }`}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}
