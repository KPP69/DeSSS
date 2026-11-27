import React from "react";

export type CheckboxProps = Omit<React.InputHTMLAttributes<HTMLInputElement>, "type"> & {
  label?: React.ReactNode;
};

/**
 * Native checkbox with a custom look: appearance-none box, accent
 * fill + white tick when checked, control-line border, focus-visible
 * ring, disabled greys out. The tick is a sibling SVG shown by
 * peer-checked, so no JS is needed for the visual state.
 */
export default function Checkbox({ label, className = "", ...rest }: CheckboxProps) {
  return (
    <label className={`inline-flex items-center gap-2 cursor-pointer text-[13px] text-ds-text-1 ${className}`}>
      <span className="relative inline-flex h-4 w-4 shrink-0">
        <input
          type="checkbox"
          className="peer h-4 w-4 appearance-none rounded border border-ds-control-line bg-ds-panel transition-colors hover:border-ds-text-3 checked:bg-ds-accent checked:border-ds-accent checked:hover:border-ds-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring disabled:cursor-not-allowed disabled:border-ds-line disabled:bg-ds-bg-disabled disabled:checked:bg-ds-text-disabled disabled:hover:border-ds-line"
          {...rest}
        />
        <svg
          className="pointer-events-none absolute inset-0 m-auto h-3 w-3 text-ds-accent-contrast opacity-0 peer-checked:opacity-100"
          viewBox="0 0 12 12"
          fill="none"
          aria-hidden="true"
        >
          <path d="M2 6.5 4.8 9 10 3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </span>
      {label}
    </label>
  );
}
