import React from "react";

export type InputProps = Omit<React.InputHTMLAttributes<HTMLInputElement>, "size"> & {
  /** Error text: down border + a text-xs hint below the field. */
  error?: string;
};

/**
 * Single-line text / number / password input. Border is the
 * control-line token (error: down); focus deepens to accent + ring.
 * Number inputs are right-aligned with tabular numerals. Placeholder
 * uses text-3; disabled uses the disabled tokens.
 */
export default function Input({ error, type = "text", className = "", ...rest }: InputProps) {
  return (
    <span className="block">
      <input
        type={type}
        className={`w-full h-8 px-3 rounded-lg border bg-ds-panel-2 text-ds-text-1 outline-none transition-colors placeholder:text-ds-text-3 hover:border-ds-text-3 focus:border-ds-accent focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50 disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled disabled:cursor-not-allowed disabled:hover:border-ds-control-line ${
          error ? "border-ds-down" : "border-ds-control-line"
        } ${type === "number" ? "text-right num" : ""} ${className}`}
        {...rest}
      />
      {error && <span className="mt-1 block text-xs text-ds-down">{error}</span>}
    </span>
  );
}
