import React from "react";
import Spinner from "./Spinner";

type Variant = "primary" | "secondary" | "danger" | "ghost";
type Size = "sm" | "md";

// Controls are 32px tall (md) with 8px radius; table actions use sm (28px).
// Disabled uses the disabled tokens (never just opacity, so the label
// stays legible); focus uses the shared focus-ring token, keyboard-only
// (focus-visible) so mouse clicks do not draw a ring.
// Loading keeps the label in the layout (invisible) and centers a
// spinner over it, so the width never jumps.
const base =
  "relative inline-flex items-center justify-center font-medium transition-colors disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled disabled:cursor-not-allowed focus:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50";

const variants: Record<Variant, string> = {
  primary: "bg-ds-accent text-ds-accent-contrast hover:bg-ds-accent-hover active:bg-ds-accent/80",
  secondary: "border border-ds-line text-ds-text-1 bg-ds-panel hover:bg-ds-hover-strong active:bg-ds-active",
  danger: "bg-ds-down text-ds-down-contrast hover:bg-ds-down-hover active:bg-ds-down/80",
  ghost: "text-ds-text-2 hover:text-ds-text-1 hover:bg-ds-hover active:bg-ds-active",
};

const sizes: Record<Size, string> = {
  sm: "px-2 py-1 text-xs rounded-lg",
  md: "h-8 px-3.5 text-[13px] rounded-lg",
};

export type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: Variant;
  size?: Size;
  /** Loading disables the button and shows an inline spinner; the
   * label stays in the layout so the width does not jump. */
  loading?: boolean;
};

export default function Button({
  variant = "primary",
  size = "md",
  loading = false,
  disabled,
  children,
  className = "",
  ...rest
}: ButtonProps) {
  return (
    <button
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      className={`${base} ${variants[variant]} ${sizes[size]} ${className}`}
      {...rest}
    >
      <span className={`inline-flex items-center ${loading ? "invisible" : ""}`}>{children}</span>
      {loading && (
        <span className="absolute inset-0 flex items-center justify-center"><Spinner /></span>
      )}
    </button>
  );
}
