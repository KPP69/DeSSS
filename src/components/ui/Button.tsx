import React from "react";

type Variant = "primary" | "secondary" | "danger" | "ghost";
type Size = "sm" | "md";

// Controls are 32px tall (md) with 8px radius; table actions use sm (28px).
// Disabled uses the disabled tokens (never just opacity, so the label
// stays legible); focus uses the shared focus-ring token.
const base =
  "inline-flex items-center justify-center font-medium transition-colors disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-ds-focus-ring/50";

const variants: Record<Variant, string> = {
  primary: "bg-ds-accent text-ds-accent-contrast hover:bg-ds-accent-hover active:bg-ds-accent/80",
  secondary: "border border-ds-line text-ds-text-1 bg-ds-panel hover:bg-ds-hover-strong active:bg-ds-active",
  danger: "bg-ds-down text-ds-down-contrast hover:bg-ds-down-hover active:bg-ds-down/80",
  ghost: "text-ds-text-2 hover:text-ds-text-1 hover:bg-ds-hover active:bg-ds-active",
};

const sizes: Record<Size, string> = {
  sm: "px-2 py-1 text-xs rounded-md",
  md: "h-8 px-3.5 text-[13px] rounded-lg",
};

export type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: Variant;
  size?: Size;
};

export default function Button({
  variant = "primary",
  size = "md",
  className = "",
  ...rest
}: ButtonProps) {
  return <button className={`${base} ${variants[variant]} ${sizes[size]} ${className}`} {...rest} />;
}
