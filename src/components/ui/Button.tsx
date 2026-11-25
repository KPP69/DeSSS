import React from "react";

type Variant = "primary" | "secondary" | "danger" | "ghost";
type Size = "sm" | "md";

// Controls are 32px tall (md) with 8px radius; table actions use sm (28px).
const base =
  "inline-flex items-center justify-center font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-ds-ring/50";

const variants: Record<Variant, string> = {
  primary: "bg-ds-accent text-ds-accent-contrast hover:bg-ds-accent-hover active:bg-ds-accent/80",
  secondary: "border border-ds-line text-ds-text-1 bg-ds-panel hover:bg-ds-panel-2 active:bg-ds-panel-2/70",
  danger: "bg-ds-down text-ds-down-contrast hover:bg-ds-down-hover active:bg-ds-down/80",
  ghost: "text-ds-text-2 hover:text-ds-text-1 hover:bg-ds-panel-2",
};

const sizes: Record<Size, string> = {
  sm: "h-7 px-2.5 text-xs rounded-md",
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
