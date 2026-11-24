import React from "react";

type Variant = "primary" | "secondary" | "danger" | "ghost";
type Size = "sm" | "md";

const base =
  "inline-flex items-center justify-center rounded font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary/50";

const variants: Record<Variant, string> = {
  primary: "bg-primary text-white hover:bg-primary/85 active:bg-primary/75",
  secondary: "border border-line text-t1 bg-panel hover:bg-overlay active:bg-overlay/70",
  danger: "bg-loss text-white hover:bg-loss/85 active:bg-loss/75",
  ghost: "text-t2 hover:text-t1 hover:bg-overlay",
};

const sizes: Record<Size, string> = {
  sm: "px-3 py-1 text-xs",
  md: "px-4 py-2 text-[13px]",
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
