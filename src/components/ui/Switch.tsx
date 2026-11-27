import React from "react";

/**
 * Toggle switch (F3): a button with role="switch", so Enter/Space
 * toggle it natively and screen readers announce the state. On =
 * accent track, off = disabled-background track with a control-line
 * border; the knob is always panel-colored. States: default / hover /
 * active / focus-visible / disabled (disabled also covers "busy" —
 * the caller disables the switch while its async toggle runs).
 * Sizes: sm (list rows) / lg (the Risk page kill switch).
 */
export default function Switch({
  checked,
  onChange,
  disabled = false,
  size = "sm",
  label,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  disabled?: boolean;
  size?: "sm" | "lg";
  /** Accessible name, e.g. the strategy id or "风控总开关". */
  label: string;
}) {
  const track =
    size === "sm" ? "h-5 w-9" : "h-7 w-12";
  const knob =
    size === "sm"
      ? `h-4 w-4 ${checked ? "translate-x-4" : "translate-x-0"}`
      : `h-6 w-6 ${checked ? "translate-x-5" : "translate-x-0"}`;
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-label={label}
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className={`${track} inline-flex shrink-0 items-center rounded-full border px-0.5 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring disabled:cursor-not-allowed disabled:opacity-50 ${
        checked
          ? "border-ds-accent bg-ds-accent hover:bg-ds-accent-hover active:bg-ds-accent-hover"
          : "border-ds-control-line bg-ds-bg-disabled hover:border-ds-text-3 active:bg-ds-bg-2"
      }`}
    >
      <span
        className={`${knob} rounded-full bg-ds-panel shadow-card transition-transform`}
      />
    </button>
  );
}
