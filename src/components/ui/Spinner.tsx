import React from "react";

/** Indeterminate spinner (Button loading, PageState loading); inherits color via currentColor. */
export default function Spinner({ size = "sm" }: { size?: "sm" | "md" }) {
  return (
    <svg className={`${size === "sm" ? "h-3.5 w-3.5" : "h-5 w-5"} animate-spin`} viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" opacity="0.25" />
      <path d="M22 12a10 10 0 0 0-10-10" stroke="currentColor" strokeWidth="4" strokeLinecap="round" />
    </svg>
  );
}
