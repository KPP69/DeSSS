import React from "react";

/** Dashed empty-state panel used by shell pages and empty lists. */
export default function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <div className="border border-dashed border-ds-line rounded-[10px] p-8 text-ds-text-3 text-center">
      {children}
    </div>
  );
}
