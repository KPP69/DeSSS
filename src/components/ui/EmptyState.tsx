import React from "react";

/** Dashed empty-state panel used by shell pages and empty lists. */
export default function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <div className="border border-dashed border-line rounded-lg p-8 text-t3 text-center">
      {children}
    </div>
  );
}
