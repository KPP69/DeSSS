import React from "react";

/** Page title on the left, actions on the right — one consistent header. */
export default function PageHeader({
  title,
  children,
}: {
  title: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between mb-5 min-h-[40px]">
      <h1 className="text-xl font-semibold text-t1">{title}</h1>
      <div className="flex items-center gap-2">{children}</div>
    </div>
  );
}
