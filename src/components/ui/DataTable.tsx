import React from "react";

/**
 * Centered data table with a horizontal-scroll wrapper.
 * Headers and cells are always center-aligned; rows highlight on hover.
 * Pass body rows as `rows` (arrays of cell nodes) or as children <tbody>
 * content via `children` for full control (e.g. a pinned first row).
 */
export default function DataTable({
  headers,
  rows,
  emptyText,
  minWidth = 720,
  children,
}: {
  headers: string[];
  rows?: React.ReactNode[][];
  emptyText?: string;
  minWidth?: number;
  children?: React.ReactNode;
}) {
  const isEmpty = !children && (!rows || rows.length === 0);
  return (
    <div className="bg-panel border border-line rounded-lg overflow-x-auto">
      <table className="w-full" style={{ minWidth }}>
        <thead className="bg-overlay/60">
          <tr>
            {headers.map((h) => (
              <th
                key={h}
                className="text-center text-xs font-medium text-t2 px-4 py-3 whitespace-nowrap"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {children}
          {rows?.map((cells, i) => (
            <tr key={i} className="border-t border-line hover:bg-overlay/50 transition-colors">
              {cells.map((cell, j) => (
                <td key={j} className="text-center px-4 py-3 text-sm text-t1">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
          {isEmpty && (
            <tr className="border-t border-line">
              <td
                colSpan={headers.length}
                className="text-center px-4 py-8 text-sm text-t3"
              >
                {emptyText ?? "暂无数据"}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

/** Shared centered cell classes for hand-built rows inside DataTable. */
export const tdCls = "text-center px-4 py-3 text-sm text-t1";
export const trCls = "border-t border-line hover:bg-overlay/50 transition-colors";
