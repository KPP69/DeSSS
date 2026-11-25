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
  compact = false,
  colWidths,
}: {
  headers: string[];
  rows?: React.ReactNode[][];
  emptyText?: string;
  minWidth?: number;
  children?: React.ReactNode;
  /** Compact rows: tighter padding and smaller text (e.g. account list). */
  compact?: boolean;
  /** Fixed column widths (px or CSS length), one per header. */
  colWidths?: (number | string)[];
}) {
  const isEmpty = !children && (!rows || rows.length === 0);
  return (
    <div className="bg-ds-panel border border-ds-line rounded-[10px] shadow-card overflow-x-auto">
      <table className={`w-full ${colWidths ? "table-fixed" : ""}`} style={{ minWidth }}>
        {colWidths && (
          <colgroup>
            {colWidths.map((w, i) => (
              <col key={i} style={{ width: w }} />
            ))}
          </colgroup>
        )}
        <thead>
          <tr>
            {headers.map((h) => (
              <th
                key={h}
                className={`text-center font-medium text-ds-text-2 whitespace-nowrap ${
                  compact ? "px-3 py-1.5 text-xs" : "px-4 py-3 text-xs"
                }`}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {children}
          {rows?.map((cells, i) => (
            <tr key={i} className="border-t border-ds-line hover:bg-ds-panel-2/50 transition-colors">
              {cells.map((cell, j) => (
                <td
                  key={j}
                  className={`text-center text-ds-text-1 ${compact ? "px-3 py-1.5 text-[13px]" : "px-4 py-3"}`}
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
          {isEmpty && (
            <tr className="border-t border-ds-line">
              <td
                colSpan={headers.length}
                className="text-center px-4 py-8 text-ds-text-3"
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
export const tdCls = "text-center px-4 py-3 text-ds-text-1";
export const trCls = "border-t border-ds-line hover:bg-ds-panel-2/50 transition-colors";
/** Compact variant: tighter padding and smaller text for dense lists. */
export const tdClsCompact = "text-center px-3 py-1.5 text-[13px] text-ds-text-1";
