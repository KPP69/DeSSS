import React from "react";
import PageState from "../PageState";

/**
 * Centered data table with a horizontal-scroll wrapper. Headers and
 * cells are center-aligned by default; pass `aligns` to right-align
 * numeric columns (amounts, counts) — header and cells follow the
 * same alignment. Pass body rows as `rows`, or children <tbody> for
 * full control (pinned rows). Header: text-xs text-2, line-strong
 * bottom border. Body rows are keyboard-reachable (tabIndex 0,
 * focus-visible background — a ring does not render reliably on <tr>).
 * Empty state is PageState. No sorting this round (F4). Numbers use
 * tabular numerals (`.num`); alignment stays centered per the
 * existing API unless `aligns` says otherwise.
 */
export type ColumnAlign = "left" | "center" | "right";

const alignCls: Record<ColumnAlign, string> = {
  left: "text-left",
  center: "text-center",
  right: "text-right",
};

export default function DataTable({
  headers,
  rows,
  emptyText,
  minWidth = 720,
  children,
  compact = false,
  colWidths,
  aligns,
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
  /** Per-column alignment, one per header; defaults to center. */
  aligns?: ColumnAlign[];
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
          <tr className="border-b border-ds-line-strong">
            {headers.map((h, i) => (
              <th
                key={h}
                className={`${alignCls[aligns?.[i] ?? "center"]} font-medium text-ds-text-2 whitespace-nowrap ${
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
            <tr
              key={i}
              tabIndex={0}
              className="border-t border-ds-line hover:bg-ds-hover active:bg-ds-active focus-visible:bg-ds-hover focus-visible:outline-none transition-colors"
            >
              {cells.map((cell, j) => (
                <td
                  key={j}
                  className={`num ${alignCls[aligns?.[j] ?? "center"]} text-ds-text-1 ${compact ? "px-3 py-1.5 text-[13px]" : "px-4 py-3"}`}
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
          {isEmpty && (
            <tr className="border-t border-ds-line">
              <td colSpan={headers.length} className="px-4 py-4">
                <PageState empty>{emptyText ?? "暂无数据"}</PageState>
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

/** Shared centered cell classes for hand-built rows inside DataTable. */
export const tdCls = "num text-center px-4 py-3 text-ds-text-1";
/** Right-aligned variant, for amount columns in hand-built rows. */
export const tdClsRight = "num text-right px-4 py-3 text-ds-text-1";
export const trCls =
  "border-t border-ds-line hover:bg-ds-hover active:bg-ds-active focus-visible:bg-ds-hover focus-visible:outline-none transition-colors";
/** Compact variant: tighter padding and smaller text for dense lists. */
export const tdClsCompact = "num text-center px-3 py-1.5 text-[13px] text-ds-text-1";
/** Compact right-aligned variant, for amount columns in dense lists. */
export const tdClsCompactRight = "num text-right px-3 py-1.5 text-[13px] text-ds-text-1";
