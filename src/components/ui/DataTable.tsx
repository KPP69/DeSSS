import React, { useEffect, useLayoutEffect, useRef, useState } from "react";
import PageState from "../PageState";
import { computeWindow } from "../../lib/virtualWindow";

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
 *
 * Virtual scrolling (F4): when the table holds more than 200 row
 * groups, only the rows around the viewport (plus a 5-row overscan)
 * stay in the DOM; spacer rows above and below reserve the space of
 * the hidden rows. Row heights are measured after render and fall
 * back to an estimate until measured, so an expanded detail row
 * (Data page) gets its real height. The header sticks to the top of
 * the scroll viewport while virtualized.
 */
export type ColumnAlign = "left" | "center" | "right";

const alignCls: Record<ColumnAlign, string> = {
  left: "text-left",
  center: "text-center",
  right: "text-right",
};

/** Row groups above this count switch to virtual scrolling. */
const VIRTUAL_THRESHOLD = 200;
const OVERSCAN = 5;
const VIEWPORT_MAX_H = 560;

/** Tag the first <tr> of one row group with its absolute index so
 * the measurement pass can attribute DOM heights to groups. A
 * Fragment group (Data page: row + optional detail row) is tagged
 * on its first child; a plain <tr> group is tagged directly. */
function tagGroup(node: React.ReactNode, i: number): React.ReactNode {
  if (!React.isValidElement(node)) return node;
  const el = node as React.ReactElement<Record<string, unknown>>;
  if (el.type === React.Fragment) {
    const kids = React.Children.toArray(el.props.children as React.ReactNode);
    if (kids.length === 0) return node;
    const first = kids[0];
    if (!React.isValidElement(first)) return node;
    const tagged = React.cloneElement(first as React.ReactElement, {
      "data-vgroup": i,
    } as Record<string, unknown>);
    return React.cloneElement(el, {}, [tagged, ...kids.slice(1)]);
  }
  return React.cloneElement(el, { "data-vgroup": i } as Record<string, unknown>);
}

export default function DataTable({
  headers,
  rows,
  emptyText,
  minWidth = 720,
  children,
  compact = false,
  dense = false,
  colWidths,
  aligns,
  onRowClick,
  onRowKeyDown,
}: {
  headers: string[];
  rows?: React.ReactNode[][];
  emptyText?: string;
  minWidth?: number;
  children?: React.ReactNode;
  /** Compact rows: tighter padding and smaller text (e.g. account list). */
  compact?: boolean;
  /** Dense: 16px column spacing (px-2 each side) instead of compact's
   * 24px — the Data page (F4.3). Only affects header + rows-path cells;
   * hand-built children rows use their own cell classes. */
  dense?: boolean;
  /** Fixed column widths (px or CSS length), one per header. */
  colWidths?: (number | string)[];
  /** Per-column alignment, one per header; defaults to center. */
  aligns?: ColumnAlign[];
  /** Rows-path only: row click (index is the absolute row index). */
  onRowClick?: (index: number) => void;
  /** Rows-path only: row keydown, e.g. Enter to expand. */
  onRowKeyDown?: (index: number, e: React.KeyboardEvent<HTMLTableRowElement>) => void;
}) {
  const isEmpty = !children && (!rows || rows.length === 0);

  // One entry per row group: a `rows` row, or one top-level child
  // (a <tr>, or a Fragment holding a row + its detail row).
  const rowElements: React.ReactNode[] = rows
    ? rows.map((cells, i) => (
        <tr
          key={i}
          tabIndex={0}
          onClick={onRowClick ? () => onRowClick(i) : undefined}
          onKeyDown={onRowKeyDown ? (e) => onRowKeyDown(i, e) : undefined}
          className="border-t border-ds-line hover:bg-ds-hover active:bg-ds-active focus-visible:bg-ds-hover focus-visible:outline-none transition-colors"
        >
          {cells.map((cell, j) => (
            <td
              key={j}
              className={`num ${alignCls[aligns?.[j] ?? "center"]} text-ds-text-1 ${compact ? (dense ? "px-2 py-1.5 text-[13px]" : "px-3 py-1.5 text-[13px]") : "px-4 py-3"}`}
            >
              {cell}
            </td>
          ))}
        </tr>
      ))
    : React.Children.toArray(children);

  const virtualized = rowElements.length > VIRTUAL_THRESHOLD;
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const tbodyRef = useRef<HTMLTableSectionElement | null>(null);
  const heightsRef = useRef(new Map<number, number>());
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportH, setViewportH] = useState(VIEWPORT_MAX_H);
  const [, setMeasureTick] = useState(0);
  // Estimate until measured: compact rows are one line of 13px text
  // with py-1.5 (~34px); normal rows carry buttons or 14px text
  // with py-3 (~46px).
  const estimate = compact ? 34 : 46;
  const heightAt = (i: number) => heightsRef.current.get(i) ?? estimate;

  // A new dataset (different row count) invalidates measured
  // heights and scrolls back to the top.
  const count = rowElements.length;
  useEffect(() => {
    heightsRef.current.clear();
    if (scrollRef.current) scrollRef.current.scrollTop = 0;
    setScrollTop(0);
    setMeasureTick((t) => t + 1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [count]);

  const win = virtualized
    ? computeWindow(count, heightAt, scrollTop, viewportH, OVERSCAN)
    : null;
  const visibleGroups = win ? rowElements.slice(win.start, win.end) : rowElements;
  const renderedGroups = win
    ? visibleGroups.map((g, k) => tagGroup(g, win.start + k))
    : visibleGroups;

  // Measure the rendered groups' real heights from the DOM: walk
  // the tbody's <tr>s in order, attributing each run of rows to the
  // group whose tagged <tr> starts it. A changed height re-renders
  // so the window and spacers use real numbers.
  useLayoutEffect(() => {
    if (!virtualized || !tbodyRef.current) return;
    const next = new Map<number, number>();
    let current: number | null = null;
    let acc = 0;
    const flush = () => {
      if (current != null) next.set(current, acc);
    };
    tbodyRef.current.querySelectorAll("tr").forEach((tr) => {
      const g = tr.getAttribute("data-vgroup");
      if (g != null) {
        flush();
        current = Number(g);
        acc = tr.offsetHeight;
        return;
      }
      if (tr.hasAttribute("data-vspacer")) return;
      if (current != null) acc += tr.offsetHeight;
    });
    flush();
    let changed = next.size !== heightsRef.current.size;
    if (!changed) {
      next.forEach((h, i) => {
        if (heightsRef.current.get(i) !== h) changed = true;
      });
    }
    if (changed) {
      heightsRef.current = next;
      setMeasureTick((t) => t + 1);
    }
  });

  // Scroll updates are rAF-throttled so a fast wheel drag renders
  // at most one window per frame.
  const rafRef = useRef(0);
  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    cancelAnimationFrame(rafRef.current);
    rafRef.current = requestAnimationFrame(() => {
      setScrollTop(el.scrollTop);
      setViewportH(el.clientHeight);
    });
  };
  useEffect(() => () => cancelAnimationFrame(rafRef.current), []);

  return (
    <div
      ref={virtualized ? scrollRef : undefined}
      onScroll={virtualized ? onScroll : undefined}
      className={`bg-ds-panel border border-ds-line rounded-[10px] shadow-card overflow-x-auto ${
        virtualized ? "max-h-[560px] overflow-y-auto" : ""
      }`}
    >
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
                  compact ? (dense ? "px-2 py-1.5 text-xs" : "px-3 py-1.5 text-xs") : "px-4 py-3 text-xs"
                } ${virtualized ? "sticky top-0 bg-ds-panel z-10" : ""}`}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody ref={virtualized ? tbodyRef : undefined}>
          {win && (
            <tr aria-hidden="true" data-vspacer>
              <td colSpan={headers.length} style={{ height: win.offsetBefore, padding: 0, border: 0 }} />
            </tr>
          )}
          {renderedGroups}
          {win && (
            <tr aria-hidden="true" data-vspacer>
              <td colSpan={headers.length} style={{ height: win.offsetAfter, padding: 0, border: 0 }} />
            </tr>
          )}
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
