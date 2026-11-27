import React from "react";
import { Badge, Button, DataTable, EmptyState } from "../ui";
import { displaySymbol } from "../../lib/symbol";
import type { OrderBook, OrderInfo } from "../../lib/types";
import {
  bookFresh,
  protectionFor,
  statusTone,
  tpslLabel,
  STATUS_LABEL,
  type PositionRow,
} from "../../hooks/useTradingData";

/**
 * Positions segment: the live position table plus the expanded
 * protection-orders table for one position. Rows are keyboard
 * reachable (tabIndex 0, focus-visible background) — click a row or
 * press Enter on it to expand its protection orders; the 平仓 /
 * 保护 buttons stop the row click so they never expand by accident.
 * Amounts are right-aligned, header and cells alike. Above 200
 * positions the table virtualizes (DataTable).
 */
export default function PositionsSection({
  positions,
  openOrders,
  books,
  tradable,
  degraded,
  labelOf,
  venueName,
  expandedKey,
  onToggleExpand,
  onClose,
  onProtect,
  cancelingId,
  onCancelOrder,
}: {
  positions: PositionRow[];
  openOrders: OrderInfo[];
  books: Record<string, OrderBook>;
  tradable: Set<string>;
  /** True while any account's WS is down: its rows are REST fallback. */
  degraded: boolean;
  labelOf: (accountId: string) => string;
  venueName: (id: string) => string;
  expandedKey: string | null;
  onToggleExpand: (key: string) => void;
  onClose: (p: PositionRow) => void;
  onProtect: (p: PositionRow) => void;
  cancelingId: string | null;
  onCancelOrder: (o: OrderInfo) => void;
}) {
  const positionKey = (p: PositionRow) => `${p.accountId}:${p.symbol}`;
  const canAct = (p: PositionRow) =>
    tradable.has(p.adapterId) &&
    Math.abs(p.qty) > 0 &&
    bookFresh(books[`${p.adapterId}:${p.symbol}`]);

  const expanded = expandedKey != null ? positions.find((x) => positionKey(x) === expandedKey) : undefined;

  return (
    <div>
      {degraded && (
        <div className="text-xs text-ds-text-3 mb-2">数据降级：REST 兜底</div>
      )}
      <DataTable
        headers={["账户 · 交易所", "交易对", "方向", "数量", "开仓价", "标记价", "未实现盈亏", "当前保护", "操作"]}
        minWidth={1020}
        colWidths={["16%", "10%", "7%", "10%", "11%", "11%", "12%", "14%", "9%"]}
        aligns={["left", "center", "center", "right", "right", "right", "right", "right", "center"]}
        emptyText="暂无真实持仓 — 连接账户后，交易所同步到的当前持仓会显示在这里。"
        onRowClick={(i) => onToggleExpand(positionKey(positions[i]))}
        onRowKeyDown={(i, e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            onToggleExpand(positionKey(positions[i]));
          }
        }}
        rows={positions.map((p) => {
          const prot = protectionFor(p.accountId, p.symbol, openOrders, p);
          const act = canAct(p);
          const key = positionKey(p);
          const parts: string[] = [];
          if (prot.tp != null) parts.push(`止盈 ${prot.tp}${prot.tpCount > 1 ? ` ×${prot.tpCount}` : ""}`);
          if (prot.sl != null) parts.push(`止损 ${prot.sl}${prot.slCount > 1 ? ` ×${prot.slCount}` : ""}`);
          const protectionText = parts.length > 0 ? parts.join(" / ") : "无";
          return [
            <span key="a">
              {labelOf(p.accountId)} · {venueName(p.adapterId)}
            </span>,
            <span key="s" className="num">{displaySymbol(p.symbol)}</span>,
            <span key="d">
              <Badge tone={p.side === "long" ? "up" : "down"}>
                {p.side === "long" ? "多" : "空"}
              </Badge>
            </span>,
            <span key="q" className="num">{p.qty}</span>,
            <span key="ep" className="num">{p.entryPrice.toFixed(2)}</span>,
            <span key="mp" className="num">{p.markPrice.toFixed(2)}</span>,
            <span
              key="p"
              className={`num ${p.unrealizedPnl > 0 ? "text-ds-up" : p.unrealizedPnl < 0 ? "text-ds-down" : ""}`}
            >
              {p.unrealizedPnl > 0 ? "+" : ""}
              {p.unrealizedPnl.toFixed(2)} {p.currency}
            </span>,
            <span key="prot" className="num text-ds-text-2">{protectionText}</span>,
            <span key="ops" className="flex gap-1 justify-center" onClick={(e) => e.stopPropagation()}>
              <Button variant="ghost" size="sm" disabled={!act} onClick={() => onClose(p)}>
                平仓
              </Button>
              <Button variant="ghost" size="sm" disabled={!act} onClick={() => onProtect(p)}>
                保护
              </Button>
              <Button
                variant="ghost"
                size="sm"
                disabled={!act}
                onClick={() => onToggleExpand(key)}
              >
                {expandedKey === key ? "收起" : "展开"}
              </Button>
            </span>,
          ];
        })}
      />
      <div className="mt-2 text-[12px] text-ds-text-2">
        平仓按盘口反向 2 bps 的 IOC 限价单发出（仅减仓）；保护挂止盈 / 止损触发单（市价触发、仅减仓）。
        盘口过期、持仓为 0 或交易所不可交易时，三个按钮不可用。
      </div>
      {expanded && (
        <div className="mt-3">
          <div className="font-semibold text-ds-text-1 mb-2">
            保护单 · {labelOf(expanded.accountId)} {displaySymbol(expanded.symbol)}
            （{expanded.side === "long" ? "多" : "空"} {Math.abs(expanded.qty)}）
          </div>
          {(() => {
            const prot = protectionFor(expanded.accountId, expanded.symbol, openOrders, expanded);
            if (prot.orders.length === 0) {
              return <EmptyState>当前无保护单 — 点「保护」挂止盈 / 止损触发单。</EmptyState>;
            }
            return (
              <DataTable
                headers={["订单号", "类型", "触发价", "数量", "状态", "操作"]}
                minWidth={640}
                aligns={["center", "center", "right", "right", "center", "center"]}
                rows={prot.orders.map((o) => [
                  <span key="o" className="num">{o.orderId || o.clientOrderId}</span>,
                  <span key="t">
                    <Badge tone={tpslLabel(o, expanded) === "tp" ? "up" : "down"}>
                      {tpslLabel(o, expanded) === "tp" ? "止盈" : "止损"}
                    </Badge>
                  </span>,
                  <span key="px" className="num">{o.triggerPx ?? "—"}</span>,
                  <span key="q" className="num">{o.quantity}</span>,
                  <span key="st">
                    <Badge tone={statusTone(o.status)}>
                      {STATUS_LABEL[o.status] ?? o.status}
                    </Badge>
                  </span>,
                  <span key="c">
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={cancelingId === (o.orderId || o.clientOrderId)}
                      onClick={() => onCancelOrder(o)}
                    >
                      {cancelingId === (o.orderId || o.clientOrderId) ? "撤单中…" : "撤单"}
                    </Button>
                  </span>,
                ])}
              />
            );
          })()}
        </div>
      )}
    </div>
  );
}
