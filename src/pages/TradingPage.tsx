import React, { useLayoutEffect, useState } from "react";
import { Button } from "../components/ui";
import { useShellActions } from "../store/useShellActions";
import {
  useTradingData,
  type CloseTarget,
  type PositionRow,
} from "../hooks/useTradingData";
import CloseConfirm from "../components/trading/CloseConfirm";
import OrdersSection from "../components/trading/OrdersSection";
import PositionsSection from "../components/trading/PositionsSection";
import ProtectionModal from "../components/trading/ProtectionModal";
import TradesSection from "../components/trading/TradesSection";

/** Trading page (F4): orchestration only. Data lives in
 * `useTradingData` at this level, so switching segments never
 * refetches and never loses data; sections own their tables and
 * the modals own their flows. No manual order form (see the hook). */
type Segment = "positions" | "orders" | "trades";

const SEGMENTS: { key: Segment; label: string }[] = [
  { key: "positions", label: "持仓" },
  { key: "orders", label: "挂单" },
  { key: "trades", label: "成交" },
];

export default function TradingPage() {
  const data = useTradingData();
  const [segment, setSegment] = useState<Segment>("positions");
  const [closeTarget, setCloseTarget] = useState<CloseTarget | null>(null);
  const [protectTarget, setProtectTarget] = useState<PositionRow | null>(null);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);

  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useLayoutEffect(() => {
    register("trading", {
      actions: <Button onClick={() => data.refresh()}>刷新</Button>,
      status: `持仓 ${data.positions.length} · 挂单 ${data.openOrders.length}`,
    });
    return () => unregister("trading");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [register, unregister, data.positions.length, data.openOrders.length]);

  const askClose = (p: PositionRow) => setCloseTarget(data.askClose(p));

  return (
    <div className="flex h-full flex-col">
      <div className="mb-3 flex shrink-0">
        <div className="flex rounded-lg border border-ds-line bg-ds-panel p-0.5">
          {SEGMENTS.map((s) => (
            <button
              key={s.key}
              onClick={() => setSegment(s.key)}
              className={`px-3 py-1 rounded-md text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ds-focus-ring/50 ${
                segment === s.key
                  ? "bg-ds-accent text-ds-accent-contrast"
                  : "text-ds-text-2 hover:text-ds-text-1"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        {segment === "positions" && (
          <PositionsSection
            positions={data.positions}
            openOrders={data.openOrders}
            books={data.books}
            tradable={data.tradable}
            degraded={data.degraded}
            labelOf={data.labelOf}
            venueName={data.venueName}
            expandedKey={expandedKey}
            onToggleExpand={(key) => setExpandedKey((cur) => (cur === key ? null : key))}
            onClose={askClose}
            onProtect={setProtectTarget}
            cancelingId={data.cancelingId}
            onCancelOrder={(o) => void data.cancel(o)}
          />
        )}
        {segment === "orders" && (
          <OrdersSection
            openOrders={data.openOrders}
            records={data.records}
            tradeAccounts={data.tradeAccounts}
            loading={data.listsLoading}
            error={data.listsError}
            onRetry={data.refresh}
            labelOf={data.labelOf}
            venueName={data.venueName}
            cancelingId={data.cancelingId}
            onCancel={data.cancel}
            cancelResult={data.cancelResult}
          />
        )}
        {segment === "trades" && (
          <TradesSection
            fills={data.fills}
            records={data.records}
            loading={data.listsLoading}
            error={data.listsError}
            onRetry={data.refresh}
            labelOf={data.labelOf}
            venueName={data.venueName}
          />
        )}
      </div>
      <CloseConfirm target={closeTarget} onClose={() => setCloseTarget(null)} onSuccess={data.refresh} />
      <ProtectionModal target={protectTarget} openOrders={data.openOrders} onClose={() => setProtectTarget(null)} onSaved={data.refresh} />
    </div>
  );
}
