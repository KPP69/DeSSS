import React, { useState } from "react";
import { Button } from "../components/ui";
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
 * the modals own their flows. No manual order form (see the hook).
 * F4.1: two segments — 持仓 and 记录 (open orders + records +
 * fills); refresh sits at the right of the segment row. F4.4: the
 * toolbar is gone (it held only the status text); the segments and
 * refresh live in the page itself. */
type Segment = "positions" | "records";

const SEGMENTS: { key: Segment; label: string }[] = [
  { key: "positions", label: "持仓" },
  { key: "records", label: "记录" },
];

export default function TradingPage() {
  const data = useTradingData();
  const [segment, setSegment] = useState<Segment>("positions");
  const [closeTarget, setCloseTarget] = useState<CloseTarget | null>(null);
  const [protectTarget, setProtectTarget] = useState<PositionRow | null>(null);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);

  const askClose = (p: PositionRow) => setCloseTarget(data.askClose(p));

  return (
    <div className="flex h-full flex-col">
      <div className="mb-3 flex shrink-0 items-center justify-between">
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
        <Button variant="secondary" size="sm" onClick={() => data.refresh()}>
          刷新
        </Button>
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
            strategyFor={data.strategyFor}
            expandedKey={expandedKey}
            onToggleExpand={(key) => setExpandedKey((cur) => (cur === key ? null : key))}
            onClose={askClose}
            onProtect={setProtectTarget}
            cancelingId={data.cancelingId}
            onCancelOrder={(o) => void data.cancel(o)}
          />
        )}
        {segment === "records" && (
          <>
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
            <div className="mt-6">
              <TradesSection
                fills={data.fills}
                records={data.records}
                loading={data.listsLoading}
                error={data.listsError}
                onRetry={data.refresh}
                labelOf={data.labelOf}
                venueName={data.venueName}
              />
            </div>
          </>
        )}
      </div>
      <CloseConfirm target={closeTarget} onClose={() => setCloseTarget(null)} onSuccess={data.refresh} />
      <ProtectionModal target={protectTarget} openOrders={data.openOrders} onClose={() => setProtectTarget(null)} onSaved={data.refresh} />
    </div>
  );
}
