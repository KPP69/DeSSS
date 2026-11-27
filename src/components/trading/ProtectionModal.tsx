import React, { useEffect, useState } from "react";
import { Button, Input, Modal } from "../ui";
import { displaySymbol } from "../../lib/symbol";
import { api } from "../../lib/tauri";
import type { OrderInfo } from "../../lib/types";
import { protectionFor, validateProtect, type PositionRow } from "../../hooks/useTradingData";
import { toast } from "../../store/useToastStore";

/**
 * Protection modal (PR-3.1.1): hang TP/SL trigger orders on a
 * position. Either price may be left empty; both empty clears all
 * protection. Saving first cancels the position's existing manual
 * trigger orders, then rebuilds from the inputs — the explanation
 * line says so. Prices are validated per field against the mark
 * (long: TP above, SL below; short: the reverse); backend failures
 * toast verbatim and keep the modal open.
 */
export default function ProtectionModal({
  target,
  openOrders,
  onClose,
  onSaved,
}: {
  target: PositionRow | null;
  openOrders: OrderInfo[];
  onClose: () => void;
  onSaved: () => void;
}) {
  const [tpInput, setTpInput] = useState("");
  const [slInput, setSlInput] = useState("");
  const [tpError, setTpError] = useState<string | undefined>(undefined);
  const [slError, setSlError] = useState<string | undefined>(undefined);
  const [note, setNote] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // Prefill from the position's current protection when a new
  // target opens; later order-list refreshes never reset the inputs.
  useEffect(() => {
    if (!target) return;
    const prot = protectionFor(target.accountId, target.symbol, openOrders, target);
    setTpInput(prot.tp != null ? String(prot.tp) : "");
    setSlInput(prot.sl != null ? String(prot.sl) : "");
    setTpError(undefined);
    setSlError(undefined);
    setNote(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target]);

  const save = async () => {
    if (!target) return;
    const errs = validateProtect(target.side, target.markPrice, tpInput, slInput);
    setTpError(errs.tp);
    setSlError(errs.sl);
    if (errs.tp || errs.sl) return;
    const tpNum = tpInput.trim() === "" ? null : Number(tpInput);
    const slNum = slInput.trim() === "" ? null : Number(slInput);
    setNote(null);
    setBusy(true);
    const { accountId, symbol, side, qty } = target;
    const closeSide = side === "long" ? "sell" : "buy";
    // How many old protection orders were already cancelled when a
    // later step fails — the catch reports it, they never come back.
    let cancelled = 0;
    try {
      // "改" = 撤旧 + 建新：先撤该仓位所有手动触发单，再按输入重建。
      const manual = openOrders.filter(
        (o) =>
          o.accountId === accountId &&
          o.symbol === symbol &&
          o.origin === "manual" &&
          o.triggerPx != null,
      );
      for (const o of manual) {
        await api.cancelOrder(accountId, o.orderId || o.clientOrderId, symbol);
        cancelled++;
      }
      const placed: string[] = [];
      for (const [px, tpsl] of [
        [tpNum, "tp"],
        [slNum, "sl"],
      ] as const) {
        if (px == null) continue;
        const info = await api.placeOrder({
          accountId,
          symbol,
          side: closeSide,
          orderType: "trigger",
          quantity: Math.abs(qty),
          price: px,
          reduceOnly: true,
          triggerPx: px,
          tpsl,
          isMarket: true,
        });
        placed.push(
          `${tpsl === "tp" ? "止盈" : "止损"} ${px}` +
            `${info.status === "unresolved" ? "（未确认）" : ""}`,
        );
      }
      toast.success(
        `保护已更新：${manual.length > 0 ? `撤掉 ${manual.length} 笔旧保护单；` : ""}` +
          `${placed.length > 0 ? `已挂 ${placed.join("、")}` : "已清除全部保护"}`,
      );
      onSaved();
      onClose();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
      if (cancelled > 0) {
        setNote(`已撤 ${cancelled} 笔旧单，不会自动恢复，可重试`);
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      open={target != null}
      title={
        target
          ? `止盈止损 · ${displaySymbol(target.symbol)}（${target.side === "long" ? "多" : "空"} ${Math.abs(target.qty)}）`
          : "止盈止损"
      }
      onClose={() => {
        if (!busy) onClose();
      }}
      widthClass="w-[420px]"
      footer={
        <>
          <Button variant="secondary" disabled={busy} onClick={onClose}>
            取消
          </Button>
          <Button variant="primary" loading={busy} onClick={() => void save()}>
            保存
          </Button>
        </>
      }
    >
      {target && (
        <div className="space-y-3 pb-2">
          <div className="text-ds-text-2 text-[13px]">
            现价 {target.markPrice}。
            {target.side === "long"
              ? "多仓：止盈价高于现价，止损价低于现价。"
              : "空仓：止盈价低于现价，止损价高于现价。"}
            只填一个也可以；两个都留空 = 清除全部保护。保存时先撤掉该仓位全部手动触发单，再按输入重建。
          </div>
          <label className="block">
            <span className="text-ds-text-2 text-[13px]">止盈价</span>
            <Input
              type="number"
              inputMode="decimal"
              placeholder="留空 = 不挂止盈"
              value={tpInput}
              disabled={busy}
              error={tpError}
              onChange={(e) => setTpInput(e.target.value)}
            />
          </label>
          <label className="block">
            <span className="text-ds-text-2 text-[13px]">止损价</span>
            <Input
              type="number"
              inputMode="decimal"
              placeholder="留空 = 不挂止损"
              value={slInput}
              disabled={busy}
              error={slError}
              onChange={(e) => setSlInput(e.target.value)}
            />
          </label>
          {note && <div className="text-[13px] text-ds-down">{note}</div>}
        </div>
      )}
    </Modal>
  );
}
