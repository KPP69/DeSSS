import React, { useState } from "react";
import { ConfirmDialog } from "../ui";
import { displaySymbol } from "../../lib/symbol";
import { api } from "../../lib/tauri";
import { bookFresh, STATUS_LABEL, type CloseTarget } from "../../hooks/useTradingData";
import { useLiveStore } from "../../store/useLiveStore";
import { toast } from "../../store/useToastStore";

/**
 * Close confirmation: symbol / direction / quantity / price. The
 * dialog may have sat open while the book went stale, so confirming
 * re-checks freshness first and refuses a stale price. The close
 * goes out as an IOC reduce-only order; success toasts and refreshes
 * the lists (positions + that leg's protection orders).
 */
export default function CloseConfirm({
  target,
  onClose,
  onSuccess,
}: {
  target: CloseTarget | null;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [closing, setClosing] = useState(false);

  const confirm = async () => {
    if (!target || closing) return;
    // The dialog may have sat open while the book went stale:
    // re-check freshness, never confirm a stale price.
    const freshBook = useLiveStore.getState().books[
      `${target.adapterId}:${target.symbol}`
    ];
    if (!bookFresh(freshBook)) {
      toast.error("盘口已过期，请关闭后重试");
      onClose();
      return;
    }
    setClosing(true);
    try {
      const info = await api.placeOrder({
        accountId: target.accountId,
        symbol: target.symbol,
        side: target.side === "long" ? "sell" : "buy",
        orderType: "ioc",
        quantity: target.qty,
        price: target.price,
        reduceOnly: true,
      });
      toast.success(
        `平仓单已发：${displaySymbol(info.symbol)} ${info.side === "buy" ? "买入" : "卖出"} ${info.quantity}` +
          `${info.price != null ? ` @ ${info.price}` : ""}（${STATUS_LABEL[info.status] ?? info.status}）` +
          "。仓位归零后，挂着的保护单会随之失效，请在挂单表里清除。",
      );
      onClose();
      onSuccess();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
      onClose();
    } finally {
      setClosing(false);
    }
  };

  return (
    <ConfirmDialog
      open={target != null}
      title="确认平仓"
      confirmText={closing ? "下单中…" : "确认平仓"}
      message={
        target && (
          <div className="space-y-1">
            <div>
              {displaySymbol(target.symbol)} ·{" "}
              {target.side === "long" ? "平多（卖出）" : "平空（买入）"}
            </div>
            <div className="num">数量 {target.qty}</div>
            <div className="num">价格 {target.price}（IOC 限价，仅减仓）</div>
            <div className="text-ds-warn">
              平仓后该仓位挂着的保护单会随之失效，请在挂单表里清除。
            </div>
          </div>
        )
      }
      onConfirm={() => void confirm()}
      onCancel={() => {
        if (!closing) onClose();
      }}
    />
  );
}
