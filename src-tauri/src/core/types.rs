use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tick {
    pub adapter_id: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub volume24h: f64,
    pub ts: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    pub adapter_id: String,
    pub account_id: String,
    pub asset: String,
    pub free: f64,
    pub locked: f64,
    pub total: f64,
    pub equity_usd: f64,
    pub ts: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub adapter_id: String,
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub qty: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    pub exchange_id: String,
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: Option<f64>,
    pub client_order_id: Option<String>,
}

/// The `place_order` command's input, exactly as the Trading page sends
/// it (`src/lib/tauri.ts` `placeOrder`). The `rename_all` is load-bearing:
/// without it serde expects snake_case (`account_id`, `order_type`) while
/// the frontend sends camelCase, and the command fails with
/// "missing field `account_id`" (real-machine incident 2026-11-26).
/// Wire DTOs live here — not in `commands/` — so the serde alignment
/// tests below run in the scratch crate; see `commands/mod.rs` for the
/// two alignment rules that every new command must follow.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderInput {
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: f64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub order_id: String,
    pub client_order_id: String,
    pub exchange_id: String,
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: Option<f64>,
    pub status: OrderStatus,
    pub ts: i64,
}

/// Canonical order status (phase 5A §3, task-book §三.1). Serialized as
/// the snake_case name, so the frontend and the console see the same
/// words the task book uses.
///
/// Timeouts are NEVER treated as fills: a send that times out or whose
/// outcome is unknown lands in `Unresolved` and must be reconciled by
/// an order-status query — it never becomes `Filled` on a guess.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Built locally, not yet sent.
    Pending,
    /// Sent to the exchange, awaiting acknowledgement.
    Submitted,
    /// Acknowledged and resting on the book.
    Open,
    /// Partially filled, still resting.
    PartiallyFilled,
    /// Fully filled.
    Filled,
    /// Rejected by the exchange (or by the risk gateway before sending).
    Rejected,
    /// Cancelled by the user (or by the kill switch / dead man's switch).
    Canceled,
    /// Removed by the exchange when its time-in-force expired.
    Expired,
    /// Failed to place for a non-rejection reason (network, signing).
    Failed,
    /// Outcome unknown (timeout, dropped connection mid-send): must be
    /// reconciled by querying the exchange before any further action.
    Unresolved,
}

impl OrderStatus {
    /// Terminal states: no further transition is legal from these.
    /// `Unresolved` is deliberately NOT terminal — it exists to be
    /// resolved by reconciliation.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Rejected
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::Failed
        )
    }
}

/// Canonical order record (phase 5A §3, field names per the §3 spec).
/// `Order` above is the thin wire shape the adapter trait and
/// `AppEvent::OrderUpdate` carry; this is the record the order store,
/// the risk gateway and the Trading page reason about. The two agree by
/// construction (`Order::from_info`).
///
/// `account_id` is not in the spec's wire shape but the order path
/// routes by account, so it is kept. `remaining_quantity` is always
/// `quantity - filled_quantity`, maintained by `apply_fill`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderInfo {
    /// Exchange order id. Empty until the exchange acknowledges the order.
    pub order_id: String,
    /// Our client order id (cloid): always present, unique per order,
    /// and the key reconciliation queries by.
    pub client_order_id: String,
    pub exchange_id: String,
    pub account_id: String,
    pub symbol: String,
    /// "buy" | "sell".
    pub side: String,
    /// "limit" | "ioc" | "post_only".
    pub order_type: String,
    pub status: OrderStatus,
    pub price: Option<f64>,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub average_fill_price: Option<f64>,
    /// Last rejection / failure / reconciliation note, if any.
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl OrderInfo {
    /// Move to a new status, enforcing the legal transitions. Illegal
    /// moves (out of a terminal state, or backwards to Pending /
    /// Submitted) are rejected — a state machine that accepts anything
    /// silently is not a state machine.
    ///
    /// Rules (§3 spec §2; the chain is Pending → Submitted → Open →
    /// PartiallyFilled → Filled, plus the terminal / abnormal states):
    /// - Pending → Submitted | Rejected | Failed
    /// - Submitted → Open | PartiallyFilled | Filled | Canceled |
    ///   Rejected | Failed (an IOC / market order can fill, or be
    ///   cancelled, before it is ever seen resting)
    /// - Open / PartiallyFilled → Filled | Canceled | Expired | Failed
    ///   (and PartiallyFilled from Open)
    /// - Any non-terminal state → Unresolved (timeout / unknown outcome)
    /// - Unresolved → any live or terminal outcome found by
    ///   reconciliation, never back to Pending / Submitted.
    pub fn transition(&mut self, to: OrderStatus, now_ms: i64) -> Result<(), String> {
        let from = self.status;
        let legal = if from == to {
            true
        } else if from.is_terminal() {
            false
        } else if to == OrderStatus::Unresolved {
            true
        } else {
            match from {
                OrderStatus::Pending => matches!(
                    to,
                    OrderStatus::Submitted | OrderStatus::Rejected | OrderStatus::Failed
                ),
                OrderStatus::Submitted => matches!(
                    to,
                    OrderStatus::Open
                        | OrderStatus::PartiallyFilled
                        | OrderStatus::Filled
                        | OrderStatus::Canceled
                        | OrderStatus::Rejected
                        | OrderStatus::Failed
                ),
                OrderStatus::Open => matches!(
                    to,
                    OrderStatus::PartiallyFilled
                        | OrderStatus::Filled
                        | OrderStatus::Canceled
                        | OrderStatus::Expired
                        | OrderStatus::Failed
                ),
                OrderStatus::PartiallyFilled => matches!(
                    to,
                    OrderStatus::Filled
                        | OrderStatus::Canceled
                        | OrderStatus::Expired
                        | OrderStatus::Failed
                ),
                // Reconciliation lands the truth, whatever it is — but
                // an unresolved order never goes back to being unsent.
                OrderStatus::Unresolved => {
                    !matches!(to, OrderStatus::Pending | OrderStatus::Submitted)
                }
                _ => false,
            }
        };
        if !legal {
            return Err(format!(
                "订单状态非法转移：{from:?} → {to:?}（订单 {0}）",
                self.client_order_id
            ));
        }
        self.status = to;
        self.updated_at = now_ms;
        Ok(())
    }

    /// Record a fill: original quantity, filled quantity, remaining
    /// quantity and the volume-weighted average fill price are all
    /// updated, and the status moves to PartiallyFilled or Filled.
    /// The money math runs in `Decimal` — a fill must never be fudged
    /// by float error. Rejects fills on unsent / terminal orders and
    /// any fill that would take the total over the ordered quantity.
    pub fn apply_fill(
        &mut self,
        fill_qty: f64,
        fill_price: f64,
        now_ms: i64,
    ) -> Result<(), String> {
        if self.status.is_terminal() {
            return Err(format!(
                "订单 {} 已终结（{:?}），不能再记录成交",
                self.client_order_id, self.status
            ));
        }
        if self.status == OrderStatus::Pending {
            return Err("订单尚未发出，不能记录成交".to_string());
        }
        if !(fill_qty > 0.0) {
            return Err("成交量必须大于 0".to_string());
        }
        if !(fill_price > 0.0) {
            return Err("成交价必须大于 0".to_string());
        }
        let qty = crate::core::precision::to_decimal(fill_qty);
        let price = crate::core::precision::to_decimal(fill_price);
        let filled = crate::core::precision::to_decimal(self.filled_quantity) + qty;
        let total = crate::core::precision::to_decimal(self.quantity);
        if filled > total {
            return Err(format!(
                "成交量 {filled} 超过下单量 {total}（订单 {}）",
                self.client_order_id
            ));
        }
        let new_avg = match self.average_fill_price {
            Some(prev) => {
                let prev_filled = crate::core::precision::to_decimal(self.filled_quantity);
                (crate::core::precision::to_decimal(prev) * prev_filled + price * qty) / filled
            }
            None => price,
        };
        use rust_decimal::prelude::ToPrimitive;
        self.filled_quantity = filled.to_f64().unwrap_or(self.filled_quantity);
        self.remaining_quantity = (total - filled).to_f64().unwrap_or(0.0);
        self.average_fill_price = Some(new_avg.to_f64().unwrap_or(fill_price));
        let target = if filled >= total {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        self.transition(target, now_ms)
    }
}

impl From<&OrderInfo> for Order {
    fn from(info: &OrderInfo) -> Self {
        Order {
            order_id: info.order_id.clone(),
            client_order_id: info.client_order_id.clone(),
            exchange_id: info.exchange_id.clone(),
            account_id: info.account_id.clone(),
            symbol: info.symbol.clone(),
            side: info.side.clone(),
            order_type: info.order_type.clone(),
            quantity: info.quantity,
            price: info.price,
            status: info.status,
            ts: info.created_at,
        }
    }
}

/// One fill against an order (§3 spec field names). `fee_currency`
/// matters because fees are not always in the settlement currency;
/// `client_order_id` / `account_id` are kept for reconciliation and
/// routing although the spec's wire shape omits them.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeInfo {
    pub trade_id: String,
    pub exchange_id: String,
    pub order_id: String,
    pub client_order_id: String,
    pub account_id: String,
    pub symbol: String,
    /// "buy" | "sell".
    pub side: String,
    pub price: f64,
    pub quantity: f64,
    pub fee: Option<f64>,
    pub fee_currency: Option<String>,
    pub timestamp: i64,
}

/// One price level of an order book: [price, quantity].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceLevel(pub f64, pub f64);

/// Full order-book snapshot for one market (top levels only, best first).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBook {
    pub adapter_id: String,
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub ts: i64,
}

/// One open position in the live-account snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LivePosition {
    pub symbol: String,
    pub side: String,
    pub qty: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
}

/// Live read-only state of one connected real account, pushed on every
/// REST poll and on every connection state change. `currency` is the
/// settlement currency reported by the exchange (e.g. USDC).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountLive {
    pub account_id: String,
    pub adapter_id: String,
    /// Underlying wallet behind this live state, when the funds are
    /// shared: the Hyperliquid master wallet address for HL/Entropy.
    /// The 10s sampler counts each wallet once — both adapters
    /// aggregate main + io + spot for the same wallet, so summing
    /// both would double the equity. `None` falls back to the
    /// account id (each account counted separately).
    pub wallet_key: Option<String>,
    /// "connecting" | "connected" | "error" | "disconnected"
    pub status: String,
    /// Last measured single round-trip: the last REST request time.
    /// Never frame staleness.
    pub latency_ms: Option<u64>,
    pub equity_usd: f64,
    pub available_usd: f64,
    pub pnl_usd: f64,
    pub currency: String,
    pub positions: Vec<LivePosition>,
    pub error: Option<String>,
    pub ts: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> OrderInfo {
        OrderInfo {
            order_id: String::new(),
            client_order_id: "desss-test-1".to_string(),
            exchange_id: "hyperliquid".to_string(),
            account_id: "acc-1".to_string(),
            symbol: "BTC".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            status: OrderStatus::Pending,
            price: Some(90000.0),
            quantity: 0.01,
            filled_quantity: 0.0,
            remaining_quantity: 0.01,
            average_fill_price: None,
            error: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn happy_path_transitions() {
        let mut o = info();
        o.transition(OrderStatus::Submitted, 1).expect("submit");
        o.transition(OrderStatus::Open, 2).expect("open");
        o.transition(OrderStatus::PartiallyFilled, 3)
            .expect("partial");
        o.transition(OrderStatus::Filled, 4).expect("fill");
        assert!(o.status.is_terminal());
        assert_eq!(o.updated_at, 4);
    }

    #[test]
    fn terminal_states_reject_everything() {
        for terminal in [
            OrderStatus::Filled,
            OrderStatus::Rejected,
            OrderStatus::Canceled,
            OrderStatus::Expired,
            OrderStatus::Failed,
        ] {
            let mut o = info();
            o.status = terminal;
            assert!(o.transition(OrderStatus::Open, 1).is_err(), "{terminal:?}");
            assert!(
                o.transition(OrderStatus::Unresolved, 1).is_err(),
                "{terminal:?}"
            );
        }
    }

    #[test]
    fn timeout_is_unresolved_not_filled() {
        let mut o = info();
        o.transition(OrderStatus::Submitted, 1).expect("submit");
        // A send timeout: the outcome is unknown, never a fill.
        o.transition(OrderStatus::Unresolved, 2)
            .expect("unresolved");
        assert_eq!(o.status, OrderStatus::Unresolved);
        assert!(!o.status.is_terminal());
        // Reconciliation finds the order resting on the book.
        o.transition(OrderStatus::Open, 3).expect("reconciled open");
        // But it can never go back to being unsent.
        let mut o2 = info();
        o2.transition(OrderStatus::Submitted, 1).expect("submit");
        o2.transition(OrderStatus::Unresolved, 2)
            .expect("unresolved");
        assert!(o2.transition(OrderStatus::Pending, 3).is_err());
        assert!(o2.transition(OrderStatus::Submitted, 3).is_err());
    }

    #[test]
    fn illegal_moves_rejected() {
        let mut o = info();
        // Pending cannot jump straight to Open or Filled.
        assert!(o.transition(OrderStatus::Open, 1).is_err());
        assert!(o.transition(OrderStatus::Filled, 1).is_err());
        // Any non-terminal state may go Unresolved.
        assert!(o.transition(OrderStatus::Unresolved, 1).is_ok());
    }

    #[test]
    fn order_wire_shape_matches_info() {
        let mut o = info();
        o.transition(OrderStatus::Submitted, 1).expect("submit");
        let w = Order::from(&o);
        assert_eq!(w.status, OrderStatus::Submitted);
        assert_eq!(w.quantity, 0.01);
        assert_eq!(w.price, Some(90000.0));
        assert_eq!(w.client_order_id, "desss-test-1");
    }

    #[test]
    fn submitted_can_fill_or_cancel_without_resting() {
        // An IOC / market order can fill before it is ever seen resting.
        let mut o = info();
        o.transition(OrderStatus::Submitted, 1).expect("submit");
        o.transition(OrderStatus::Filled, 2)
            .expect("immediate fill");
        // And a cancel can win the race with the acknowledgement.
        let mut o2 = info();
        o2.transition(OrderStatus::Submitted, 1).expect("submit");
        o2.transition(OrderStatus::Canceled, 2)
            .expect("cancel wins");
    }

    #[test]
    fn partial_fills_record_quantities_and_weighted_avg() {
        let mut o = info();
        o.transition(OrderStatus::Submitted, 1).expect("submit");
        o.transition(OrderStatus::Open, 2).expect("open");
        o.apply_fill(0.004, 90000.0, 3).expect("first fill");
        assert_eq!(o.status, OrderStatus::PartiallyFilled);
        assert!((o.filled_quantity - 0.004).abs() < 1e-12);
        assert!((o.remaining_quantity - 0.006).abs() < 1e-12);
        assert_eq!(o.average_fill_price, Some(90000.0));
        // Second fill at a worse price: weighted average moves.
        o.apply_fill(0.006, 90100.0, 4).expect("second fill");
        assert_eq!(o.status, OrderStatus::Filled);
        assert!((o.filled_quantity - 0.01).abs() < 1e-12);
        assert!((o.remaining_quantity).abs() < 1e-12);
        // (0.004*90000 + 0.006*90100) / 0.01 = 90060.
        assert!((o.average_fill_price.unwrap() - 90060.0).abs() < 1e-9);
    }

    #[test]
    fn fills_reject_overfill_and_unsent_orders() {
        let mut o = info();
        assert!(o.apply_fill(0.001, 90000.0, 1).is_err());
        o.transition(OrderStatus::Submitted, 1).expect("submit");
        o.transition(OrderStatus::Open, 2).expect("open");
        assert!(o.apply_fill(0.02, 90000.0, 3).is_err());
        assert!(o.apply_fill(0.0, 90000.0, 3).is_err());
        assert!(o.apply_fill(0.001, 0.0, 3).is_err());
    }

    #[test]
    fn status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&OrderStatus::PartiallyFilled).unwrap(),
            "\"partially_filled\""
        );
    }

    /// Frontend/backend field alignment (incident 2026-11-26): the exact
    /// JSON `tauri.ts` `placeOrder` sends must deserialize. If this test
    /// fails, the Trading page's 下单 button fails with
    /// "missing field `account_id`" on the real machine.
    #[test]
    fn place_order_input_matches_frontend_payload() {
        let input: PlaceOrderInput = serde_json::from_str(
            r#"{"accountId":"acc-1","symbol":"BTC","side":"buy","orderType":"limit","quantity":0.01,"price":90000.0}"#,
        )
        .expect("frontend payload must deserialize");
        assert_eq!(input.account_id, "acc-1");
        assert_eq!(input.symbol, "BTC");
        assert_eq!(input.side, "buy");
        assert_eq!(input.order_type, "limit");
        assert!((input.quantity - 0.01).abs() < 1e-12);
        assert!((input.price - 90000.0).abs() < 1e-9);
    }

    /// Regression guard for the same incident: the old snake_case payload
    /// must NOT deserialize — the contract is camelCase, and silently
    /// accepting both would hide a frontend/backend drift.
    #[test]
    fn place_order_input_rejects_legacy_snake_case() {
        let err = serde_json::from_str::<PlaceOrderInput>(
            r#"{"account_id":"acc-1","symbol":"BTC","side":"buy","order_type":"limit","quantity":0.01,"price":90000.0}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("missing field `accountId`"),
            "{err}"
        );
    }

    /// Response direction: an order record sent back to the frontend must
    /// use camelCase keys, or the Trading page reads `undefined`.
    #[test]
    fn order_info_serializes_with_camel_case_keys() {
        let json = serde_json::to_string(&info()).unwrap();
        assert!(
            json.contains("\"clientOrderId\":\"desss-test-1\""),
            "{json}"
        );
        assert!(json.contains("\"filledQuantity\""), "{json}");
        assert!(!json.contains("client_order_id"), "{json}");
        assert!(!json.contains("filled_quantity"), "{json}");
    }
}
