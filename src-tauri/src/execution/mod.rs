//! Execution engine (phase 5A §4): the only path from a trading
//! intent to an adapter.
//!
//! Pipeline (spec §3 §4): 下单意图 → Risk Engine（允许/拒绝） →
//! Execution Engine → Adapter. Nothing — no page, no command, no
//! other module — calls an adapter's `place_order` / `cancel_order`
//! directly; every write goes through here.
//!
//! Rules the engine enforces:
//! - Prices / quantities are aligned to the catalog tick / step in
//!   Decimal before the gateway sees them (buy rounds down,
//!   sell rounds up); the gateway re-verifies alignment.
//! - While an order is Unresolved, a duplicate submission of the
//!   same intent is rejected — the user must reconcile first.
//! - A write whose outcome is unknown (`AdapterError::UnknownOutcome`)
//!   becomes Unresolved and is reconciled by cloid immediately;
//!   a timeout is never treated as a fill.
//! - Cancels wait for their result: the cancel is submitted, then
//!   the order is queried — canceled / already filled /
//!   partially-filled-then-canceled / unknown are all recorded.
//! - Fills are applied once only (the delta vs. the previously
//!   recorded filled quantity), then the local position estimate is
//!   updated and a reconciliation is triggered after the grace.
//! - Gateway rejections do NOT count toward the auto-pause counter:
//!   a rejection is the gateway working, not an anomaly. Exchange
//!   failures (UnknownOutcome, definite rejections) do.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::adapters::hl_signing;
use crate::adapters::market_catalog::MarketCatalog;
use crate::adapters::sync::SyncManager;
use crate::adapters::types::{OrderInfo, OrderRequest, OrderStatus, TradeInfo};
use crate::adapters::{AdapterError, ExchangeAdapter};
use crate::bus::EventBus;
use crate::data_engine::realtime::RealtimeEngine;
use crate::risk::precision::{align_down, align_price_for_side, to_decimal};
use crate::risk::{RiskCheckContext, RiskGateway};

/// Everything the engine needs, passed explicitly so the pipeline
/// stays testable without the whole app state.
pub struct ExecDeps {
    pub risk: Arc<RiskGateway>,
    pub orders: Arc<OrderStore>,
    pub sync: Arc<SyncManager>,
    pub streams: Arc<RealtimeEngine>,
    pub catalog: Arc<MarketCatalog>,
    pub bus: EventBus,
}

/// In-memory order store: the execution engine owns it. Orders are
/// keyed by client order id (cloid); fills are kept per cloid.
/// Nothing here is persisted — the exchange is the source of truth,
/// and the store is rebuilt from it on demand.
pub struct OrderStore {
    orders: RwLock<HashMap<String, OrderInfo>>,
    fills: RwLock<HashMap<String, Vec<TradeInfo>>>,
}

impl OrderStore {
    pub fn new() -> Self {
        Self {
            orders: RwLock::new(HashMap::new()),
            fills: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, info: OrderInfo) {
        if let Ok(mut m) = self.orders.write() {
            m.insert(info.client_order_id.clone(), info);
        }
    }

    pub fn get(&self, cloid: &str) -> Option<OrderInfo> {
        self.orders.read().ok().and_then(|m| m.get(cloid).cloned())
    }

    pub fn get_by_oid(&self, oid: &str) -> Option<OrderInfo> {
        self.orders
            .read()
            .ok()?
            .values()
            .find(|o| !o.order_id.is_empty() && o.order_id == oid)
            .cloned()
    }

    /// All records, newest first.
    pub fn list(&self) -> Vec<OrderInfo> {
        let mut out: Vec<OrderInfo> = self
            .orders
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        out.sort_by_key(|o| -o.created_at);
        out
    }

    /// Non-terminal records (Open / PartiallyFilled / Submitted /
    /// Unresolved), newest first.
    pub fn list_open(&self) -> Vec<OrderInfo> {
        self.list()
            .into_iter()
            .filter(|o| !o.status.is_terminal())
            .collect()
    }

    /// All recorded fills, newest first.
    pub fn list_fills(&self) -> Vec<TradeInfo> {
        let mut out: Vec<TradeInfo> = self
            .fills
            .read()
            .map(|m| m.values().flatten().cloned().collect())
            .unwrap_or_default();
        out.sort_by_key(|f| -f.timestamp);
        out
    }

    pub fn record_fills(&self, cloid: &str, fills: Vec<TradeInfo>) {
        if fills.is_empty() {
            return;
        }
        if let Ok(mut m) = self.fills.write() {
            m.insert(cloid.to_string(), fills);
        }
    }

    /// True when an order with the same account + symbol + side +
    /// quantity + price is Unresolved — resubmitting it would be a
    /// duplicate while its outcome is unknown.
    pub fn has_unresolved_duplicate(
        &self,
        account_id: &str,
        symbol: &str,
        side: &str,
        qty: f64,
        price: f64,
    ) -> Option<String> {
        self.orders
            .read()
            .ok()?
            .values()
            .find(|o| {
                o.status == OrderStatus::Unresolved
                    && o.account_id == account_id
                    && o.symbol == symbol
                    && o.side == side
                    && (o.quantity - qty).abs() < 1e-12
                    && o.price.map(|p| (p - price).abs() < 1e-12).unwrap_or(false)
            })
            .map(|o| o.client_order_id.clone())
    }

    /// Apply an authoritative query result to the stored record
    /// (status + fills + quantities). The queried record wins; when
    /// the store has no such order (placed outside this app) it is
    /// inserted as-is. Returns the merged record.
    pub fn apply_query(&self, queried: &OrderInfo) -> OrderInfo {
        if let Ok(mut m) = self.orders.write() {
            if let Some(stored) = m.get_mut(&queried.client_order_id) {
                // Keep the original intent's price/qty when the query
                // cannot see them (unknownOid never reaches here).
                let mut merged = queried.clone();
                if merged.quantity == 0.0 {
                    merged.quantity = stored.quantity;
                }
                *stored = merged.clone();
                return merged;
            }
            m.insert(queried.client_order_id.clone(), queried.clone());
        }
        queried.clone()
    }
}

/// A manual order intent from the Trading page.
pub struct PlaceIntent {
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub quantity: f64,
    pub price: f64,
}

fn log(deps: &ExecDeps, level: &str, message: String) {
    deps.bus.log(level, "order", message);
}

fn live_of(deps: &ExecDeps, account_id: &str) -> Option<crate::adapters::types::AccountLive> {
    deps.sync
        .live_states()
        .into_iter()
        .find(|s| s.account_id == account_id)
}

/// Place one order through the full pipeline. Returns the order
/// record: Open / Filled / Unresolved are all `Ok` (an Unresolved
/// order is a result the user must see, not a thrown error);
/// definite failures (gateway rejection, validation, exchange
/// rejection) are `Err` with the reason.
pub async fn place_order(deps: &ExecDeps, intent: PlaceIntent) -> Result<OrderInfo, String> {
    let now = crate::adapters::now_ms();
    let adapter = deps
        .sync
        .adapter(&intent.account_id)
        .ok_or_else(|| "账户未连接，请先在账户中心连接".to_string())?;
    let exchange_id = adapter.id().to_string();

    // Tick / step from the catalog (exact symbol match). The order
    // cannot be aligned without them.
    let row = deps
        .catalog
        .search(&intent.symbol, &[exchange_id.clone()])
        .into_iter()
        .find(|r| r.symbol == intent.symbol)
        .ok_or_else(|| format!("{} 不在 {} 的品种目录中", intent.symbol, exchange_id))?;
    let tick = row
        .tick_size
        .map(to_decimal)
        .filter(|t| *t > Decimal::ZERO)
        .ok_or_else(|| format!("{} 没有可用的 tick 数据，无法对齐价格", intent.symbol))?;
    let step = row
        .step_size
        .map(to_decimal)
        .filter(|s| *s > Decimal::ZERO)
        .ok_or_else(|| format!("{} 没有可用的 step 数据，无法对齐数量", intent.symbol))?;
    let min_notional = row.min_notional.map(to_decimal);

    // `to_decimal` maps NaN / infinity to zero, so the > 0 check
    // below is also the validity check.
    let price_dec = to_decimal(intent.price);
    let qty_dec = to_decimal(intent.quantity);
    if price_dec <= Decimal::ZERO || qty_dec <= Decimal::ZERO {
        return Err("价格和数量必须大于 0".to_string());
    }
    if let Some(min) = min_notional {
        if price_dec * qty_dec < min {
            return Err(format!("订单名义价值低于该品种最小值 ${min}", min = min));
        }
    }
    // Align first (buy rounds down, sell rounds up); the gateway
    // re-verifies alignment, so a bug here is caught, not shipped.
    let aligned_price = align_price_for_side(price_dec, tick, intent.side == "buy");
    let aligned_qty = align_down(qty_dec, step);
    if aligned_qty <= Decimal::ZERO {
        return Err("数量对齐步长后为 0（数量小于最小步长）".to_string());
    }
    if aligned_price != price_dec || aligned_qty != qty_dec {
        log(
            deps,
            "info",
            format!(
                "{} {} 下单价格/数量已按步长对齐：价格 {} → {}，数量 {} → {}",
                intent.symbol, intent.side, price_dec, aligned_price, qty_dec, aligned_qty
            ),
        );
    }

    let cloid = hl_signing::generate_cloid();
    let mut info = OrderInfo {
        order_id: String::new(),
        client_order_id: cloid.clone(),
        exchange_id: exchange_id.clone(),
        account_id: intent.account_id.clone(),
        symbol: intent.symbol.clone(),
        side: intent.side.clone(),
        order_type: intent.order_type.clone(),
        status: OrderStatus::Pending,
        price: Some(aligned_price.to_f64().unwrap_or(0.0)),
        quantity: aligned_qty.to_f64().unwrap_or(0.0),
        filled_quantity: 0.0,
        remaining_quantity: aligned_qty.to_f64().unwrap_or(0.0),
        average_fill_price: None,
        error: None,
        created_at: now,
        updated_at: now,
    };
    deps.orders.insert(info.clone());

    // The stream must be wanted before the freshness check means
    // anything; the Trading page also subscribes on selection, so
    // by the time the user clicks, the book is usually live.
    deps.streams
        .ensure_wanted((exchange_id.clone(), intent.symbol.clone()));

    // Fresh equity sample for the drawdown check (the 10s sampler
    // covers the steady state; the order attempt samples now).
    let live = live_of(deps, &intent.account_id);
    let connected = live
        .as_ref()
        .map(|s| s.status == "connected")
        .unwrap_or(false);
    let available = live.as_ref().map(|s| s.available_usd).unwrap_or(0.0);
    let equity = live.as_ref().map(|s| s.equity_usd).unwrap_or(0.0);
    if equity > 0.0 {
        deps.risk.record_equity(equity);
    }
    let current_position_notional: Decimal = live
        .as_ref()
        .map(|s| {
            s.positions
                .iter()
                .map(|p| to_decimal(p.qty.abs()) * to_decimal(p.mark_price))
                .sum()
        })
        .unwrap_or(Decimal::ZERO);
    let ctx = RiskCheckContext {
        account_connected: connected,
        book_stale: deps
            .streams
            .is_stale(&(exchange_id.clone(), intent.symbol.clone())),
        side: intent.side.clone(),
        notional_usd: aligned_price * aligned_qty,
        current_position_notional_usd: current_position_notional,
        available_usd: to_decimal(available),
        qty: aligned_qty,
        price: Some(aligned_price),
        tick_size: tick,
        step_size: step,
    };
    // A gateway rejection is the gateway working — it does not count
    // toward auto-pause, and the record is Rejected, not Failed.
    if let Err(e) = deps.risk.check_order(&ctx) {
        info.transition(OrderStatus::Rejected, crate::adapters::now_ms())
            .ok();
        info.error = Some(e.clone());
        deps.orders.insert(info.clone());
        return Err(e);
    }

    if let Some(dup) = deps.orders.has_unresolved_duplicate(
        &intent.account_id,
        &intent.symbol,
        &intent.side,
        aligned_qty.to_f64().unwrap_or(0.0),
        aligned_price.to_f64().unwrap_or(0.0),
    ) {
        return Err(format!(
            "存在未确认订单 {dup}（相同账户/品种/方向/数量/价格），请先查询或对账，禁止重复提交"
        ));
    }

    let req = OrderRequest {
        exchange_id: exchange_id.clone(),
        account_id: intent.account_id.clone(),
        symbol: intent.symbol.clone(),
        side: intent.side.clone(),
        order_type: intent.order_type.clone(),
        quantity: aligned_qty.to_f64().unwrap_or(0.0),
        price: Some(aligned_price.to_f64().unwrap_or(0.0)),
        client_order_id: Some(cloid.clone()),
    };
    info.transition(OrderStatus::Submitted, crate::adapters::now_ms())
        .ok();
    deps.orders.insert(info.clone());

    match adapter.place_order(req).await {
        Ok(order) => {
            info.order_id = order.order_id.clone();
            match order.status {
                OrderStatus::Open => {
                    info.transition(OrderStatus::Open, crate::adapters::now_ms())
                        .ok();
                    deps.orders.insert(info.clone());
                    deps.risk.record_order_outcome(true);
                    log(
                        deps,
                        "info",
                        format!(
                            "下单成功：{} {} {} {} @ {}（oid {}，GTC 挂单中）",
                            info.exchange_id,
                            info.symbol,
                            if info.side == "buy" {
                                "买入"
                            } else {
                                "卖出"
                            },
                            info.quantity,
                            info.price.unwrap_or(0.0),
                            info.order_id
                        ),
                    );
                    Ok(info)
                }
                OrderStatus::Filled => {
                    // The response says filled; the authoritative
                    // record (fills + avg price) comes from the query.
                    deps.risk.record_order_outcome(true);
                    Ok(reconcile_by_cloid(deps, &adapter, &info).await)
                }
                other => {
                    info.transition(other, crate::adapters::now_ms()).ok();
                    deps.orders.insert(info.clone());
                    Ok(info)
                }
            }
        }
        Err(AdapterError::UnknownOutcome(msg)) => {
            // The order may have landed: Unresolved, then reconcile
            // by cloid right away.
            info.transition(OrderStatus::Unresolved, crate::adapters::now_ms())
                .ok();
            info.error = Some(msg.clone());
            deps.orders.insert(info.clone());
            log(
                deps,
                "warn",
                format!(
                    "下单结果未知（{msg}），订单 {} 已进入对账",
                    info.client_order_id
                ),
            );
            deps.risk.record_order_outcome(false);
            Ok(reconcile_by_cloid(deps, &adapter, &info).await)
        }
        Err(AdapterError::Other(msg)) => {
            // A definite exchange rejection.
            info.transition(OrderStatus::Rejected, crate::adapters::now_ms())
                .ok();
            info.error = Some(msg.clone());
            deps.orders.insert(info.clone());
            deps.risk.record_order_outcome(false);
            Err(msg)
        }
        Err(AdapterError::NotConnected) => {
            info.transition(OrderStatus::Failed, crate::adapters::now_ms())
                .ok();
            info.error = Some("账户未连接".to_string());
            deps.orders.insert(info.clone());
            Err("账户未连接，无法下单".to_string())
        }
    }
}

/// Reconcile one order by cloid: query the exchange, apply the
/// authoritative record, record fills once (delta only), update the
/// local position estimate and trigger a position reconciliation.
/// Returns the merged record; on any query failure the order stays
/// exactly as it was (Unresolved is never guessed away).
async fn reconcile_by_cloid(
    deps: &ExecDeps,
    adapter: &Arc<dyn ExchangeAdapter>,
    info: &OrderInfo,
) -> OrderInfo {
    let prev_filled = deps
        .orders
        .get(&info.client_order_id)
        .map(|o| o.filled_quantity)
        .unwrap_or(0.0);
    match adapter
        .query_order_by_cloid(&info.account_id, &info.client_order_id)
        .await
    {
        Ok(queried) => {
            let merged = deps.orders.apply_query(&queried);
            let delta = (merged.filled_quantity - prev_filled).max(0.0);
            if delta > 0.0 {
                if let Ok(fills) = adapter
                    .query_fills(&info.account_id, &info.client_order_id)
                    .await
                {
                    deps.orders.record_fills(&info.client_order_id, fills);
                }
                deps.sync.apply_fill(
                    &info.account_id,
                    &info.symbol,
                    &info.side,
                    delta,
                    merged
                        .average_fill_price
                        .unwrap_or(merged.price.unwrap_or(0.0)),
                );
                deps.sync.reconcile_account(&info.account_id);
                log(
                    deps,
                    "info",
                    format!(
                        "订单 {} 对账：成交 {}（均价 {}），状态 {:?}",
                        info.client_order_id,
                        merged.filled_quantity,
                        merged.average_fill_price.unwrap_or(0.0),
                        merged.status
                    ),
                );
            } else if merged.status == OrderStatus::Canceled {
                deps.sync.reconcile_account(&info.account_id);
            }
            merged
        }
        Err(AdapterError::Other(msg)) if msg == "unknownOid" => {
            log(
                deps,
                "warn",
                format!(
                    "订单 {} 在交易所暂无记录（unknownOid），保持未确认状态",
                    info.client_order_id
                ),
            );
            deps.orders
                .get(&info.client_order_id)
                .unwrap_or_else(|| info.clone())
        }
        Err(e) => {
            log(
                deps,
                "warn",
                format!(
                    "订单 {} 对账查询失败（{e}），保持未确认状态",
                    info.client_order_id
                ),
            );
            deps.orders
                .get(&info.client_order_id)
                .unwrap_or_else(|| info.clone())
        }
    }
}

/// Cancel one order and wait for the result. `order_id` is the
/// exchange oid (from the open-orders list) or a cloid. Returns the
/// merged record: Canceled, Filled (the cancel lost the race),
/// PartiallyFilled-then-Canceled (status Canceled with fills), or
/// Unresolved when the outcome could not be confirmed. Only a
/// cancel that was never attempted (no key, bad symbol) is `Err`.
pub async fn cancel_order(
    deps: &ExecDeps,
    account_id: &str,
    order_id: &str,
    symbol: &str,
) -> Result<OrderInfo, String> {
    let adapter = deps
        .sync
        .adapter(account_id)
        .ok_or_else(|| "账户未连接，请先在账户中心连接".to_string())?;
    let live = live_of(deps, account_id);
    let connected = live
        .as_ref()
        .map(|s| s.status == "connected")
        .unwrap_or(false);
    deps.risk.check_cancel(connected)?;

    // Prefer the cloid when we know the order: cancelByCloid is the
    // stable path for an order we track; the open-orders list only
    // has the oid.
    let (cancel_id, info) = match deps.orders.get_by_oid(order_id) {
        Some(stored) => (stored.client_order_id.clone(), stored),
        None => (
            order_id.to_string(),
            OrderInfo {
                order_id: order_id.to_string(),
                client_order_id: order_id.to_string(),
                exchange_id: adapter.id().to_string(),
                account_id: account_id.to_string(),
                symbol: symbol.to_string(),
                side: String::new(),
                order_type: "limit".to_string(),
                status: OrderStatus::Open,
                price: None,
                quantity: 0.0,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                average_fill_price: None,
                error: None,
                created_at: crate::adapters::now_ms(),
                updated_at: crate::adapters::now_ms(),
            },
        ),
    };

    let submit = adapter.cancel_order(account_id, &cancel_id, symbol).await;
    match submit {
        Ok(()) | Err(AdapterError::UnknownOutcome(_)) => {
            // The cancel was accepted, or its outcome is unknown:
            // either way the order's fate is decided by the query.
            Ok(reconcile_by_cloid(deps, &adapter, &info).await)
        }
        Err(AdapterError::Other(msg)) => {
            // A cancel that was never attempted (no key, bad symbol,
            // bad oid, or an exchange whose ordering lands in a later
            // phase) has nothing to query for.
            if msg.contains("未保存 Agent")
                || msg.contains("不在该交易所")
                || msg.contains("订单号格式不正确")
                || msg.contains("下一阶段")
            {
                return Err(msg);
            }
            // "already canceled, or filled" and friends: query to
            // find out which it was.
            Ok(reconcile_by_cloid(deps, &adapter, &info).await)
        }
        Err(AdapterError::NotConnected) => Err("账户未连接，无法撤单".to_string()),
    }
}

/// Live open orders from the exchange, merged with the store so the
/// cloid (and our record) travels with each row.
pub async fn list_open_orders(deps: &ExecDeps, account_id: &str) -> Result<Vec<OrderInfo>, String> {
    let adapter = deps
        .sync
        .adapter(account_id)
        .ok_or_else(|| "账户未连接，请先在账户中心连接".to_string())?;
    let orders = adapter
        .get_open_orders(account_id)
        .await
        .map_err(|e| e.to_string())?;
    Ok(orders
        .into_iter()
        .map(|o| {
            if let Some(mut stored) = deps.orders.get_by_oid(&o.order_id) {
                // The exchange says it is open; our record may be
                // stale (e.g. a cancel that raced). Trust the query.
                stored
                    .transition(OrderStatus::Open, crate::adapters::now_ms())
                    .ok();
                stored
            } else {
                OrderInfo {
                    order_id: o.order_id,
                    client_order_id: o.client_order_id,
                    exchange_id: o.exchange_id,
                    account_id: o.account_id,
                    symbol: o.symbol,
                    side: o.side,
                    order_type: o.order_type,
                    status: OrderStatus::Open,
                    price: o.price,
                    quantity: o.quantity,
                    filled_quantity: 0.0,
                    remaining_quantity: o.quantity,
                    average_fill_price: None,
                    error: None,
                    created_at: o.ts,
                    updated_at: o.ts,
                }
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(cloid: &str, status: OrderStatus) -> OrderInfo {
        OrderInfo {
            order_id: "1".to_string(),
            client_order_id: cloid.to_string(),
            exchange_id: "hyperliquid".to_string(),
            account_id: "acc-1".to_string(),
            symbol: "BTC".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            status,
            price: Some(90000.0),
            quantity: 0.01,
            filled_quantity: 0.0,
            remaining_quantity: 0.01,
            average_fill_price: None,
            error: None,
            created_at: 1000,
            updated_at: 1000,
        }
    }

    #[test]
    fn duplicate_guard_only_blocks_unresolved() {
        let store = OrderStore::new();
        let mut unresolved = record("0xaaa", OrderStatus::Unresolved);
        unresolved.price = Some(90000.0);
        store.insert(unresolved);
        store.insert(record("0xbbb", OrderStatus::Filled));

        let dup = store.has_unresolved_duplicate("acc-1", "BTC", "buy", 0.01, 90000.0);
        assert_eq!(dup.as_deref(), Some("0xaaa"));
        assert!(store
            .has_unresolved_duplicate("acc-1", "BTC", "sell", 0.01, 90000.0)
            .is_none());
        assert!(store
            .has_unresolved_duplicate("acc-1", "ETH", "buy", 0.01, 90000.0)
            .is_none());
        assert!(store
            .has_unresolved_duplicate("acc-1", "BTC", "buy", 0.02, 90000.0)
            .is_none());
    }

    #[test]
    fn apply_query_merges_authoritative_state() {
        let store = OrderStore::new();
        store.insert(record("0xaaa", OrderStatus::Submitted));
        let mut queried = record("0xaaa", OrderStatus::PartiallyFilled);
        queried.filled_quantity = 0.004;
        queried.remaining_quantity = 0.006;
        queried.average_fill_price = Some(90100.0);
        let merged = store.apply_query(&queried);
        assert_eq!(merged.status, OrderStatus::PartiallyFilled);
        assert!((merged.filled_quantity - 0.004).abs() < 1e-12);
        assert_eq!(
            store.get("0xaaa").unwrap().status,
            OrderStatus::PartiallyFilled
        );
    }

    #[test]
    fn open_list_keeps_only_live_orders() {
        let store = OrderStore::new();
        store.insert(record("0xaaa", OrderStatus::Open));
        store.insert(record("0xbbb", OrderStatus::Filled));
        store.insert(record("0xccc", OrderStatus::Unresolved));
        let open = store.list_open();
        assert_eq!(open.len(), 2);
        assert!(open.iter().all(|o| !o.status.is_terminal()));
    }
}
