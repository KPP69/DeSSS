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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::adapters::hl_signing;
use crate::adapters::market_catalog::MarketCatalog;
use crate::adapters::sync::SyncManager;
use crate::adapters::types::{Order, OrderInfo, OrderRequest, OrderStatus, TradeInfo};
use crate::adapters::{AdapterError, ExchangeAdapter, UNKNOWN_OID};
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
    /// `unknownOid` strikes per cloid (F4): (count, last strike ms).
    /// In-memory, cumulative, reset on restart.
    unresolved_attempts: std::sync::Mutex<HashMap<String, (u32, i64)>>,
}

impl OrderStore {
    pub fn new() -> Self {
        Self {
            orders: RwLock::new(HashMap::new()),
            fills: RwLock::new(HashMap::new()),
            unresolved_attempts: std::sync::Mutex::new(HashMap::new()),
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

    /// Count one `unknownOid` for an Unresolved order (F4); returns
    /// the running total. Only an explicit `unknownOid` reaches
    /// here — transport / HTTP / parse failures never call it.
    pub fn unknown_oid_strike(&self, cloid: &str, now_ms: i64) -> u32 {
        if let Ok(mut m) = self.unresolved_attempts.lock() {
            let e = m.entry(cloid.to_string()).or_insert((0, 0));
            e.0 += 1;
            e.1 = now_ms;
            e.0
        } else {
            0
        }
    }

    /// Forget the strike entry when an order leaves Unresolved (F4).
    pub fn clear_unknown_oid(&self, cloid: &str) {
        if let Ok(mut m) = self.unresolved_attempts.lock() {
            m.remove(cloid);
        }
    }

    /// Drop strike entries not updated for over 10 minutes (F4):
    /// the table must not grow with runtime.
    pub fn prune_unknown_oid_strikes(&self, now_ms: i64) {
        if let Ok(mut m) = self.unresolved_attempts.lock() {
            m.retain(|_, (_, ts)| now_ms - *ts <= 600_000);
        }
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
                // F4: leaving Unresolved for a terminal state clears
                // the strike entry, whatever path applied the update.
                if stored.status == OrderStatus::Unresolved && merged.status.is_terminal() {
                    self.clear_unknown_oid(&queried.client_order_id);
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
    // F6: the side is exactly "buy" or "sell" — anything else is an
    // error, never silently converted to one of them.
    if intent.side != "buy" && intent.side != "sell" {
        return Err("下单方向只能是 buy 或 sell".to_string());
    }
    let now = crate::adapters::now_ms();
    let adapter = deps
        .sync
        .adapter(&intent.account_id)
        .ok_or_else(|| "账户未连接，请先在账户中心连接".to_string())?;
    let exchange_id = adapter.id().to_string();

    // The catalog stores the canonical symbol ("BTC", "io:NVDA");
    // the lookup below is case-insensitive, but everything
    // downstream uses the catalog row's symbol verbatim (rule 2.2)
    // — never the caller's spelling of it.
    let query = intent.symbol.trim();

    // Cold start: the user may order without ever visiting the Data
    // page, so this exchange's catalog can be empty. Load just this
    // one exchange (never the others) before looking the symbol up.
    // If the load fails, the lookup below reports the symbol as
    // missing, with the fetch failure already in the console log.
    if !deps.catalog.has_rows_for(&exchange_id) {
        log(
            deps,
            "info",
            format!("{exchange_id} 的品种目录尚未加载，正在按需拉取…"),
        );
        deps.catalog.ensure_exchange_loaded(&exchange_id).await;
    }

    // Tick / step from the catalog (case-insensitive symbol match).
    // The order cannot be aligned without them.
    let row = deps
        .catalog
        .search(query, &[exchange_id.clone()])
        .into_iter()
        .find(|r| r.symbol.eq_ignore_ascii_case(query))
        .ok_or_else(|| format!("{query} 不在 {exchange_id} 的品种目录中"))?;
    // Canonical symbol from here on: the row's, verbatim.
    let symbol = row.symbol.clone();
    let tick = row
        .tick_size
        .map(to_decimal)
        .filter(|t| *t > Decimal::ZERO)
        .ok_or_else(|| format!("{symbol} 没有可用的 tick 数据，无法对齐价格"))?;
    let step = row
        .step_size
        .map(to_decimal)
        .filter(|s| *s > Decimal::ZERO)
        .ok_or_else(|| format!("{symbol} 没有可用的 step 数据，无法对齐数量"))?;
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
                symbol, intent.side, price_dec, aligned_price, qty_dec, aligned_qty
            ),
        );
    }

    let cloid = hl_signing::generate_cloid();
    let mut info = OrderInfo {
        order_id: String::new(),
        client_order_id: cloid.clone(),
        exchange_id: exchange_id.clone(),
        account_id: intent.account_id.clone(),
        symbol: symbol.clone(),
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
        .ensure_wanted((exchange_id.clone(), symbol.clone()));

    // F6: cold start — a freshly subscribed stream has no frame
    // yet. Wait up to 3s for the first one before the risk check
    // (which would only see "stale"); on timeout the order is
    // Rejected, never sent on an empty book.
    if !deps
        .streams
        .wait_for_first_frame(
            &(exchange_id.clone(), symbol.clone()),
            std::time::Duration::from_secs(3),
        )
        .await
    {
        let err = "行情尚未就绪，请稍后重试".to_string();
        info.transition(OrderStatus::Rejected, now).ok();
        info.error = Some(err.clone());
        deps.orders.insert(info.clone());
        return Err(err);
    }

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
    // F5: the signed net position of this symbol (long +, short -),
    // matched by the native symbol (identical on both sides since
    // F1), for the reduce exemption in the max-position check.
    let position_qty_signed = live
        .as_ref()
        .and_then(|s| s.positions.iter().find(|p| p.symbol == symbol))
        .map(|p| if p.side == "short" { -p.qty } else { p.qty })
        .unwrap_or(0.0);
    let ctx = RiskCheckContext {
        account_connected: connected,
        book_stale: deps
            .streams
            .is_stale(&(exchange_id.clone(), symbol.clone())),
        side: intent.side.clone(),
        notional_usd: aligned_price * aligned_qty,
        current_position_notional_usd: current_position_notional,
        available_usd: to_decimal(available),
        qty: aligned_qty,
        position_qty_signed,
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
        &symbol,
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
        symbol: symbol.clone(),
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
                    Ok(reconcile_order(deps, &adapter, &info).await)
                }
                other => {
                    info.transition(other, crate::adapters::now_ms()).ok();
                    deps.orders.insert(info.clone());
                    Ok(info)
                }
            }
        }
        Err(AdapterError::UnknownOutcome(msg)) | Err(AdapterError::Transport(msg)) => {
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
            Ok(reconcile_order(deps, &adapter, &info).await)
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
        Err(AdapterError::Rejected { msg, .. }) => {
            // A definite exchange rejection, typed (no producer
            // emits this yet; the arm exists so the new variant
            // compiles). Same treatment as the Other arm above.
            info.transition(OrderStatus::Rejected, crate::adapters::now_ms())
                .ok();
            info.error = Some(msg.clone());
            deps.orders.insert(info.clone());
            deps.risk.record_order_outcome(false);
            Err(msg)
        }
        Err(e @ AdapterError::Unsupported(_)) => {
            // The venue does not implement ordering (no producer
            // emits this yet): definitely not sent, same treatment
            // as a rejection.
            info.transition(OrderStatus::Rejected, crate::adapters::now_ms())
                .ok();
            info.error = Some(e.to_string());
            deps.orders.insert(info.clone());
            deps.risk.record_order_outcome(false);
            Err(e.to_string())
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

/// Authoritative order state, by whatever id the record has: a cloid
/// (`0x…`) goes to `query_order_by_cloid`, a numeric oid to
/// `query_order_by_oid`. The two id forms must never be confused —
/// the exchange rejects a decimal oid sent as a string, which used
/// to make the post-cancel reconcile of an oid-only order silently
/// keep the old state (real-machine incident 2026-11-26).
async fn query_order_latest(
    adapter: &Arc<dyn ExchangeAdapter>,
    info: &OrderInfo,
) -> Result<OrderInfo, AdapterError> {
    if !info.client_order_id.is_empty() && info.client_order_id.starts_with("0x") {
        adapter
            .query_order_by_cloid(&info.account_id, &info.client_order_id)
            .await
    } else if let Ok(oid) = info.order_id.parse::<u64>() {
        adapter.query_order_by_oid(&info.account_id, oid).await
    } else if let Ok(oid) = info.client_order_id.parse::<u64>() {
        adapter.query_order_by_oid(&info.account_id, oid).await
    } else {
        Err(AdapterError::Other(
            "该订单没有可用的订单号，无法查询最新状态".to_string(),
        ))
    }
}

/// Reconcile one order against the exchange: query its authoritative
/// state, apply it to the store (fills once, delta only), update the
/// local position estimate and trigger a position reconciliation.
/// Returns the merged record; on any query failure the order stays
/// exactly as it was (Unresolved is never guessed away).
async fn reconcile_order(
    deps: &ExecDeps,
    adapter: &Arc<dyn ExchangeAdapter>,
    info: &OrderInfo,
) -> OrderInfo {
    let prev_filled = deps
        .orders
        .get(&info.client_order_id)
        .map(|o| o.filled_quantity)
        .unwrap_or(0.0);
    match query_order_latest(adapter, info).await {
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
        Err(AdapterError::Other(msg)) if msg == UNKNOWN_OID => {
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
        Ok(()) | Err(AdapterError::UnknownOutcome(_)) | Err(AdapterError::Transport(_)) => {
            // The cancel was accepted, or its outcome is unknown:
            // either way the order's fate is decided by the query.
            Ok(reconcile_order(deps, &adapter, &info).await)
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
            Ok(reconcile_order(deps, &adapter, &info).await)
        }
        Err(AdapterError::Rejected { .. }) => {
            // Definite refusal (no producer emits this yet): the
            // order's fate is decided by the query, same as the
            // Other arm's fall-through above.
            Ok(reconcile_order(deps, &adapter, &info).await)
        }
        Err(AdapterError::Unsupported(_)) => {
            // Ordering lands in a later phase: the cancel was never
            // attempted, nothing to query for.
            Err("该交易所撤单将在下一阶段接入".to_string())
        }
        Err(AdapterError::NotConnected) => Err("账户未连接，无法撤单".to_string()),
    }
}

/// Merge the exchange's live open orders into the store (pure step,
/// no network) and return the live list plus the local non-terminal
/// records that are no longer on the exchange.
///
/// - Each exchange row is authoritative for open state: a stored
///   record matching by oid is refreshed to Open; a row with no
///   stored record (an order placed on the website, or before a
///   restart) is inserted as an Open record, so it shows up and can
///   be cancelled. A website order without a cloid is keyed by its
///   oid string, exactly like the cancel flow's synthetic record.
/// - Local records in Open / Submitted / PartiallyFilled with a real
///   oid that are missing from the live list are returned for the
///   caller to query — they may have been filled or canceled
///   elsewhere. Terminal records and records without an oid (never
///   sent) are never reported missing. Unresolved is deliberately
///   excluded: it already has its own reconcile flow and must never
///   be cleaned up by a list diff.
fn merge_open_orders(
    store: &OrderStore,
    account_id: &str,
    exchange: &[Order],
) -> (Vec<OrderInfo>, Vec<OrderInfo>) {
    let mut live = Vec::new();
    let mut live_oids: HashSet<&str> = HashSet::new();
    for o in exchange {
        live_oids.insert(o.order_id.as_str());
        let row = if let Some(mut stored) = store.get_by_oid(&o.order_id) {
            // The exchange says it is open; our record may be stale
            // (e.g. a cancel that raced). Trust the exchange.
            stored
                .transition(OrderStatus::Open, crate::adapters::now_ms())
                .ok();
            stored.updated_at = crate::adapters::now_ms();
            stored
        } else {
            OrderInfo {
                order_id: o.order_id.clone(),
                client_order_id: if o.client_order_id.is_empty() {
                    o.order_id.clone()
                } else {
                    o.client_order_id.clone()
                },
                exchange_id: o.exchange_id.clone(),
                account_id: o.account_id.clone(),
                symbol: o.symbol.clone(),
                side: o.side.clone(),
                order_type: o.order_type.clone(),
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
        };
        store.insert(row.clone());
        live.push(row);
    }
    let missing: Vec<OrderInfo> = store
        .list()
        .into_iter()
        .filter(|r| {
            r.account_id == account_id
                && matches!(
                    r.status,
                    OrderStatus::Open | OrderStatus::Submitted | OrderStatus::PartiallyFilled
                )
                && !r.order_id.is_empty()
                && !live_oids.contains(r.order_id.as_str())
        })
        .collect();
    (live, missing)
}

/// Re-query Unresolved records older than 30s (F4): the 10s
/// open-orders task calls this after the list sync. A terminal
/// answer updates the record; each `unknownOid` is one strike and
/// the 5th turns the record Failed — the duplicate guard then
/// releases the same-params order on its own. Any other query
/// failure is ignored (retried next cycle, never counted).
async fn reconcile_unresolved(
    deps: &ExecDeps,
    adapter: &Arc<dyn ExchangeAdapter>,
    account_id: &str,
) {
    let now = crate::adapters::now_ms();
    deps.orders.prune_unknown_oid_strikes(now);
    let stale: Vec<OrderInfo> = deps
        .orders
        .list()
        .into_iter()
        .filter(|o| {
            o.account_id == account_id
                && o.status == OrderStatus::Unresolved
                && now - o.updated_at > 30_000
        })
        .collect();
    for record in stale {
        match query_order_latest(adapter, &record).await {
            Ok(queried) if queried.status.is_terminal() => {
                deps.orders.apply_query(&queried);
                deps.orders.clear_unknown_oid(&record.client_order_id);
            }
            Ok(_) => {}
            Err(AdapterError::Other(msg)) if msg == UNKNOWN_OID => {
                if deps.orders.unknown_oid_strike(&record.client_order_id, now) >= 5 {
                    let mut failed = record.clone();
                    failed.transition(OrderStatus::Failed, now).ok();
                    failed.error = Some("多次查询仍无此单，请人工核对".to_string());
                    deps.orders.insert(failed);
                    deps.orders.clear_unknown_oid(&record.client_order_id);
                }
            }
            Err(_) => {}
        }
    }
}

/// Sync one account's open orders with the exchange (the periodic
/// poll's job, and what the Trading page's refresh calls): the
/// exchange's live list is the only source of truth. Local Open /
/// Submitted / PartiallyFilled records that are gone from it are
/// queried for their final state (Canceled / Filled / …) and
/// updated; a failed query leaves the record untouched, never
/// guessed. Website-placed orders are imported into the store by
/// the merge, so they show up and can be cancelled.
pub async fn sync_open_orders(deps: &ExecDeps, account_id: &str) -> Result<Vec<OrderInfo>, String> {
    let adapter = deps
        .sync
        .adapter(account_id)
        .ok_or_else(|| "账户未连接，请先在账户中心连接".to_string())?;
    let exchange = adapter
        .get_open_orders(account_id)
        .await
        .map_err(|e| e.to_string())?;
    let (live, missing) = merge_open_orders(&deps.orders, account_id, &exchange);
    for record in missing {
        // The exchange no longer lists this order: ask for its final
        // state. `reconcile_order` keeps the record unchanged when
        // the query fails (unknownOid / network), so this never
        // deletes or guesses an order away.
        let updated = reconcile_order(deps, &adapter, &record).await;
        if updated.status != record.status {
            log(
                deps,
                "info",
                format!(
                    "订单 {} 已不在 {} 挂单中，状态更新为 {:?}",
                    record.client_order_id, record.exchange_id, updated.status
                ),
            );
        }
    }
    reconcile_unresolved(deps, &adapter, account_id).await;
    Ok(live)
}

/// Live open orders from the store only (F7): the 10s background
/// task owns the exchange sync, so this never touches the network.
/// The page may lag the exchange by up to 10s.
pub async fn list_open_orders(deps: &ExecDeps, account_id: &str) -> Result<Vec<OrderInfo>, String> {
    Ok(deps
        .orders
        .list()
        .into_iter()
        .filter(|o| {
            o.account_id == account_id
                && matches!(
                    o.status,
                    OrderStatus::Open | OrderStatus::Submitted | OrderStatus::PartiallyFilled
                )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::market_catalog::MarketCatalog;
    use crate::adapters::sync::SyncManager;
    use crate::adapters::types::{Balance, Position};
    use crate::adapters::AdapterResult;
    use crate::bus::EventBus;
    use crate::data_engine::realtime::RealtimeEngine;
    use crate::risk::{RiskConfig, RiskGateway};
    use async_trait::async_trait;

    fn deps_with(store: Arc<OrderStore>, sync: Arc<SyncManager>) -> ExecDeps {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        ExecDeps {
            risk: Arc::new(RiskGateway::new(RiskConfig::default())),
            orders: store,
            sync,
            streams: Arc::new(RealtimeEngine::new(bus.clone(), catalog.clone(), None)),
            catalog,
            bus,
        }
    }

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

    /// Cold-start order (incident 2026-11-26): the user opened the app
    /// and ordered BTC on Hyperliquid without ever visiting the Data
    /// page, so the catalog was empty and `place_order` failed with
    /// "BTC 不在 hyperliquid 的品种目录中". The order path must now
    /// load just that one exchange's catalog on demand first.
    ///
    /// The test does one real Hyperliquid `meta` fetch (the scratch
    /// crate has network; CI never runs `cargo test`). The intent uses
    /// lowercase "btc" on purpose: the engine must normalize the symbol
    /// before matching, or the row is missed even when loaded. The order
    /// itself is expected to fail later in the pipeline — there is no
    /// live account state here, so the risk gateway rejects it — but
    /// never with the catalog-missing error.
    #[tokio::test]
    async fn place_order_loads_cold_catalog_for_its_exchange() {
        use crate::adapters::hyperliquid::HyperliquidAdapter;
        use crate::adapters::market_catalog::MarketCatalog;
        use crate::adapters::sync::SyncManager;
        use crate::bus::EventBus;
        use crate::data_engine::realtime::RealtimeEngine;
        use crate::risk::{RiskConfig, RiskGateway};

        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let sync = Arc::new(SyncManager::new());
        let adapter = Arc::new(HyperliquidAdapter::new(
            "hyperliquid",
            None,
            "0x0000000000000000000000000000000000000000".to_string(),
            "acc-1".to_string(),
            bus.clone(),
            sync.live_map(),
            sync.reconcile_state(),
            None,
        ));
        sync.insert("acc-1".to_string(), adapter);
        let deps = ExecDeps {
            risk: Arc::new(RiskGateway::new(RiskConfig::default())),
            orders: Arc::new(OrderStore::new()),
            sync: sync.clone(),
            streams: Arc::new(RealtimeEngine::new(bus.clone(), catalog.clone(), None)),
            catalog: catalog.clone(),
            bus: bus.clone(),
        };
        assert!(!catalog.has_rows_for("hyperliquid"));
        let err = place_order(
            &deps,
            PlaceIntent {
                account_id: "acc-1".to_string(),
                symbol: "btc".to_string(),
                side: "buy".to_string(),
                order_type: "limit".to_string(),
                quantity: 0.01,
                price: 90000.0,
            },
        )
        .await
        .unwrap_err();
        // The cold catalog must have triggered a load attempt for
        // exactly this exchange: either the rows arrived, or the
        // failure was recorded. Other exchanges are never touched.
        assert!(
            catalog.has_rows_for("hyperliquid")
                || catalog.last_refresh_error("hyperliquid").is_some(),
            "cold catalog must trigger a load attempt for hyperliquid"
        );
        assert!(!catalog.has_rows_for("entropy"));
        assert!(catalog.last_refresh_error("entropy").is_none());
        if catalog.has_rows_for("hyperliquid") {
            // The catalog stage passed; the stored record carries the
            // normalized symbol even though the intent was lowercase.
            assert!(!err.contains("品种目录"), "{err}");
            let records = deps.orders.list();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].symbol, "BTC");
        }
    }

    /// F6: an unknown side is rejected before anything else runs —
    /// no adapter call, no record, no silent conversion to buy/sell.
    #[tokio::test]
    async fn place_order_rejects_unknown_side_without_fallback() {
        let deps = deps_with(Arc::new(OrderStore::new()), Arc::new(SyncManager::new()));
        let err = place_order(
            &deps,
            PlaceIntent {
                account_id: "acc-1".to_string(),
                symbol: "BTC".to_string(),
                side: "hold".to_string(),
                order_type: "limit".to_string(),
                quantity: 0.01,
                price: 90000.0,
            },
        )
        .await
        .unwrap_err();
        assert!(err.contains("buy") && err.contains("sell"), "{err}");
        assert!(deps.orders.list().is_empty());
    }

    /// One exchange open-orders row, for the sync tests.
    fn exchange_order(oid: &str, cloid: &str) -> Order {
        Order {
            order_id: oid.to_string(),
            client_order_id: cloid.to_string(),
            exchange_id: "hyperliquid".to_string(),
            account_id: "acc-1".to_string(),
            symbol: "BTC".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            quantity: 0.01,
            price: Some(90000.0),
            status: OrderStatus::Open,
            ts: 1000,
        }
    }

    /// Test double for the open-orders sync: a canned open-orders
    /// list and canned order-status answers, no network.
    struct FakeOrdersAdapter {
        open: std::sync::Mutex<Vec<Order>>,
        by_cloid: std::sync::Mutex<HashMap<String, OrderInfo>>,
        by_oid: std::sync::Mutex<HashMap<u64, OrderInfo>>,
    }

    #[async_trait]
    impl ExchangeAdapter for FakeOrdersAdapter {
        fn id(&self) -> &str {
            "hyperliquid"
        }
        fn name(&self) -> &str {
            "Fake Hyperliquid"
        }
        async fn connect(&self) -> AdapterResult<()> {
            Ok(())
        }
        async fn disconnect(&self) -> AdapterResult<()> {
            Ok(())
        }
        async fn subscribe_ticks(&self, _symbols: Vec<String>) -> AdapterResult<()> {
            Ok(())
        }
        async fn unsubscribe_ticks(&self, _symbols: Vec<String>) -> AdapterResult<()> {
            Ok(())
        }
        async fn get_balances(&self, _account_id: &str) -> AdapterResult<Vec<Balance>> {
            Ok(vec![])
        }
        async fn get_positions(&self, _account_id: &str) -> AdapterResult<Vec<Position>> {
            Ok(vec![])
        }
        async fn get_open_orders(&self, _account_id: &str) -> AdapterResult<Vec<Order>> {
            Ok(self.open.lock().unwrap().clone())
        }
        async fn place_order(&self, _req: OrderRequest) -> AdapterResult<Order> {
            Err(AdapterError::Other("fake adapter cannot place".to_string()))
        }
        async fn cancel_order(
            &self,
            _account_id: &str,
            _order_id: &str,
            _symbol: &str,
        ) -> AdapterResult<()> {
            Ok(())
        }
        async fn query_order_by_cloid(
            &self,
            _account_id: &str,
            cloid: &str,
        ) -> AdapterResult<OrderInfo> {
            if cloid == "0xerr" {
                // A read-path transport failure: realistic `Other`,
                // deliberately NOT "unknownOid" — it must never
                // strike (F4a).
                return Err(AdapterError::Other(
                    "Hyperliquid 查询失败：connection reset".to_string(),
                ));
            }
            self.by_cloid
                .lock()
                .unwrap()
                .get(cloid)
                .cloned()
                .ok_or_else(|| AdapterError::Other(UNKNOWN_OID.to_string()))
        }
        async fn query_order_by_oid(
            &self,
            _account_id: &str,
            oid: u64,
        ) -> AdapterResult<OrderInfo> {
            self.by_oid
                .lock()
                .unwrap()
                .get(&oid)
                .cloned()
                .ok_or_else(|| AdapterError::Other(UNKNOWN_OID.to_string()))
        }
    }

    #[test]
    fn merge_open_orders_reports_missing_and_imports_external() {
        let store = OrderStore::new();
        let mut a = record("0xaaa", OrderStatus::Open);
        a.order_id = "1".to_string();
        store.insert(a);
        let mut b = record("0xbbb", OrderStatus::Open);
        b.order_id = "2".to_string();
        store.insert(b);
        let mut filled = record("0xeee", OrderStatus::Filled);
        filled.order_id = "5".to_string();
        store.insert(filled);
        let mut unresolved = record("0xfff", OrderStatus::Unresolved);
        unresolved.order_id = "6".to_string();
        store.insert(unresolved);

        let (live, missing) = merge_open_orders(
            &store,
            "acc-1",
            &[exchange_order("2", "0xbbb"), exchange_order("3", "0xccc")],
        );
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].order_id, "2");
        assert_eq!(live[1].order_id, "3");
        assert_eq!(live[1].client_order_id, "0xccc");
        // Only A is missing: the Filled record is terminal, and the
        // Unresolved one is deliberately never cleaned up by a list
        // diff.
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].client_order_id, "0xaaa");
        // The website order was imported into the store.
        assert_eq!(store.get("0xccc").unwrap().status, OrderStatus::Open);
    }

    /// End-to-end sync (real-machine incident 2026-11-26): an order
    /// canceled on the website must leave the store, and a
    /// website-placed order must appear and be cancellable.
    #[tokio::test]
    async fn sync_open_orders_imports_external_and_cleans_up_missing() {
        use crate::adapters::market_catalog::MarketCatalog;
        use crate::adapters::sync::SyncManager;
        use crate::bus::EventBus;
        use crate::data_engine::realtime::RealtimeEngine;
        use crate::risk::{RiskConfig, RiskGateway};

        // Local records: A (oid "1", Open) will be missing from the
        // exchange list — the fake query says Canceled. B (oid "2",
        // Open) stays listed — stays Open.
        let store = Arc::new(OrderStore::new());
        let mut a = record("0xaaa", OrderStatus::Open);
        a.order_id = "1".to_string();
        store.insert(a);
        let mut b = record("0xbbb", OrderStatus::Open);
        b.order_id = "2".to_string();
        store.insert(b);
        // A record for another account must be untouched.
        let mut other = record("0xddd", OrderStatus::Open);
        other.account_id = "acc-2".to_string();
        other.order_id = "9".to_string();
        store.insert(other);

        // Exchange list: B (still open) + C (oid "3", placed on the
        // website with a cloid) + D (oid "4", website order, no cloid).
        let fake = Arc::new(FakeOrdersAdapter {
            open: std::sync::Mutex::new(vec![
                exchange_order("2", "0xbbb"),
                exchange_order("3", "0xccc"),
                exchange_order("4", ""),
            ]),
            by_cloid: std::sync::Mutex::new(HashMap::from([("0xaaa".to_string(), {
                let mut q = record("0xaaa", OrderStatus::Canceled);
                q.order_id = "1".to_string();
                q
            })])),
            by_oid: std::sync::Mutex::new(HashMap::new()),
        });
        let sync = Arc::new(SyncManager::new());
        sync.insert("acc-1".to_string(), fake);
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let deps = ExecDeps {
            risk: Arc::new(RiskGateway::new(RiskConfig::default())),
            orders: store.clone(),
            sync: sync.clone(),
            streams: Arc::new(RealtimeEngine::new(bus.clone(), catalog.clone(), None)),
            catalog: catalog.clone(),
            bus: bus.clone(),
        };
        let live = sync_open_orders(&deps, "acc-1").await.expect("sync");
        // Live list: B + C + D, in exchange order.
        assert_eq!(live.len(), 3);
        assert_eq!(live[0].order_id, "2");
        assert_eq!(live[1].order_id, "3");
        assert_eq!(live[1].client_order_id, "0xccc");
        assert_eq!(live[2].order_id, "4");
        assert_eq!(live[2].client_order_id, "4");
        // A was queried and marked Canceled; B stayed Open; the
        // website orders were imported into the store.
        assert_eq!(store.get("0xaaa").unwrap().status, OrderStatus::Canceled);
        assert_eq!(store.get("0xbbb").unwrap().status, OrderStatus::Open);
        assert_eq!(store.get("0xccc").unwrap().status, OrderStatus::Open);
        assert_eq!(store.get("4").unwrap().status, OrderStatus::Open);
        // The other account's record was not touched.
        assert_eq!(store.get("0xddd").unwrap().status, OrderStatus::Open);
    }

    /// F4: an Unresolved record older than 30s is re-queried on the
    /// 10s sync; 5 consecutive `unknownOid` strikes turn it Failed
    /// (releasing the duplicate guard), a terminal answer updates
    /// it, and a fresh Unresolved record is never touched.
    #[tokio::test]
    async fn unresolved_requery_five_strikes_turns_failed() {
        let store = Arc::new(OrderStore::new());
        let mut old = record("0xaaa", OrderStatus::Unresolved);
        old.updated_at = crate::adapters::now_ms() - 31_000;
        store.insert(old);
        let mut fresh = record("0xbbb", OrderStatus::Unresolved);
        fresh.side = "sell".to_string();
        fresh.updated_at = crate::adapters::now_ms();
        store.insert(fresh);
        // A transport failure (not unknownOid) must never strike.
        let mut err = record("0xerr", OrderStatus::Unresolved);
        err.side = "sell".to_string();
        err.updated_at = crate::adapters::now_ms() - 31_000;
        store.insert(err);
        let fake = Arc::new(FakeOrdersAdapter {
            open: std::sync::Mutex::new(vec![]),
            by_cloid: std::sync::Mutex::new(HashMap::new()),
            by_oid: std::sync::Mutex::new(HashMap::new()),
        });
        let sync = Arc::new(SyncManager::new());
        sync.insert("acc-1".to_string(), fake.clone());
        let deps = deps_with(store.clone(), sync);
        for _ in 0..4 {
            sync_open_orders(&deps, "acc-1").await.expect("sync");
            assert_eq!(store.get("0xaaa").unwrap().status, OrderStatus::Unresolved);
        }
        sync_open_orders(&deps, "acc-1").await.expect("sync");
        let failed = store.get("0xaaa").unwrap();
        assert_eq!(failed.status, OrderStatus::Failed);
        assert_eq!(
            failed.error.as_deref(),
            Some("多次查询仍无此单，请人工核对")
        );
        // Fresh record untouched, transport-error record never
        // struck, and the Failed one no longer blocks the
        // same-params order.
        assert_eq!(store.get("0xbbb").unwrap().status, OrderStatus::Unresolved);
        assert_eq!(store.get("0xerr").unwrap().status, OrderStatus::Unresolved);
        assert!(store
            .has_unresolved_duplicate("acc-1", "BTC", "buy", 0.01, 90000.0)
            .is_none());
        // A terminal answer updates the record instead of striking.
        let mut old2 = record("0xccc", OrderStatus::Unresolved);
        old2.updated_at = crate::adapters::now_ms() - 31_000;
        store.insert(old2);
        fake.by_cloid
            .lock()
            .unwrap()
            .insert("0xccc".to_string(), record("0xccc", OrderStatus::Canceled));
        sync_open_orders(&deps, "acc-1").await.expect("sync");
        assert_eq!(store.get("0xccc").unwrap().status, OrderStatus::Canceled);
    }

    /// F7: listing open orders is a pure store read — no adapter is
    /// needed (or consulted), and only open-state records come back.
    #[tokio::test]
    async fn list_open_orders_reads_the_store_only() {
        let store = Arc::new(OrderStore::new());
        store.insert(record("0xaaa", OrderStatus::Open));
        store.insert(record("0xbbb", OrderStatus::Unresolved));
        store.insert(record("0xccc", OrderStatus::Filled));
        let deps = deps_with(store, Arc::new(SyncManager::new()));
        let open = list_open_orders(&deps, "acc-1").await.expect("list");
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].client_order_id, "0xaaa");
    }

    /// F4b: strikes clear explicitly on leaving Unresolved, and the
    /// 10-minute sweep drops entries that stopped updating.
    #[test]
    fn unknown_oid_strikes_clear_and_prune() {
        let store = OrderStore::new();
        assert_eq!(store.unknown_oid_strike("0xaaa", 1000), 1);
        assert_eq!(store.unknown_oid_strike("0xaaa", 2000), 2);
        store.clear_unknown_oid("0xaaa");
        assert_eq!(store.unknown_oid_strike("0xaaa", 3000), 1);
        store.unknown_oid_strike("0xold", 0);
        store.prune_unknown_oid_strikes(600_001);
        assert_eq!(store.unknown_oid_strike("0xold", 600_002), 1);
    }
}
