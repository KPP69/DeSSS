//! Strategy runner: one task, 200ms rounds. Each round loads the
//! enabled spread instances from settings, evaluates them against
//! the live books, and places both legs concurrently when a signal
//! fires. The runner never invents state: books come from the bus
//! snapshot, positions from the live snapshots, specs from the
//! catalog. A round that cannot verify its inputs is skipped, and a
//! skip is logged only when its reason changes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::core::types::{AccountLive, OrderBook, OrderInfo};
use crate::core::util::now_ms;
use crate::data_engine::DataEngine;
use crate::execution::{self, ExecDeps, PlaceIntent as ExecIntent};

use super::spread::{self, BookSnapshot, MarketSpec, PlaceIntent as SpreadIntent, SpreadState};
use super::{load_strategies, set_strategy_enabled, LegRef, StrategyInstance, StrategyStatsMap};

/// A book older than this is stale for the runner (same 5s as the
/// evaluator's rule).
const BOOK_STALE_MS: i64 = 5_000;
/// A position snapshot older than this is stale.
const POSITION_STALE_MS: i64 = 15_000;
/// Warm-up: an exchange with no successful runner request inside
/// this window gets one light read before any order is placed.
const WARMUP_IDLE_MS: i64 = 60_000;
const ROUND_MS: u64 = 200;

pub struct RunnerDeps {
    pub exec: ExecDeps,
    pub db: Arc<DataEngine>,
    pub stats: StrategyStatsMap,
    /// Armed spread state per instance (memory only, restart resets).
    pub states: Arc<Mutex<HashMap<String, SpreadState>>>,
    /// Last successful runner request per exchange id (warm-up).
    pub last_success: Arc<Mutex<HashMap<String, i64>>>,
}

impl RunnerDeps {
    pub fn new(exec: ExecDeps, db: Arc<DataEngine>, stats: StrategyStatsMap) -> Self {
        Self {
            exec,
            db,
            stats,
            states: Default::default(),
            last_success: Default::default(),
        }
    }
}

fn log(deps: &RunnerDeps, level: &str, message: String) {
    deps.exec.bus.log(level, "strategy", message);
}

/// The production loop: one round every 200ms, forever. Spawned once
/// from `lib.rs` via `tauri::async_runtime::spawn`.
pub async fn run(deps: RunnerDeps) {
    loop {
        run_round(&deps, now_ms()).await;
        tokio::time::sleep(Duration::from_millis(ROUND_MS)).await;
    }
}

/// One round over all enabled spread instances. Testable directly:
/// the tests drive this with a fake adapter and injected books.
pub async fn run_round(deps: &RunnerDeps, now: i64) {
    let instances = match load_strategies(&deps.db) {
        Ok(v) => v,
        Err(e) => {
            log(deps, "error", format!("策略配置读取失败：{e}"));
            return;
        }
    };
    for inst in instances
        .into_iter()
        .filter(|s| s.enabled && s.kind == "spread")
    {
        run_instance(deps, &inst, now).await;
    }
}

fn find_book(books: &[OrderBook], leg: &LegRef) -> Option<OrderBook> {
    books
        .iter()
        .find(|b| b.adapter_id == leg.exchange_id && b.symbol == leg.symbol)
        .cloned()
}

fn live_of(deps: &RunnerDeps, account_id: &str) -> Option<AccountLive> {
    deps.exec
        .sync
        .live_states()
        .into_iter()
        .find(|s| s.account_id == account_id)
}

/// Signed net position of one symbol in a live snapshot (long +,
/// short −), matched exactly by the leg's symbol.
fn net_qty(live: &AccountLive, symbol: &str) -> f64 {
    live.positions
        .iter()
        .find(|p| p.symbol == symbol)
        .map(|p| if p.side == "short" { -p.qty } else { p.qty })
        .unwrap_or(0.0)
}

fn spec_for(deps: &RunnerDeps, leg: &LegRef) -> Option<MarketSpec> {
    let row = deps
        .exec
        .catalog
        .search(&leg.symbol, std::slice::from_ref(&leg.exchange_id))
        .into_iter()
        .find(|r| r.symbol == leg.symbol)?;
    MarketSpec::from_row(&row)
}

/// Skip this round for one instance, logging only when the reason
/// changed since the last skip.
fn skip(deps: &RunnerDeps, inst: &StrategyInstance, reason: &str) {
    let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
    let entry = stats.entry(inst.id.clone()).or_default();
    if entry.last_skip.as_deref() != Some(reason) {
        entry.last_skip = Some(reason.to_string());
        drop(stats);
        log(deps, "info", format!("策略 {} 跳过本轮：{reason}", inst.id));
    }
}

fn clear_skip(deps: &RunnerDeps, inst: &StrategyInstance) {
    let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(entry) = stats.get_mut(&inst.id) {
        entry.last_skip = None;
    }
}

fn note_success(deps: &RunnerDeps, exchange_id: &str, now: i64) {
    if let Ok(mut m) = deps.last_success.lock() {
        m.insert(exchange_id.to_string(), now);
    }
}

fn recent_success(deps: &RunnerDeps, exchange_id: &str, now: i64) -> bool {
    deps.last_success
        .lock()
        .ok()
        .and_then(|m| m.get(exchange_id).copied())
        .map(|t| now - t <= WARMUP_IDLE_MS)
        .unwrap_or(false)
}

fn to_exec_intent(
    leg: &LegRef,
    intent: &SpreadIntent,
    origin: &str,
    reduce_only: bool,
) -> ExecIntent {
    ExecIntent {
        account_id: leg.account_id.clone(),
        symbol: leg.symbol.clone(),
        side: intent.side.clone(),
        order_type: intent.order_type.clone(),
        quantity: intent.quantity,
        price: intent.price,
        reduce_only,
        origin: origin.to_string(),
    }
}

fn filled_of(r: &Result<OrderInfo, String>) -> f64 {
    r.as_ref().map(|info| info.filled_quantity).unwrap_or(0.0)
}

/// Disable one instance (persisted) with an error log and a
/// last-error the page shows. A disabled instance never trades
/// again until the user re-enables it.
fn disable_instance(deps: &RunnerDeps, inst: &StrategyInstance, reason: &str) {
    if let Err(e) = set_strategy_enabled(&deps.db, &inst.id, false) {
        log(
            deps,
            "error",
            format!("策略 {} 停用失败（{}）：{e}", inst.id, reason),
        );
        return;
    }
    {
        let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
        let entry = stats.entry(inst.id.clone()).or_default();
        entry.last_error = Some(reason.to_string());
    }
    log(deps, "error", format!("策略 {} 已停用：{reason}", inst.id));
}

/// First-order check (M1 follow-up): the order just placed must be
/// found on the exchange, under its cloid, on the intended coin.
/// The live open-orders list is tried first; a filled (or
/// list-lagging) order falls back to a direct cloid query. Any
/// mismatch — or any failure to verify — is an error: the caller
/// cancels and disables.
async fn verify_coin(deps: &RunnerDeps, leg: &LegRef, info: &OrderInfo) -> Result<(), String> {
    let Some(adapter) = deps.exec.sync.adapter(&leg.account_id) else {
        return Err("账户未连接".to_string());
    };
    if let Ok(orders) = adapter.get_open_orders(&leg.account_id).await {
        if let Some(o) = orders
            .iter()
            .find(|o| o.client_order_id == info.client_order_id)
        {
            if o.symbol == leg.symbol {
                return Ok(());
            }
            return Err(format!(
                "订单 coin 不符：期望 {}，实际 {}",
                leg.symbol, o.symbol
            ));
        }
    }
    match adapter
        .query_order_by_cloid(&leg.account_id, &info.client_order_id)
        .await
    {
        Ok(q) if q.symbol == leg.symbol => Ok(()),
        Ok(q) => Err(format!(
            "订单 coin 不符：期望 {}，实际 {}",
            leg.symbol, q.symbol
        )),
        Err(e) => Err(format!("首单校验查询失败：{e}")),
    }
}

async fn run_instance(deps: &RunnerDeps, inst: &StrategyInstance, now: i64) {
    // The legs' streams must be wanted, or no frames ever arrive.
    // Only (re)subscribe when the book is missing or stale: a fresh
    // book means the stream is already running, and the Data page's
    // set_stream_symbols may have removed a key the strategy needs.
    let books = deps.exec.bus.snapshot_books();
    for leg in [&inst.leg_a, &inst.leg_b] {
        let fresh = find_book(&books, leg).map(|b| now - b.ts <= BOOK_STALE_MS);
        if fresh != Some(true) {
            deps.exec
                .streams
                .ensure_wanted((leg.exchange_id.clone(), leg.symbol.clone()));
        }
    }
    // (a) books fresh (≤5s).
    let Some(book_a) = find_book(&books, &inst.leg_a) else {
        return skip(deps, inst, "A 腿盘口尚未就绪");
    };
    let Some(book_b) = find_book(&books, &inst.leg_b) else {
        return skip(deps, inst, "B 腿盘口尚未就绪");
    };
    if now - book_a.ts > BOOK_STALE_MS {
        return skip(deps, inst, "A 腿盘口陈旧");
    }
    if now - book_b.ts > BOOK_STALE_MS {
        return skip(deps, inst, "B 腿盘口陈旧");
    }
    // (b) position snapshots fresh (≤15s) and (c) neither leg's
    // exchange down / rate-limited. There is no separate venue-health
    // registry: the account's live status is the signal — the
    // adapter poll loop reports "error" when its REST fails,
    // including 429s — so "connected" is the gate.
    let Some(live_a) = live_of(deps, &inst.leg_a.account_id) else {
        return skip(deps, inst, "A 腿账户未连接");
    };
    let Some(live_b) = live_of(deps, &inst.leg_b.account_id) else {
        return skip(deps, inst, "B 腿账户未连接");
    };
    if live_a.status != "connected" {
        return skip(deps, inst, &format!("A 腿账户状态 {}", live_a.status));
    }
    if live_b.status != "connected" {
        return skip(deps, inst, &format!("B 腿账户状态 {}", live_b.status));
    }
    if now - live_a.ts > POSITION_STALE_MS {
        return skip(deps, inst, "A 腿持仓快照陈旧");
    }
    if now - live_b.ts > POSITION_STALE_MS {
        return skip(deps, inst, "B 腿持仓快照陈旧");
    }
    let Some(spec_a) = spec_for(deps, &inst.leg_a) else {
        return skip(deps, inst, "A 腿品种不在目录中");
    };
    let Some(spec_b) = spec_for(deps, &inst.leg_b) else {
        return skip(deps, inst, "B 腿品种不在目录中");
    };
    clear_skip(deps, inst);
    let net = net_qty(&live_a, &inst.leg_a.symbol);
    let snap_a = BookSnapshot::from_order_book(&book_a);
    let snap_b = BookSnapshot::from_order_book(&book_b);
    let mut state = {
        let mut states = deps.states.lock().unwrap_or_else(|e| e.into_inner());
        states.remove(&inst.id).unwrap_or_default()
    };
    let intents = spread::evaluate(
        &snap_a,
        &snap_b,
        &spec_a,
        &spec_b,
        &inst.params,
        &mut state,
        net,
        now,
    );
    {
        let mut states = deps.states.lock().unwrap_or_else(|e| e.into_inner());
        states.insert(inst.id.clone(), state);
    }
    {
        let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
        let entry = stats.entry(inst.id.clone()).or_default();
        entry.last_eval_ms = Some(now);
    }
    let Some(intents) = intents else {
        // No signal: a clean evaluation clears a stale last error.
        let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = stats.get_mut(&inst.id) {
            entry.last_error = None;
        }
        return;
    };
    let origin = format!("strategy:{}", inst.id);
    // (e) warm-up: no successful request to this exchange in 60s.
    for leg in [&inst.leg_a, &inst.leg_b] {
        if recent_success(deps, &leg.exchange_id, now) {
            continue;
        }
        let Some(adapter) = deps.exec.sync.adapter(&leg.account_id) else {
            return skip(deps, inst, "下单前账户未连接");
        };
        match adapter.get_open_orders(&leg.account_id).await {
            Ok(_) => note_success(deps, &leg.exchange_id, now),
            Err(e) => {
                return skip(deps, inst, &format!("{} 预热失败：{e}", leg.exchange_id));
            }
        }
    }
    let (ia, ib) = (&intents[0], &intents[1]);
    let dir_label = if ia.side == "sell" {
        "卖A买B"
    } else {
        "买A卖B"
    };
    // (c) both legs concurrently: the order of the two sends is not
    // guaranteed, and must not matter.
    let (ra, rb) = tokio::join!(
        execution::place_order(&deps.exec, to_exec_intent(&inst.leg_a, ia, &origin, false)),
        execution::place_order(&deps.exec, to_exec_intent(&inst.leg_b, ib, &origin, false)),
    );
    for (leg, r) in [(&inst.leg_a, &ra), (&inst.leg_b, &rb)] {
        if let Ok(info) = r {
            if matches!(
                info.status,
                crate::core::types::OrderStatus::Open
                    | crate::core::types::OrderStatus::Filled
                    | crate::core::types::OrderStatus::PartiallyFilled
            ) {
                note_success(deps, &leg.exchange_id, now);
            }
        }
    }
    let fill_a = filled_of(&ra);
    let fill_b = filled_of(&rb);
    if let (Err(ea), Err(eb)) = (&ra, &rb) {
        let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
        let entry = stats.entry(inst.id.clone()).or_default();
        entry.last_error = Some(format!("下单失败：A {ea} / B {eb}"));
        log(
            deps,
            "error",
            format!("策略 {} 下单失败：A {ea} / B {eb}", inst.id),
        );
        return;
    }
    let (prem_sell, prem_buy) = spread::premiums(&snap_a, &snap_b);
    let prem = if ia.side == "sell" {
        prem_sell
    } else {
        prem_buy
    }
    .unwrap_or(0.0);
    log(
        deps,
        "info",
        format!(
            "策略 {} 下单：{dir_label} {} @ A {} / B {}，溢价 {prem:.1} bps，{origin}",
            inst.id, ia.quantity, ia.price, ib.price
        ),
    );
    // (d) first-order check first: never balance (or trust) an order
    // that landed on the wrong coin.
    let mut coin_bad = false;
    for (leg, r) in [(&inst.leg_a, &ra), (&inst.leg_b, &rb)] {
        let Ok(info) = r else { continue };
        if let Err(e) = verify_coin(deps, leg, info).await {
            coin_bad = true;
            if let Some(adapter) = deps.exec.sync.adapter(&leg.account_id) {
                let _ = adapter
                    .cancel_order(&leg.account_id, &info.client_order_id, &leg.symbol)
                    .await;
            }
            log(
                deps,
                "error",
                format!("策略 {} 首单校验失败（{}）：{e}", inst.id, leg.symbol),
            );
        }
    }
    if coin_bad {
        disable_instance(deps, inst, "首单校验失败，订单 coin 与腿品种不符");
        return;
    }
    // Balance the fills: the leg that filled more is flattened down
    // to the matched quantity with a reduce-only order.
    let matched = fill_a.min(fill_b);
    let legs: [(&LegRef, &SpreadIntent, f64, &MarketSpec, &OrderBook); 2] = [
        (&inst.leg_a, ia, fill_a - matched, &spec_a, &book_a),
        (&inst.leg_b, ib, fill_b - matched, &spec_b, &book_b),
    ];
    for (leg, intent, excess, spec, book) in legs {
        if excess <= 0.0 {
            continue;
        }
        if excess < spec.step_size {
            log(
                deps,
                "info",
                format!("策略 {} 未匹配余量 {excess} 小于一步长，暂不补平", inst.id),
            );
            continue;
        }
        // Reduce at the current opposite top price (fall back to the
        // original limit when the book has no such side).
        let price = if intent.side == "sell" {
            book.bids.first().map(|l| l.0)
        } else {
            book.asks.first().map(|l| l.0)
        }
        .unwrap_or(intent.price);
        let balance = SpreadIntent {
            side: if intent.side == "sell" {
                "buy".to_string()
            } else {
                "sell".to_string()
            },
            order_type: "limit".to_string(),
            quantity: excess,
            price,
        };
        match execution::place_order(&deps.exec, to_exec_intent(leg, &balance, &origin, true)).await
        {
            Ok(_) => {
                log(
                    deps,
                    "info",
                    format!(
                        "策略 {} 补平：{} {} x {excess} @ {price}",
                        inst.id, leg.symbol, balance.side
                    ),
                );
            }
            Err(e) => {
                disable_instance(deps, inst, &format!("补平失败：{e}"));
                return;
            }
        }
    }
    // A clean signal round clears a stale last error.
    let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(entry) = stats.get_mut(&inst.id) {
        entry.last_error = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::EventBus;
    use crate::core::market::Instrument;
    use crate::core::types::{AccountLive, OrderBook, PriceLevel};
    use crate::data_engine::realtime::RealtimeEngine;
    use crate::market::catalog::MarketCatalog;
    use crate::market::sync::SyncManager;
    use crate::risk::{RiskConfig, RiskGateway};
    use crate::store::orders::OrderStore;
    use crate::strategy::testkit::{self, FakeStrategyAdapter};
    use std::collections::HashMap;

    fn live(account_id: &str, exchange_id: &str, now: i64) -> AccountLive {
        AccountLive {
            account_id: account_id.to_string(),
            adapter_id: exchange_id.to_string(),
            wallet_key: None,
            status: "connected".to_string(),
            latency_ms: Some(50),
            equity_usd: 10_000.0,
            available_usd: 10_000.0,
            pnl_usd: Some(0.0),
            currency: "USDC".to_string(),
            positions: vec![],
            error: None,
            ts: now,
        }
    }

    fn book(
        exchange_id: &str,
        symbol: &str,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        ts: i64,
    ) -> OrderBook {
        OrderBook {
            adapter_id: exchange_id.to_string(),
            symbol: symbol.to_string(),
            bids: bids.iter().map(|&(p, q)| PriceLevel(p, q)).collect(),
            asks: asks.iter().map(|&(p, q)| PriceLevel(p, q)).collect(),
            ts,
        }
    }

    /// Full runner harness: temp db with accounts + catalog rows,
    /// fake adapters on both legs, live states, and injected books
    /// with a wide (firing) spread. Returns the deps, the instance
    /// id, and the two fakes (for assertions).
    struct Harness {
        deps: RunnerDeps,
        inst_id: String,
        fake_a: std::sync::Arc<FakeStrategyAdapter>,
        fake_b: std::sync::Arc<FakeStrategyAdapter>,
        streams: Arc<RealtimeEngine>,
    }

    async fn harness(now: i64) -> Harness {
        let (db, _dir) = testkit::temp_db();
        let sync = Arc::new(SyncManager::new());
        let fake_a = Arc::new(FakeStrategyAdapter::new("hyperliquid"));
        let fake_b = Arc::new(FakeStrategyAdapter::new("entropy"));
        for (id, exchange, fake) in [
            ("acc-a", "hyperliquid", fake_a.clone()),
            ("acc-b", "entropy", fake_b.clone()),
        ] {
            db.insert_account(&testkit::account_row(id, exchange))
                .expect("insert account");
            sync.insert(id.to_string(), fake);
        }
        let saved =
            crate::strategy::save_strategy(&db, &sync, testkit::input()).expect("save instance");
        // Catalog rows for both legs (tick 1.0, step 0.001, min 10).
        for (exchange, symbol) in [("hyperliquid", "BTC"), ("entropy", "io:BTC")] {
            db.replace_instruments(
                exchange,
                &[Instrument {
                    exchange_id: exchange.to_string(),
                    symbol: symbol.to_string(),
                    tick_size: Some(1.0),
                    step_size: Some(0.001),
                    min_notional: Some(10.0),
                    max_leverage: Some(40.0),
                    funding_interval_h: Some(1.0),
                    settlement_currency: "USDC".to_string(),
                    market_id: None,
                    status: "trading".to_string(),
                    updated_at: now,
                }],
            )
            .expect("seed catalog");
        }
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::with_db(bus.clone(), db.clone()));
        let streams = Arc::new(RealtimeEngine::new(bus.clone(), catalog.clone(), None));
        // Live states: connected, fresh, flat.
        {
            let map = sync.live_map();
            let mut m = map.write().unwrap();
            m.insert("acc-a".to_string(), live("acc-a", "hyperliquid", now));
            m.insert("acc-b".to_string(), live("acc-b", "entropy", now));
        }
        // Wide books: A bid 91000 / B ask 90000 → fires.
        let books = [
            book(
                "hyperliquid",
                "BTC",
                &[(91000.0, 1.0)],
                &[(91010.0, 1.0)],
                now,
            ),
            book(
                "entropy",
                "io:BTC",
                &[(89990.0, 1.0)],
                &[(90000.0, 1.0)],
                now,
            ),
        ];
        for b in &books {
            streams.inject_book(&(b.adapter_id.clone(), b.symbol.clone()), b.clone());
        }
        let deps = RunnerDeps::new(
            ExecDeps {
                risk: Arc::new(RiskGateway::new(RiskConfig::default())),
                orders: Arc::new(OrderStore::new()),
                sync: sync.clone(),
                streams: streams.clone(),
                catalog: catalog.clone(),
                bus: bus.clone(),
            },
            db,
            Arc::new(Mutex::new(HashMap::new())),
        );
        Harness {
            deps,
            inst_id: saved.instance.id,
            fake_a,
            fake_b,
            streams,
        }
    }

    fn placed_accounts(fake: &FakeStrategyAdapter) -> Vec<String> {
        fake.placed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|r| r.account_id.clone())
            .collect()
    }

    #[tokio::test]
    async fn both_legs_placed_concurrently_with_strategy_origin() {
        let now = now_ms();
        let h = harness(now).await;
        run_round(&h.deps, now).await;
        // Both legs were placed (join: no guaranteed order).
        assert_eq!(placed_accounts(&h.fake_a), vec!["acc-a"]);
        assert_eq!(placed_accounts(&h.fake_b), vec!["acc-b"]);
        // The stored records carry the strategy origin.
        let records = h.deps.exec.orders.list();
        assert_eq!(records.len(), 2);
        assert!(records
            .iter()
            .all(|o| o.origin == format!("strategy:{}", h.inst_id)));
        // Opposite sides, same quantity.
        let a = records.iter().find(|o| o.account_id == "acc-a").unwrap();
        let b = records.iter().find(|o| o.account_id == "acc-b").unwrap();
        assert_eq!(a.side, "sell");
        assert_eq!(b.side, "buy");
        assert!((a.quantity - b.quantity).abs() < 1e-12);
        // No reduce-only orders, no cancels, instance stays enabled.
        assert!(h
            .fake_a
            .placed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .all(|r| !r.reduce_only));
        assert!(
            crate::strategy::load_strategies(&h.deps.db)
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .enabled
        );
    }

    #[tokio::test]
    async fn second_leg_failure_flattens_the_first_with_reduce_only() {
        let now = now_ms();
        let h = harness(now).await;
        // Leg B rejects; leg A fills in full.
        h.fake_b
            .fail_accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert("acc-b".to_string());
        h.fake_a
            .fill_fraction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert("acc-a".to_string(), 1.0);
        run_round(&h.deps, now).await;
        // Leg A placed twice: the spread sell, then a reduce-only buy
        // for the same quantity.
        let placed = h
            .fake_a
            .placed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(placed.len(), 2);
        assert_eq!(placed[0].side, "sell");
        assert!(!placed[0].reduce_only);
        assert_eq!(placed[1].side, "buy");
        assert!(placed[1].reduce_only);
        assert!((placed[1].quantity - placed[0].quantity).abs() < 1e-12);
        // Leg B placed once and failed; the instance stays enabled
        // (the flatten succeeded).
        assert_eq!(placed_accounts(&h.fake_b), vec!["acc-b"]);
        assert!(
            crate::strategy::load_strategies(&h.deps.db)
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .enabled
        );
    }

    #[tokio::test]
    async fn coin_mismatch_cancels_and_disables() {
        let now = now_ms();
        let h = harness(now).await;
        // Leg A's open-orders read reports the wrong coin.
        h.fake_a
            .wrong_coin_accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert("acc-a".to_string());
        run_round(&h.deps, now).await;
        // The mismatched order was canceled by cloid.
        let cancels = h
            .fake_a
            .cancels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(cancels.len(), 1);
        assert!(cancels[0].0.starts_with("0x"));
        assert_eq!(cancels[0].1, "BTC");
        // The instance is disabled and the error is recorded.
        let inst = crate::strategy::load_strategies(&h.deps.db)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert!(!inst.enabled);
        let stats = h.deps.stats.lock().unwrap_or_else(|e| e.into_inner());
        let st = stats.get(&h.inst_id).unwrap();
        assert!(st.last_error.as_deref().unwrap().contains("首单校验失败"));
    }

    #[tokio::test]
    async fn stale_book_skips_the_round_without_placing() {
        let now = now_ms();
        let h = harness(now).await;
        // Re-inject leg B's book 6s old: stale.
        let old = book(
            "entropy",
            "io:BTC",
            &[(89990.0, 1.0)],
            &[(90000.0, 1.0)],
            now - 6_000,
        );
        h.streams
            .inject_book(&("entropy".to_string(), "io:BTC".to_string()), old);
        run_round(&h.deps, now).await;
        assert!(placed_accounts(&h.fake_a).is_empty());
        assert!(placed_accounts(&h.fake_b).is_empty());
        // A second identical skip logs nothing new (same reason);
        // the run is still a skip, not a placement.
        run_round(&h.deps, now).await;
        assert!(placed_accounts(&h.fake_a).is_empty());
    }

    #[tokio::test]
    async fn partial_fill_balances_only_the_excess() {
        let now = now_ms();
        let h = harness(now).await;
        // A fills fully, B fills half: B's excess is zero, A's
        // excess is half the quantity → one reduce-only buy on A.
        h.fake_a
            .fill_fraction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert("acc-a".to_string(), 1.0);
        h.fake_b
            .fill_fraction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert("acc-b".to_string(), 0.5);
        run_round(&h.deps, now).await;
        let placed_a = h
            .fake_a
            .placed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(placed_a.len(), 2);
        assert!(placed_a[1].reduce_only);
        // The reduce quantity is half the spread quantity, aligned
        // down to the leg step (0.055 / 2 = 0.0275 → 0.027).
        assert!((placed_a[1].quantity - 0.027).abs() < 1e-12);
        // B placed once, no reduce on B.
        assert_eq!(
            h.fake_b
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            1
        );
    }
}
