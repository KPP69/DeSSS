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

use crate::core::types::{AccountLive, OrderBook, OrderInfo, OrderStatus};
use crate::core::util::now_ms;
use crate::data_engine::DataEngine;
use crate::execution::{self, ExecDeps, PlaceIntent as ExecIntent};

use super::spread::{self, BookSnapshot, MarketSpec, PlaceIntent as SpreadIntent, SpreadState};
use super::{load_strategies, set_strategy_enabled, LegRef, StrategyInstance, StrategyStatsMap};

/// A book older than this is stale for the runner (same 5s as the
/// evaluator's rule).
const BOOK_STALE_MS: i64 = 5_000;
/// A position snapshot older than this is stale (Phase 3.3: 3s —
///
/// under the account WS the snapshot is at most a 1s heartbeat old,
/// so anything older means the live feed is gone).
const POSITION_STALE_MS: i64 = 3_000;
/// A protection order younger than this is left alone by the cleanup
/// below: it may predate the position snapshot, which can lag a fresh
/// fill by up to one 10s poll. Split from POSITION_STALE_MS in Phase
/// 3.3 — the two numbers mean different things now.
const PROTECTION_STALE_MS: i64 = 15_000;
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
/// from `lib.rs` via `tauri::async_runtime::spawn`. An `interval`
/// (not sleep-after-round) keeps the period fixed at 200ms even when
/// a round runs slow; `Delay` drops missed ticks instead of bursting
/// to catch up.
pub async fn run(deps: RunnerDeps) {
    let mut tick = tokio::time::interval(Duration::from_millis(ROUND_MS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        run_round(&deps, now_ms()).await;
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

/// In-flight gate (D6): true while this instance still has a
/// non-terminal order on the books. `Unresolved` is deliberately
/// NOT in the set — it exists to be resolved by the 10s open-orders
/// poll, and blocking on it would stall the strategy on every
/// timeout.
fn has_inflight_order(deps: &RunnerDeps, inst_id: &str) -> bool {
    let origin = format!("strategy:{inst_id}");
    deps.exec.orders.list().iter().any(|o| {
        o.origin == origin
            // (3.1.2) resting protection (trigger) orders do not block
            // firing: they are exits, not entries. Without this, the
            // first protected position would deadlock the strategy —
            // it could never close or reopen while TP/SL rests.
            && o.trigger_px.is_none()
            && matches!(
                o.status,
                OrderStatus::Pending
                    | OrderStatus::Submitted
                    | OrderStatus::Open
                    | OrderStatus::PartiallyFilled
            )
    })
}

// (D7) first unresolved strategy order younger than 60s, with its age
// in seconds; older ones are most likely already Failed (10s poll x5).
fn fresh_unresolved(deps: &RunnerDeps, inst_id: &str, now: i64) -> Option<(String, u64)> {
    let origin = format!("strategy:{inst_id}");
    deps.exec.orders.list().iter().find_map(|o| {
        let age = now - o.updated_at;
        (o.origin == origin && o.status == OrderStatus::Unresolved && age < 60_000)
            .then(|| (o.client_order_id.clone(), (age.max(0) / 1000) as u64))
    })
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
        // Entry / balance intents only — protection orders are built
        // by place_protection, which sets the trigger fields.
        trigger_px: None,
        tpsl: None,
        is_market: None,
        origin: origin.to_string(),
    }
}

/// (3.1.2) Place the TP/SL pair for one leg after an opening
/// fill. `fill_px` is the leg's average fill price, `qty` the matched
/// quantity. A side is placed only when its bps > 0 and no strategy
/// trigger order with the same tpsl and (nearly) the same trigger
/// price is already open for the leg — placing twice would double
/// the protection. The orders are reduce-only, positionTpsl (the
/// adapter derives the grouping from the trigger variant), market
/// after trigger; the D8 stop-loss buffer is applied inside
/// place_order. Returns false when any protection order failed: the
/// position is then unprotected, which is logged and recorded, but
/// does not disable the instance (the position still needs managing).
#[allow(clippy::too_many_arguments)]
async fn place_protection(
    deps: &RunnerDeps,
    inst: &StrategyInstance,
    leg: &LegRef,
    spec: &MarketSpec,
    fill_px: f64,
    qty: f64,
    is_long: bool,
    origin: &str,
) -> bool {
    let mut ok = true;
    let (tp, sl) =
        spread::protection_triggers(fill_px, is_long, inst.params.tp_bps, inst.params.sl_bps);
    // The protection order closes the position: opposite side.
    let side = if is_long { "sell" } else { "buy" };
    for (trigger_px, tpsl) in [(tp, "tp"), (sl, "sl")] {
        let Some(trigger_px) = trigger_px else {
            continue;
        };
        let tol = spec.tick_size.max(trigger_px * 1e-9);
        let exists = deps.exec.orders.list().iter().any(|o| {
            o.origin == origin
                && o.account_id == leg.account_id
                && o.symbol == leg.symbol
                && o.tpsl.as_deref() == Some(tpsl)
                && !o.status.is_terminal()
                && o.trigger_px
                    .map(|t| (t - trigger_px).abs() <= tol)
                    .unwrap_or(false)
        });
        if exists {
            log(
                deps,
                "info",
                format!("策略 {} {} 腿已有 {tpsl} 保护单，跳过", inst.id, leg.symbol),
            );
            continue;
        }
        let intent = ExecIntent {
            account_id: leg.account_id.clone(),
            symbol: leg.symbol.clone(),
            side: side.to_string(),
            order_type: "trigger".to_string(),
            quantity: qty,
            price: trigger_px,
            reduce_only: true,
            trigger_px: Some(trigger_px),
            tpsl: Some(tpsl.to_string()),
            is_market: Some(true),
            origin: origin.to_string(),
        };
        match execution::place_order(&deps.exec, intent).await {
            Ok(info) => log(
                deps,
                "info",
                format!(
                    "策略 {} {} 腿挂 {tpsl} 保护单：trigger {trigger_px}（oid {}）",
                    inst.id, leg.symbol, info.order_id
                ),
            ),
            Err(e) => {
                ok = false;
                let msg = format!("{} 腿挂 {tpsl} 保护单失败：{e}", leg.symbol);
                {
                    let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(entry) = stats.get_mut(&inst.id) {
                        entry.last_error = Some(msg.clone());
                    }
                }
                log(deps, "error", format!("策略 {} {msg}", inst.id));
            }
        }
    }
    ok
}

/// (3.1.2, item 4 fallback) Cancel stale strategy protection: a leg
/// whose position is gone must not keep trigger orders — they could
/// fire against a later position. The exchange is reported to cancel
/// positionTpsl orders when the position closes, but the official
/// docs do not state it, so the runner cancels explicitly (belt and
/// braces; cancelling an already-gone order is a benign error,
/// logged, never fatal). Only orders older than PROTECTION_STALE_MS
/// are touched: a younger order may predate the position snapshot,
/// which can lag a fresh fill by up to one 10s poll.
async fn cleanup_protection(deps: &RunnerDeps, inst: &StrategyInstance, now: i64) {
    let origin = format!("strategy:{}", inst.id);
    for leg in [&inst.leg_a, &inst.leg_b] {
        let Some(live) = live_of(deps, &leg.account_id) else {
            continue;
        };
        let Some(spec) = spec_for(deps, leg) else {
            continue;
        };
        if net_qty(&live, &leg.symbol).abs() >= spec.step_size {
            continue;
        }
        for o in deps.exec.orders.list() {
            if o.origin != origin
                || o.account_id != leg.account_id
                || o.symbol != leg.symbol
                || o.trigger_px.is_none()
                || o.status.is_terminal()
                || now - o.created_at <= PROTECTION_STALE_MS
            {
                continue;
            }
            let Some(adapter) = deps.exec.sync.adapter(&leg.account_id) else {
                continue;
            };
            match adapter
                .cancel_order(&leg.account_id, &o.client_order_id, &leg.symbol)
                .await
            {
                Ok(()) => log(
                    deps,
                    "info",
                    format!(
                        "策略 {} 清除 {} 腿过期保护单 {}",
                        inst.id, leg.symbol, o.client_order_id
                    ),
                ),
                Err(e) => log(
                    deps,
                    "info",
                    format!(
                        "策略 {} 清除 {} 腿保护单 {} 失败（可能已撤销）：{e}",
                        inst.id, leg.symbol, o.client_order_id
                    ),
                ),
            }
        }
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
    deps.exec.streams.release_pinned(vec![
        (inst.leg_a.exchange_id.clone(), inst.leg_a.symbol.clone()),
        (inst.leg_b.exchange_id.clone(), inst.leg_b.symbol.clone()),
    ]);
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
    // (3.1.2) clear stale protection first, even on a stale-book
    // round: it needs no book, only the live position snapshot.
    cleanup_protection(deps, inst, now).await;
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
    // (b) position snapshots fresh (≤3s, Phase 3.3) and (c) neither
    // leg's exchange down / rate-limited. There is no separate
    // venue-health registry: the account's live status is the signal —
    // the adapter poll loop reports "error" when its REST fails,
    // including 429s — so "connected" is the gate. (Phase 3.3) A
    // degraded leg (WS down or silent) never opens a new position:
    // the snapshot is REST fallback, not live. Existing positions are
    // untouched — the cleanup above already ran, and only new fires
    // are gated here.
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
    if live_a.degraded {
        return skip(deps, inst, "A 腿账户数据降级，不开新仓");
    }
    if live_b.degraded {
        return skip(deps, inst, "B 腿账户数据降级，不开新仓");
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
    // (D6) in-flight gate: while this instance still has a
    // non-terminal order on the books, do not fire again — a resting
    // order must fill or cancel before the next signal.
    if has_inflight_order(deps, &inst.id) {
        return skip(deps, inst, "有在途策略单，等待成交或撤销");
    }
    // (D7) fresh Unresolved, bounded to 60s: it may already be filled.
    if let Some((cloid, age)) = fresh_unresolved(deps, &inst.id, now) {
        let msg = format!("等待未确认订单 {cloid}（已 {age} 秒）");
        return skip(deps, inst, &msg);
    }
    // (D7) clear AFTER the gate — before it, last_skip was wiped every round.
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
    // (3.1.2) protection: an opening fire (net was flat) that
    // matched on both legs gets a TP/SL pair per leg, from each leg's
    // average fill price. A closing fire gets none — its position is
    // going away. A leg without an average fill price is skipped.
    let mut protection_ok = true;
    if matched > 0.0 && net.abs() < spec_a.step_size {
        for (leg, intent, r, spec) in [
            (&inst.leg_a, ia, &ra, &spec_a),
            (&inst.leg_b, ib, &rb, &spec_b),
        ] {
            let Some(fill_px) = r.as_ref().ok().and_then(|i| i.average_fill_price) else {
                log(
                    deps,
                    "info",
                    format!("策略 {} {} 腿无成交价，跳过保护单", inst.id, leg.symbol),
                );
                continue;
            };
            if !place_protection(
                deps,
                inst,
                leg,
                spec,
                fill_px,
                matched,
                intent.side == "buy",
                &origin,
            )
            .await
            {
                protection_ok = false;
            }
        }
    }
    // A clean signal round clears a stale last error — but not when
    // protection just failed: that error is current, not stale.
    if protection_ok {
        let mut stats = deps.stats.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = stats.get_mut(&inst.id) {
            entry.last_error = None;
        }
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
            source: "rest".to_string(),
            degraded: false,
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

    /// Flip every still-open strategy order of one instance to a
    /// terminal status, as the exchange would after a fill / cancel.
    fn set_strategy_orders_status(deps: &RunnerDeps, inst_id: &str, status: OrderStatus) {
        let origin = format!("strategy:{inst_id}");
        for mut o in deps.exec.orders.list() {
            if o.origin == origin && !o.status.is_terminal() {
                o.status = status;
                deps.exec.orders.apply_query(&o);
            }
        }
    }

    /// Flip non-terminal strategy orders to Unresolved and backdate
    /// their update time by `age_ms` (D7: controls the gate's age cap).
    fn set_strategy_orders_unresolved_aged(deps: &RunnerDeps, inst_id: &str, age_ms: i64) {
        let origin = format!("strategy:{inst_id}");
        for mut o in deps.exec.orders.list() {
            if o.origin == origin && !o.status.is_terminal() {
                o.status = OrderStatus::Unresolved;
                o.updated_at -= age_ms;
                deps.exec.orders.apply_query(&o);
            }
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

    #[tokio::test]
    async fn inflight_order_blocks_the_next_fire() {
        let now = now_ms();
        let h = harness(now).await;
        // Orders rest Open (fraction 0): round 1 fires, round 2 must not.
        run_round(&h.deps, now).await;
        assert_eq!(
            h.fake_a
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            1
        );
        run_round(&h.deps, now).await;
        assert_eq!(
            h.fake_a
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            1,
            "a resting strategy order must block the next fire"
        );
        assert_eq!(
            h.fake_b
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            1
        );
        // The skip is logged (deduped by skip()).
        let skip = h
            .deps
            .stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&h.inst_id)
            .and_then(|s| s.last_skip.clone());
        assert!(skip.map(|s| s.contains("在途")).unwrap_or(false));
    }

    #[tokio::test]
    async fn filled_or_canceled_order_unblocks_firing() {
        let now = now_ms();
        let h = harness(now).await;
        run_round(&h.deps, now).await;
        assert_eq!(
            h.fake_a
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            1
        );
        // Filled is terminal: the gate must clear.
        set_strategy_orders_status(&h.deps, &h.inst_id, OrderStatus::Filled);
        run_round(&h.deps, now).await;
        assert_eq!(
            h.fake_a
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            2,
            "Filled must unblock the next fire"
        );
        // ... and Canceled too.
        set_strategy_orders_status(&h.deps, &h.inst_id, OrderStatus::Canceled);
        run_round(&h.deps, now).await;
        assert_eq!(
            h.fake_a
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            3,
            "Canceled must unblock the next fire"
        );
    }

    /// Re-save the harness instance with TP/SL bps set.
    fn set_protection(deps: &RunnerDeps, inst_id: &str, tp_bps: f64, sl_bps: f64) {
        let mut input = testkit::input();
        input.id = Some(inst_id.to_string());
        input.params.tp_bps = tp_bps;
        input.params.sl_bps = sl_bps;
        crate::strategy::save_strategy(&deps.db, &deps.exec.sync, input).expect("save with tp/sl");
    }

    fn fill_both(h: &Harness) {
        for (fake, acc) in [(&h.fake_a, "acc-a"), (&h.fake_b, "acc-b")] {
            fake.fill_fraction
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(acc.to_string(), 1.0);
        }
    }

    fn placed(fake: &FakeStrategyAdapter) -> Vec<crate::core::types::OrderRequest> {
        fake.placed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    #[tokio::test]
    async fn protection_placed_after_matched_opening_fill() {
        let now = now_ms();
        let h = harness(now).await;
        set_protection(&h.deps, &h.inst_id, 100.0, 200.0);
        fill_both(&h);
        run_round(&h.deps, now).await;
        // A fires sell (short) @ 90982, B fires buy (long) @ 90018;
        // each leg then gets a TP + SL pair, reduce-only, trigger,
        // market after trigger, origin strategy:<id>.
        let placed_a = placed(&h.fake_a);
        assert_eq!(placed_a.len(), 3, "entry + TP + SL on A");
        assert_eq!(placed_a[0].side, "sell");
        assert!(placed_a[0].trigger_px.is_none());
        for (req, tpsl, want_trigger, want_px) in [
            (&placed_a[1], "tp", 90072.0, 90072.0),
            // SL p is the D8 buffer: trigger 92801 x 1.03 -> 95585.
            (&placed_a[2], "sl", 92801.0, 95585.0),
        ] {
            assert_eq!(req.order_type, "trigger", "{tpsl}");
            assert!(req.reduce_only, "{tpsl} must be reduce-only");
            assert_eq!(req.is_market, Some(true), "{tpsl}");
            assert_eq!(req.tpsl.as_deref(), Some(tpsl));
            assert_eq!(req.side, "buy", "protection closes the short");
            assert_eq!(req.trigger_px, Some(want_trigger), "{tpsl} trigger");
            assert_eq!(req.price, Some(want_px), "{tpsl} limit price");
            assert!((req.quantity - 0.055).abs() < 1e-12, "matched qty");
        }
        let placed_b = placed(&h.fake_b);
        assert_eq!(placed_b.len(), 3, "entry + TP + SL on B");
        assert_eq!(placed_b[0].side, "buy");
        for (req, tpsl, want_trigger, want_px) in [
            (&placed_b[1], "tp", 90919.0, 90919.0),
            // SL p is the D8 buffer: trigger 88218 x 0.97 -> 85572.
            (&placed_b[2], "sl", 88218.0, 85572.0),
        ] {
            assert_eq!(req.order_type, "trigger", "{tpsl}");
            assert!(req.reduce_only, "{tpsl} must be reduce-only");
            assert_eq!(req.tpsl.as_deref(), Some(tpsl));
            assert_eq!(req.side, "sell", "protection closes the long");
            assert_eq!(req.trigger_px, Some(want_trigger), "{tpsl} trigger");
            assert_eq!(req.price, Some(want_px), "{tpsl} limit price");
        }
    }

    #[tokio::test]
    async fn protection_orders_do_not_block_the_next_fire() {
        let now = now_ms();
        let h = harness(now).await;
        set_protection(&h.deps, &h.inst_id, 100.0, 200.0);
        fill_both(&h);
        run_round(&h.deps, now).await;
        assert_eq!(placed(&h.fake_a).len(), 3);
        // Flip the books so the spread now favours BuyA: the open
        // short-A position may only close, and the close must fire
        // even though the protection orders are still resting.
        for b in [
            book(
                "hyperliquid",
                "BTC",
                &[(89990.0, 1.0)],
                &[(90000.0, 1.0)],
                now,
            ),
            book(
                "entropy",
                "io:BTC",
                &[(91000.0, 1.0)],
                &[(91010.0, 1.0)],
                now,
            ),
        ] {
            h.streams
                .inject_book(&(b.adapter_id.clone(), b.symbol.clone()), b);
        }
        run_round(&h.deps, now).await;
        let placed_a = placed(&h.fake_a);
        assert_eq!(placed_a.len(), 4, "the close fires past resting protection");
        assert_eq!(placed_a[3].side, "buy", "the close buys back the short A");
        assert_eq!(placed_a[3].order_type, "ioc", "the close is IOC too");
        // No new protection: a closing fire gets none.
        assert_eq!(placed(&h.fake_b).len(), 4);
        assert!(
            placed(&h.fake_a)
                .iter()
                .filter(|r| r.trigger_px.is_some())
                .count()
                == 2,
            "still only the round-1 TP/SL pair"
        );
    }

    #[tokio::test]
    async fn entry_orders_are_ioc() {
        let now = now_ms();
        let h = harness(now).await;
        run_round(&h.deps, now).await;
        // Both entry legs are IOC; the prices are unchanged (the
        // protection-bps worst acceptable price).
        let placed_a = placed(&h.fake_a);
        let placed_b = placed(&h.fake_b);
        assert_eq!(placed_a.len(), 1);
        assert_eq!(placed_b.len(), 1);
        assert_eq!(placed_a[0].order_type, "ioc");
        assert_eq!(placed_b[0].order_type, "ioc");
        assert_eq!(placed_a[0].price, Some(90982.0));
        assert_eq!(placed_b[0].price, Some(90018.0));
    }

    #[tokio::test]
    async fn partial_fill_still_matches_and_hedges() {
        let now = now_ms();
        let h = harness(now).await;
        set_protection(&h.deps, &h.inst_id, 100.0, 200.0);
        // Leg A fills in full, leg B half: matched is the smaller
        // fill, the A excess is hedged, protection covers matched.
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
        let placed_a = placed(&h.fake_a);
        let placed_b = placed(&h.fake_b);
        assert_eq!(placed_a[0].order_type, "ioc");
        assert_eq!(placed_b[0].order_type, "ioc");
        assert_eq!(placed_a.len(), 4, "entry + hedge + TP + SL on A");
        assert_eq!(placed_b.len(), 3, "entry + TP + SL on B");
        // The hedge is unchanged: a limit reduce-only order.
        // Quantities are floored to the step in the pipeline, so the
        // 0.0275 excess / matched become 0.027 (0.0005 sub-step dust).
        assert_eq!(placed_a[1].order_type, "limit");
        assert!(placed_a[1].reduce_only);
        assert_eq!(placed_a[1].side, "buy");
        assert!(
            (placed_a[1].quantity - 0.027).abs() < 1e-12,
            "hedged excess"
        );
        // Protection covers the matched 0.027 on both legs.
        for req in [&placed_a[2], &placed_a[3], &placed_b[1], &placed_b[2]] {
            assert_eq!(req.order_type, "trigger");
            assert!((req.quantity - 0.027).abs() < 1e-12, "matched qty");
        }
    }

    #[tokio::test]
    async fn no_fill_places_no_protection_and_keeps_evaluating() {
        let now = now_ms();
        let h = harness(now).await;
        set_protection(&h.deps, &h.inst_id, 100.0, 200.0);
        run_round(&h.deps, now).await;
        // Nothing filled: no protection, no hedge, instance stays enabled.
        let placed_a = placed(&h.fake_a);
        let placed_b = placed(&h.fake_b);
        assert_eq!(placed_a.len(), 1);
        assert_eq!(placed_b.len(), 1);
        assert_eq!(placed_a[0].order_type, "ioc");
        assert_eq!(placed_b[0].order_type, "ioc");
        assert!(placed_a.iter().all(|r| r.trigger_px.is_none()));
        assert!(placed_b.iter().all(|r| r.trigger_px.is_none()));
        assert!(instance_enabled(&h.deps, &h.inst_id));
        // The exchange cancels the unfilled IOC remainder; the next
        // round evaluates and fires again.
        set_strategy_orders_status(&h.deps, &h.inst_id, OrderStatus::Canceled);
        run_round(&h.deps, now).await;
        assert_eq!(placed(&h.fake_a).len(), 2, "round 2 fires again");
        assert_eq!(placed(&h.fake_b).len(), 2);
        assert!(instance_enabled(&h.deps, &h.inst_id));
    }

    fn instance_enabled(deps: &RunnerDeps, inst_id: &str) -> bool {
        crate::strategy::load_strategies(&deps.db)
            .expect("load")
            .into_iter()
            .find(|i| i.id == inst_id)
            .expect("instance")
            .enabled
    }

    #[tokio::test]
    async fn protection_not_placed_twice_for_the_same_fill() {
        let now = now_ms();
        let h = harness(now).await;
        set_protection(&h.deps, &h.inst_id, 100.0, 200.0);
        fill_both(&h);
        let inst = load_strategies(&h.deps.db)
            .expect("load")
            .into_iter()
            .find(|i| i.id == h.inst_id)
            .expect("instance");
        let spec = spec_for(&h.deps, &inst.leg_a).expect("spec");
        let origin = format!("strategy:{}", h.inst_id);
        // Same fill, twice: the second call must find the first
        // call's orders in the store and skip.
        assert!(
            place_protection(
                &h.deps,
                &inst,
                &inst.leg_a,
                &spec,
                90982.0,
                0.055,
                false,
                &origin
            )
            .await
        );
        assert_eq!(placed(&h.fake_a).len(), 2, "TP + SL placed once");
        assert!(
            place_protection(
                &h.deps,
                &inst,
                &inst.leg_a,
                &spec,
                90982.0,
                0.055,
                false,
                &origin
            )
            .await
        );
        assert_eq!(
            placed(&h.fake_a).len(),
            2,
            "the second call must not duplicate the pair"
        );
    }

    #[tokio::test]
    async fn stale_protection_cancelled_when_position_gone() {
        let now = now_ms();
        let h = harness(now).await;
        set_protection(&h.deps, &h.inst_id, 100.0, 200.0);
        fill_both(&h);
        run_round(&h.deps, now).await;
        assert_eq!(placed(&h.fake_a).len(), 3);
        // Close the position (flip the books to the BuyA side) so
        // the live snapshot goes flat again.
        for b in [
            book(
                "hyperliquid",
                "BTC",
                &[(89990.0, 1.0)],
                &[(90000.0, 1.0)],
                now,
            ),
            book(
                "entropy",
                "io:BTC",
                &[(91000.0, 1.0)],
                &[(91010.0, 1.0)],
                now,
            ),
        ] {
            h.streams
                .inject_book(&(b.adapter_id.clone(), b.symbol.clone()), b);
        }
        run_round(&h.deps, now).await;
        assert_eq!(placed(&h.fake_a).len(), 4, "the close fired");
        // 20s later the books are stale, but cleanup runs first and
        // cancels the now-stale protection orders (the exchange is
        // reported to do this itself; the runner does it explicitly).
        run_round(&h.deps, now + 20_000).await;
        let cancels_a = h
            .fake_a
            .cancels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(cancels_a.len(), 2, "both stale protection orders on A");
        let cancels_b = h
            .fake_b
            .cancels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(cancels_b.len(), 2, "both stale protection orders on B");
        // The cancelled ids are the protection orders' cloids, not
        // the (Filled, terminal) entries'.
        let origin = format!("strategy:{}", h.inst_id);
        let protection_cloids: Vec<String> = h
            .deps
            .exec
            .orders
            .list()
            .iter()
            .filter(|o| o.origin == origin && o.trigger_px.is_some())
            .map(|o| o.client_order_id.clone())
            .collect();
        assert_eq!(protection_cloids.len(), 4);
        for (cloid, _) in cancels_a.iter().chain(cancels_b.iter()) {
            assert!(
                protection_cloids.contains(cloid),
                "{cloid} is a protection order"
            );
        }
    }

    #[tokio::test]
    async fn fresh_unresolved_blocks_the_next_fire() {
        let now = now_ms();
        let h = harness(now).await;
        run_round(&h.deps, now).await;
        assert_eq!(placed_accounts(&h.fake_a).len(), 1);
        set_strategy_orders_status(&h.deps, &h.inst_id, OrderStatus::Unresolved);
        run_round(&h.deps, now).await;
        assert_eq!(
            placed_accounts(&h.fake_a).len(),
            1,
            "a fresh Unresolved must block the next fire"
        );
        // The skip names the order and its age.
        let skip = h
            .deps
            .stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&h.inst_id)
            .and_then(|s| s.last_skip.clone())
            .unwrap_or_default();
        assert!(skip.contains("等待未确认订单"), "skip was: {skip}");
        assert!(skip.contains("（已 0 秒）"), "skip was: {skip}");
        // A third blocked round adds no second log line (same content).
        run_round(&h.deps, now).await;
        let n = h
            .deps
            .exec
            .bus
            .snapshot_logs()
            .iter()
            .filter(|l| l.message.contains("等待未确认订单"))
            .count();
        assert_eq!(n, 1, "the same Unresolved block must log once");
    }

    #[tokio::test]
    async fn aged_unresolved_does_not_block() {
        let now = now_ms();
        let h = harness(now).await;
        run_round(&h.deps, now).await;
        assert_eq!(placed_accounts(&h.fake_a).len(), 1);
        // 61s old: past the 60s cap, so the gate must let the round run.
        set_strategy_orders_unresolved_aged(&h.deps, &h.inst_id, 61_000);
        // A later `now` proves round 2 evaluated (last_eval_ms moves).
        run_round(&h.deps, now + 1_000).await;
        let entry = h
            .deps
            .stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&h.inst_id)
            .expect("stats entry")
            .clone();
        assert_eq!(entry.last_eval_ms, Some(now + 1_000));
        assert!(!entry
            .last_skip
            .unwrap_or_default()
            .contains("等待未确认订单"));
        // The identical order is still not resubmitted — that is the
        // execution duplicate guard holding the line, not the gate.
        assert_eq!(placed_accounts(&h.fake_a).len(), 1);
    }

    #[tokio::test]
    async fn unresolved_reconciled_to_failed_unblocks_immediately() {
        let now = now_ms();
        let h = harness(now).await;
        run_round(&h.deps, now).await;
        assert_eq!(placed_accounts(&h.fake_a).len(), 1);
        set_strategy_orders_status(&h.deps, &h.inst_id, OrderStatus::Unresolved);
        run_round(&h.deps, now).await;
        assert_eq!(
            placed_accounts(&h.fake_a).len(),
            1,
            "fresh Unresolved must block"
        );
        // Reconciliation flips it to Failed (terminal): the very next
        // round must fire again.
        set_strategy_orders_status(&h.deps, &h.inst_id, OrderStatus::Failed);
        run_round(&h.deps, now).await;
        assert_eq!(
            placed_accounts(&h.fake_a).len(),
            2,
            "Failed must unblock the next fire"
        );
    }

    #[tokio::test]
    async fn interval_rounds_stay_within_tolerance() {
        let now = now_ms();
        let h = harness(now).await;
        // Orders fill immediately, so the in-flight gate clears every
        // round and evaluate runs again (round 2+ is then blocked
        // from *firing* by the position state machine — already
        // short A — but last_eval_ms still updates each round).
        h.fake_a
            .fill_fraction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert("acc-a".to_string(), 1.0);
        h.fake_b
            .fill_fraction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert("acc-b".to_string(), 1.0);
        let stats = h.deps.stats.clone();
        tokio::spawn(crate::strategy::runner::run(h.deps));
        // Observe rounds via last_eval_ms, which the runner updates
        // on every evaluation — even when the position state machine
        // blocks a re-fire after round 1 fills, so this measures the
        // loop period, not the firing rate. Real time with the spec's
        // ±50ms bound (no paused clock: advance() interacts badly
        // with the engine's 100ms first-frame poll sleeps).
        let mut seen: Vec<std::time::Instant> = Vec::new();
        let mut last_seen: Option<i64> = None;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while seen.len() < 5 && std::time::Instant::now() < deadline {
            let cur = stats
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .next()
                .and_then(|s| s.last_eval_ms);
            if cur.is_some() && cur != last_seen {
                last_seen = cur;
                seen.push(std::time::Instant::now());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(seen.len(), 5, "only {} rounds in 3s", seen.len());
        for w in seen.windows(2) {
            let gap = w[1].duration_since(w[0]);
            assert!(
                gap >= Duration::from_millis(150) && gap <= Duration::from_millis(250),
                "round gap {gap:?} outside 200ms ±50ms"
            );
        }
    }

    /// Flip one leg's degraded flag in the harness live map.
    fn set_live_degraded(deps: &RunnerDeps, account_id: &str, degraded: bool) {
        let map = deps.exec.sync.live_map();
        let mut m = map.write().unwrap();
        m.get_mut(account_id).unwrap().degraded = degraded;
    }

    /// Move one leg's live timestamp (freshness is `now - ts`).
    fn set_live_ts(deps: &RunnerDeps, account_id: &str, ts: i64) {
        let map = deps.exec.sync.live_map();
        let mut m = map.write().unwrap();
        m.get_mut(account_id).unwrap().ts = ts;
    }

    fn last_skip(deps: &RunnerDeps, inst_id: &str) -> String {
        deps.stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(inst_id)
            .and_then(|s| s.last_skip.clone())
            .unwrap_or_default()
    }

    /// Phase 3.3 §10: a degraded leg blocks new fires — the snapshot
    /// is REST fallback, not live — but the instance stays enabled,
    /// and recovery lets the next round fire again.
    #[tokio::test]
    async fn degraded_leg_blocks_new_fire_but_keeps_instance_enabled() {
        let now = now_ms();
        let h = harness(now).await;
        set_live_degraded(&h.deps, "acc-a", true);
        run_round(&h.deps, now).await;
        assert!(placed(&h.fake_a).is_empty(), "degraded leg must not fire");
        assert!(placed(&h.fake_b).is_empty());
        assert!(instance_enabled(&h.deps, &h.inst_id));
        let skip = last_skip(&h.deps, &h.inst_id);
        assert!(skip.contains("数据降级"), "skip was: {skip}");
        // Recovery: the next round fires again.
        set_live_degraded(&h.deps, "acc-a", false);
        run_round(&h.deps, now).await;
        assert_eq!(placed(&h.fake_a).len(), 1);
    }

    /// Phase 3.3 §10: the position-freshness rule is 3s now (was 15s).
    #[tokio::test]
    async fn position_snapshot_older_than_3s_skips() {
        let now = now_ms();
        let h = harness(now).await;
        set_live_ts(&h.deps, "acc-a", now - 4_000);
        set_live_ts(&h.deps, "acc-b", now - 4_000);
        run_round(&h.deps, now).await;
        assert!(
            placed(&h.fake_a).is_empty(),
            "4s-old snapshot must not fire"
        );
        set_live_ts(&h.deps, "acc-a", now - 2_000);
        set_live_ts(&h.deps, "acc-b", now - 2_000);
        run_round(&h.deps, now).await;
        assert_eq!(placed(&h.fake_a).len(), 1, "2s-old snapshot must fire");
    }
}
