//! Strategy module (Phase 3.1, commit 1): mean-reversion spread
//! strategies. Instances persist in the `settings` table under the
//! `strategies` key (a JSON array) — no new table. The runtime state
//! (armed spread state, per-instance stats) lives only in memory and
//! resets on restart. Only one strategy kind exists today ("spread");
//! the `kind` field is there so a second kind can be added without
//! changing the storage shape.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::core::util::now_ms;
use crate::data_engine::DataEngine;
use crate::market::sync::SyncManager;
use crate::store::orders::OrderStore;

pub mod runner;
pub mod spread;

/// One leg of a spread: which account, on which exchange, which
/// symbol. The two legs carry their own account ids — Hyperliquid
/// main and Entropy are two account rows (same wallet).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LegRef {
    pub account_id: String,
    pub exchange_id: String,
    pub symbol: String,
}

/// Mean-reversion spread parameters (all bps unless noted).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SpreadParams {
    pub midline_bps: f64,
    pub upper_bps: f64,
    pub lower_bps: f64,
    pub persist_sec: f64,
    pub take_fraction: f64,
    pub max_notional_usd: f64,
    pub fee_bps_a: f64,
    pub fee_bps_b: f64,
    /// Take-profit / stop-loss distance from the fill price, in bps.
    /// 0 = disabled (no protection order for that side).
    pub tp_bps: f64,
    pub sl_bps: f64,
}

/// One persisted strategy instance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StrategyInstance {
    pub id: String,
    pub kind: String,
    pub leg_a: LegRef,
    pub leg_b: LegRef,
    pub params: SpreadParams,
    pub enabled: bool,
    pub created_at: i64,
}

/// What the frontend sends to save (create or replace) an instance.
/// `id: None` creates one; `Some` replaces the instance with that id
/// (or inserts it when it does not exist yet).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveStrategyInput {
    pub id: Option<String>,
    pub kind: String,
    pub leg_a: LegRef,
    pub leg_b: LegRef,
    pub params: SpreadParams,
    pub enabled: bool,
}

/// The save result: the stored instance, plus the midline warning
/// when it applies (a warning, not an error — the save succeeds).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveStrategyResult {
    pub instance: StrategyInstance,
    pub warning: Option<String>,
}

/// Runtime stats for one instance (memory only, restart resets).
/// `last_skip` is the last round-skip reason, kept so the runner logs
/// a skip only when the reason changes; it is never sent to the
/// frontend.
#[derive(Clone, Debug, Default)]
pub struct StrategyStats {
    pub last_eval_ms: Option<i64>,
    pub last_error: Option<String>,
    pub last_skip: Option<String>,
}

pub type StrategyStatsMap = Arc<Mutex<HashMap<String, StrategyStats>>>;

/// One instance as the Strategy page sees it: the persisted fields
/// plus the runtime stats and the order count (orders whose origin is
/// `strategy:<id>`).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyView {
    pub id: String,
    pub kind: String,
    pub leg_a: LegRef,
    pub leg_b: LegRef,
    pub params: SpreadParams,
    pub enabled: bool,
    pub created_at: i64,
    pub last_eval_ms: Option<i64>,
    pub last_error: Option<String>,
    pub orders_placed: usize,
}

const SETTINGS_KEY: &str = "strategies";

/// All persisted instances (empty when nothing was saved yet).
pub fn load_strategies(db: &DataEngine) -> Result<Vec<StrategyInstance>, String> {
    match db.get_setting(SETTINGS_KEY)? {
        None => Ok(Vec::new()),
        Some(raw) => serde_json::from_str(&raw).map_err(|e| format!("策略配置损坏，无法解析：{e}")),
    }
}

fn store_strategies(db: &DataEngine, list: &[StrategyInstance]) -> Result<(), String> {
    let raw = serde_json::to_string(list).map_err(|e| format!("策略配置序列化失败：{e}"))?;
    db.set_setting(SETTINGS_KEY, &raw)
}

fn validate_params(p: &SpreadParams) -> Result<(), String> {
    for (v, name) in [
        (p.midline_bps, "midline_bps"),
        (p.upper_bps, "upper_bps"),
        (p.lower_bps, "lower_bps"),
        (p.persist_sec, "persist_sec"),
        (p.take_fraction, "take_fraction"),
        (p.max_notional_usd, "max_notional_usd"),
        (p.fee_bps_a, "fee_bps_a"),
        (p.fee_bps_b, "fee_bps_b"),
    ] {
        if !v.is_finite() {
            return Err(format!("{name} 必须是有限数值"));
        }
    }
    if p.upper_bps <= 0.0 {
        return Err("upper_bps 必须大于 0".to_string());
    }
    if p.lower_bps <= 0.0 {
        return Err("lower_bps 必须大于 0".to_string());
    }
    if p.persist_sec < 0.0 {
        return Err("persist_sec 不能小于 0".to_string());
    }
    if !(p.take_fraction > 0.0 && p.take_fraction <= 1.0) {
        return Err("take_fraction 必须在 (0, 1] 之间".to_string());
    }
    if p.max_notional_usd <= 0.0 {
        return Err("max_notional_usd 必须大于 0".to_string());
    }
    if p.fee_bps_a < 0.0 || p.fee_bps_b < 0.0 {
        return Err("费率不能小于 0".to_string());
    }
    if p.tp_bps < 0.0 || p.sl_bps < 0.0 {
        return Err("止盈/止损 bps 不能小于 0（0 表示不挂）".to_string());
    }
    Ok(())
}

fn validate_legs(
    sync: &SyncManager,
    db: &DataEngine,
    a: &LegRef,
    b: &LegRef,
) -> Result<(), String> {
    if a.account_id == b.account_id {
        return Err("两条腿不能是同一个账户".to_string());
    }
    for (leg, name) in [(a, "A"), (b, "B")] {
        let Some(venue) = crate::venues::all()
            .iter()
            .find(|v| v.id() == leg.exchange_id.as_str())
        else {
            return Err(format!("{name} 腿：未知交易所 {}", leg.exchange_id));
        };
        if !venue.supports_trading() {
            return Err(format!("{name} 腿：{} 暂不支持下单", venue.display_name()));
        }
        let row = db
            .get_account(&leg.account_id)?
            .ok_or_else(|| format!("{name} 腿：账户不存在"))?;
        if row.exchange_id != leg.exchange_id {
            return Err(format!("{name} 腿：账户的交易所与腿不符"));
        }
        if !sync.is_running(&leg.account_id) {
            return Err(format!("{name} 腿：账户未连接，请先在账户中心连接"));
        }
    }
    Ok(())
}

/// Create or replace one instance, after validation. The midline
/// warning (midline 0 with non-zero fees) does not reject the save.
pub fn save_strategy(
    db: &DataEngine,
    sync: &SyncManager,
    input: SaveStrategyInput,
) -> Result<SaveStrategyResult, String> {
    if input.kind != "spread" {
        return Err(format!("未知策略类型：{}", input.kind));
    }
    validate_params(&input.params)?;
    validate_legs(sync, db, &input.leg_a, &input.leg_b)?;
    let mut list = load_strategies(db)?;
    let now = now_ms();
    let (id, created_at) = match &input.id {
        Some(id) if !id.is_empty() => {
            let created = list
                .iter()
                .find(|s| &s.id == id)
                .map(|s| s.created_at)
                .unwrap_or(now);
            (id.clone(), created)
        }
        _ => (uuid::Uuid::new_v4().to_string(), now),
    };
    let warning = if input.params.midline_bps == 0.0
        && (input.params.fee_bps_a + input.params.fee_bps_b) > 0.0
    {
        Some("midline 未标定，建议先录数据校准".to_string())
    } else {
        None
    };
    let instance = StrategyInstance {
        id,
        kind: input.kind,
        leg_a: input.leg_a,
        leg_b: input.leg_b,
        params: input.params,
        enabled: input.enabled,
        created_at,
    };
    match list.iter_mut().find(|s| s.id == instance.id) {
        Some(slot) => *slot = instance.clone(),
        None => list.push(instance.clone()),
    }
    store_strategies(db, &list)?;
    Ok(SaveStrategyResult { instance, warning })
}

/// Flip one instance's enabled flag (what the runner's disable path
/// and the page's启停 button both call).
pub fn set_strategy_enabled(
    db: &DataEngine,
    id: &str,
    enabled: bool,
) -> Result<StrategyInstance, String> {
    let mut list = load_strategies(db)?;
    let Some(slot) = list.iter_mut().find(|s| s.id == id) else {
        return Err(format!("策略不存在：{id}"));
    };
    slot.enabled = enabled;
    let out = slot.clone();
    store_strategies(db, &list)?;
    Ok(out)
}

/// Instances with their runtime stats, for the Strategy page. The
/// order count is read from the order store by origin, so it stays
/// exact even when the runner restarts mid-session.
pub fn list_strategy_views(
    db: &DataEngine,
    orders: &OrderStore,
    stats: &StrategyStatsMap,
) -> Result<Vec<StrategyView>, String> {
    let list = load_strategies(db)?;
    let all_orders = orders.list();
    let stats = stats.lock().unwrap_or_else(|e| e.into_inner());
    Ok(list
        .into_iter()
        .map(|s| {
            let origin = format!("strategy:{}", s.id);
            let orders_placed = all_orders.iter().filter(|o| o.origin == origin).count();
            let st = stats.get(&s.id);
            StrategyView {
                id: s.id.clone(),
                kind: s.kind.clone(),
                leg_a: s.leg_a.clone(),
                leg_b: s.leg_b.clone(),
                params: s.params.clone(),
                enabled: s.enabled,
                created_at: s.created_at,
                last_eval_ms: st.and_then(|x| x.last_eval_ms),
                last_error: st.and_then(|x| x.last_error.clone()),
                orders_placed,
            }
        })
        .collect())
}

#[cfg(test)]
pub(crate) mod testkit {
    //! One configurable fake adapter shared by the strategy tests:
    //! it records placements / cancels, answers open-orders and
    //! cloid queries from what was placed, and can be told to fail a
    //! leg, report a wrong coin, or fill a fraction of each order.
    //! No network, no real exchange.

    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::{LegRef, SaveStrategyInput, SpreadParams};
    use crate::core::error::{VenueError, VenueResult};
    use crate::core::types::{Balance, Order, OrderInfo, OrderRequest, OrderStatus, Position};
    use crate::venues::ExchangeAdapter;

    pub struct FakeStrategyAdapter {
        pub id: String,
        pub placed: Mutex<Vec<OrderRequest>>,
        pub cancels: Mutex<Vec<(String, String)>>,
        pub fail_accounts: Mutex<HashSet<String>>,
        pub wrong_coin_accounts: Mutex<HashSet<String>>,
        /// account_id -> fraction of the placed quantity that fills
        /// (0 = resting Open, 1 = fully Filled, between = partial).
        pub fill_fraction: Mutex<HashMap<String, f64>>,
        /// Next exchange order id. D10: the execution ledger dedups
        /// fills by oid, so a double that hands every order the same
        /// oid would make a close look like a replay of the entry.
        /// Real oids are unique per order; the fake is too.
        next_oid: Mutex<u64>,
    }

    impl FakeStrategyAdapter {
        pub fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                placed: Mutex::new(Vec::new()),
                cancels: Mutex::new(Vec::new()),
                fail_accounts: Mutex::new(HashSet::new()),
                wrong_coin_accounts: Mutex::new(HashSet::new()),
                fill_fraction: Mutex::new(HashMap::new()),
                next_oid: Mutex::new(1001),
            }
        }

        fn fraction(&self, account_id: &str) -> f64 {
            self.fill_fraction
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(account_id)
                .copied()
                .unwrap_or(0.0)
        }

        fn placed_req(&self, cloid: &str) -> Option<OrderRequest> {
            self.placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .find(|r| r.client_order_id.as_deref() == Some(cloid))
                .cloned()
        }
    }

    #[async_trait]
    impl ExchangeAdapter for FakeStrategyAdapter {
        fn id(&self) -> &str {
            &self.id
        }
        fn name(&self) -> &str {
            "Fake Strategy Venue"
        }
        async fn connect(&self) -> VenueResult<()> {
            Ok(())
        }
        async fn disconnect(&self) -> VenueResult<()> {
            Ok(())
        }
        async fn subscribe_ticks(&self, _symbols: Vec<String>) -> VenueResult<()> {
            Ok(())
        }
        async fn unsubscribe_ticks(&self, _symbols: Vec<String>) -> VenueResult<()> {
            Ok(())
        }
        async fn get_balances(&self, _account_id: &str) -> VenueResult<Vec<Balance>> {
            Ok(vec![])
        }
        async fn get_positions(&self, _account_id: &str) -> VenueResult<Vec<Position>> {
            Ok(vec![])
        }
        async fn get_open_orders(&self, account_id: &str) -> VenueResult<Vec<Order>> {
            let wrong = self
                .wrong_coin_accounts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(account_id);
            Ok(self
                .placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .filter(|r| r.account_id == account_id)
                .map(|r| Order {
                    order_id: "1001".to_string(),
                    client_order_id: r.client_order_id.clone().unwrap_or_default(),
                    exchange_id: r.exchange_id.clone(),
                    account_id: r.account_id.clone(),
                    symbol: if wrong {
                        "ETH".to_string()
                    } else {
                        r.symbol.clone()
                    },
                    side: r.side.clone(),
                    order_type: r.order_type.clone(),
                    quantity: r.quantity,
                    price: r.price,
                    status: OrderStatus::Open,
                    ts: 0,
                    trigger_px: r.trigger_px,
                    tpsl: r.tpsl.clone(),
                })
                .collect())
        }
        async fn place_order(&self, req: OrderRequest) -> VenueResult<Order> {
            // Record the attempt before the outcome: tests assert
            // that a rejected leg was still attempted.
            self.placed
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(req.clone());
            if self
                .fail_accounts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&req.account_id)
            {
                return Err(VenueError::rejected("模拟拒单".to_string()));
            }
            let fraction = self.fraction(&req.account_id);
            // A fill is reported by the return itself (D10): the
            // Filled order carries the filled size in quantity, so
            // the engine applies it without a query. A resting order
            // stays Open. A trigger order always rests on placement
            // — it never fills at once, whatever the fraction says
            // (filling one would also corrupt the live position
            // snapshot via the reconcile path).
            let status = if req.trigger_px.is_some() {
                OrderStatus::Open
            } else if fraction > 0.0 {
                OrderStatus::Filled
            } else {
                OrderStatus::Open
            };
            // D10: a Filled return carries the FILL in quantity
            // (filled size, not the request) — a partial fill
            // reports only what filled. A resting order keeps the
            // requested qty.
            let quantity = if status == OrderStatus::Filled {
                req.quantity * fraction
            } else {
                req.quantity
            };
            let oid = {
                let mut n = self.next_oid.lock().unwrap_or_else(|e| e.into_inner());
                let oid = *n;
                *n += 1;
                oid
            };
            Ok(Order {
                order_id: oid.to_string(),
                client_order_id: req.client_order_id.clone().unwrap_or_default(),
                exchange_id: req.exchange_id.clone(),
                account_id: req.account_id.clone(),
                symbol: req.symbol.clone(),
                side: req.side.clone(),
                order_type: req.order_type.clone(),
                quantity,
                price: req.price,
                status,
                ts: 0,
                trigger_px: req.trigger_px,
                tpsl: req.tpsl.clone(),
            })
        }
        async fn cancel_order(
            &self,
            account_id: &str,
            order_id: &str,
            symbol: &str,
        ) -> VenueResult<()> {
            self.cancels
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((order_id.to_string(), symbol.to_string()));
            let _ = account_id;
            Ok(())
        }
        async fn query_order_by_cloid(
            &self,
            account_id: &str,
            cloid: &str,
        ) -> VenueResult<OrderInfo> {
            let Some(req) = self.placed_req(cloid) else {
                return Err(VenueError::rejected(
                    crate::core::error::UNKNOWN_OID.to_string(),
                ));
            };
            let fraction = self.fraction(account_id);
            let filled = if req.trigger_px.is_some() {
                0.0
            } else {
                req.quantity * fraction
            };
            let status = if req.trigger_px.is_some() {
                OrderStatus::Open
            } else if fraction >= 1.0 {
                OrderStatus::Filled
            } else if fraction > 0.0 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Open
            };
            Ok(OrderInfo {
                order_id: "1001".to_string(),
                client_order_id: cloid.to_string(),
                exchange_id: req.exchange_id.clone(),
                account_id: req.account_id.clone(),
                symbol: req.symbol.clone(),
                side: req.side.clone(),
                order_type: req.order_type.clone(),
                status,
                price: req.price,
                quantity: req.quantity,
                filled_quantity: filled,
                remaining_quantity: req.quantity - filled,
                average_fill_price: req.price,
                error: None,
                trigger_px: req.trigger_px,
                tpsl: req.tpsl.clone(),
                origin: "manual".to_string(),
                created_at: 0,
                updated_at: 0,
            })
        }
        async fn query_order_by_oid(&self, _account_id: &str, _oid: u64) -> VenueResult<OrderInfo> {
            Err(VenueError::rejected(
                crate::core::error::UNKNOWN_OID.to_string(),
            ))
        }
    }

    /// A temp SQLite db for the strategy store tests (its own temp
    /// dir; the caller drops the dir when done, or leaves it to the
    /// OS temp cleaner).
    pub fn temp_db() -> (
        std::sync::Arc<crate::data_engine::DataEngine>,
        std::path::PathBuf,
    ) {
        let root =
            std::env::temp_dir().join(format!("desss-strategy-test-{}", uuid::Uuid::new_v4()));
        let data = root.join("data");
        let paths = crate::portable::PortablePaths {
            root: root.clone(),
            data_dir: data.clone(),
            db_path: data.join("desss.db"),
            log_dir: data.join("logs"),
            vault_key_path: data.join("vault.key"),
            vault_enc_path: data.join("vault.enc"),
        };
        let db =
            std::sync::Arc::new(crate::data_engine::DataEngine::open(paths).expect("open temp db"));
        (db, root)
    }

    pub fn account_row(id: &str, exchange_id: &str) -> crate::store::accounts::AccountRow {
        crate::store::accounts::AccountRow {
            id: id.to_string(),
            label: id.to_string(),
            exchange_id: exchange_id.to_string(),
            masked_identifier: "0x…".to_string(),
            created_at: 0,
            updated_at: 0,
            status: "connected".to_string(),
            credential_configured: true,
        }
    }

    pub fn params() -> SpreadParams {
        SpreadParams {
            midline_bps: 0.0,
            upper_bps: 10.0,
            lower_bps: 10.0,
            persist_sec: 0.0,
            take_fraction: 1.0,
            max_notional_usd: 5000.0,
            fee_bps_a: 0.0,
            fee_bps_b: 0.0,
            tp_bps: 0.0,
            sl_bps: 0.0,
        }
    }

    pub fn input() -> SaveStrategyInput {
        SaveStrategyInput {
            id: None,
            kind: "spread".to_string(),
            leg_a: LegRef {
                account_id: "acc-a".to_string(),
                exchange_id: "hyperliquid".to_string(),
                symbol: "BTC".to_string(),
            },
            leg_b: LegRef {
                account_id: "acc-b".to_string(),
                exchange_id: "entropy".to_string(),
                symbol: "io:BTC".to_string(),
            },
            params: params(),
            enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::sync::SyncManager;
    use std::sync::Arc;

    fn connected_sync(db: &DataEngine) -> Arc<SyncManager> {
        let sync = Arc::new(SyncManager::new());
        for (id, exchange) in [("acc-a", "hyperliquid"), ("acc-b", "entropy")] {
            db.insert_account(&testkit::account_row(id, exchange))
                .expect("insert account");
            sync.insert(
                id.to_string(),
                Arc::new(testkit::FakeStrategyAdapter::new(exchange)),
            );
        }
        sync
    }

    #[test]
    fn save_round_trips_and_generates_id() {
        let (db, _dir) = testkit::temp_db();
        let sync = connected_sync(&db);
        let saved = save_strategy(&db, &sync, testkit::input()).expect("save");
        assert!(!saved.instance.id.is_empty());
        assert!(saved.warning.is_none());
        assert_eq!(saved.instance.kind, "spread");
        assert!(saved.instance.enabled);
        let list = load_strategies(&db).expect("load");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], saved.instance);
        // Saving the same id again replaces, never duplicates.
        let mut again = testkit::input();
        again.id = Some(saved.instance.id.clone());
        again.params.upper_bps = 25.0;
        let resaved = save_strategy(&db, &sync, again).expect("resave");
        assert_eq!(resaved.instance.id, saved.instance.id);
        assert_eq!(resaved.instance.params.upper_bps, 25.0);
        assert_eq!(load_strategies(&db).unwrap().len(), 1);
    }

    #[test]
    fn save_rejects_bad_params() {
        let (db, _dir) = testkit::temp_db();
        let sync = connected_sync(&db);
        // One mutation per illegal parameter (fn pointers so the
        // cases share one type).
        type ParamCase = (fn(&mut SpreadParams), &'static str);
        let cases: [ParamCase; 9] = [
            (|p: &mut SpreadParams| p.upper_bps = 0.0, "upper 0"),
            (|p: &mut SpreadParams| p.lower_bps = -1.0, "lower -1"),
            (|p: &mut SpreadParams| p.persist_sec = -1.0, "persist -1"),
            (|p: &mut SpreadParams| p.take_fraction = 0.0, "take 0"),
            (|p: &mut SpreadParams| p.take_fraction = 1.5, "take 1.5"),
            (
                |p: &mut SpreadParams| p.max_notional_usd = 0.0,
                "notional 0",
            ),
            (|p: &mut SpreadParams| p.fee_bps_a = -1.0, "fee -1"),
            (|p: &mut SpreadParams| p.tp_bps = -1.0, "tp -1"),
            (|p: &mut SpreadParams| p.sl_bps = -1.0, "sl -1"),
        ];
        for (mutate, why) in cases {
            let mut input = testkit::input();
            mutate(&mut input.params);
            assert!(save_strategy(&db, &sync, input).is_err(), "{why}");
        }
        // take_fraction == 1 and persist_sec == 0 are legal.
        let mut ok = testkit::input();
        ok.params.take_fraction = 1.0;
        ok.params.persist_sec = 0.0;
        assert!(save_strategy(&db, &sync, ok).is_ok());
    }

    #[test]
    fn save_rejects_non_trading_venue_and_unconnected_account() {
        let (db, _dir) = testkit::temp_db();
        let sync = connected_sync(&db);
        // Lighter does not support trading yet.
        db.insert_account(&testkit::account_row("acc-li", "lighter"))
            .expect("insert lighter");
        sync.insert(
            "acc-li".to_string(),
            Arc::new(testkit::FakeStrategyAdapter::new("lighter")),
        );
        let mut input = testkit::input();
        input.leg_b.account_id = "acc-li".to_string();
        input.leg_b.exchange_id = "lighter".to_string();
        let err = save_strategy(&db, &sync, input).unwrap_err();
        assert!(err.contains("暂不支持下单"), "{err}");
        // An account row with no running adapter is not connected.
        db.insert_account(&testkit::account_row("acc-off", "entropy"))
            .expect("insert off");
        let mut input = testkit::input();
        input.leg_b.account_id = "acc-off".to_string();
        let err = save_strategy(&db, &sync, input).unwrap_err();
        assert!(err.contains("未连接"), "{err}");
        // Same account on both legs is rejected.
        let mut input = testkit::input();
        input.leg_b.account_id = "acc-a".to_string();
        input.leg_b.exchange_id = "hyperliquid".to_string();
        assert!(save_strategy(&db, &sync, input).is_err());
        // Unknown kind is rejected.
        let mut input = testkit::input();
        input.kind = "grid".to_string();
        assert!(save_strategy(&db, &sync, input).is_err());
    }

    #[test]
    fn midline_zero_with_fees_warns_but_saves() {
        let (db, _dir) = testkit::temp_db();
        let sync = connected_sync(&db);
        let mut input = testkit::input();
        input.params.midline_bps = 0.0;
        input.params.fee_bps_a = 2.0;
        input.params.fee_bps_b = 3.0;
        let saved = save_strategy(&db, &sync, input).expect("save with warning");
        assert_eq!(
            saved.warning.as_deref(),
            Some("midline 未标定，建议先录数据校准")
        );
        assert_eq!(load_strategies(&db).unwrap().len(), 1);
        // No fees, no warning; a non-zero midline, no warning.
        let (db2, _dir2) = testkit::temp_db();
        let sync2 = connected_sync(&db2);
        let mut input = testkit::input();
        input.params.fee_bps_a = 0.0;
        input.params.fee_bps_b = 0.0;
        assert!(save_strategy(&db2, &sync2, input)
            .expect("save")
            .warning
            .is_none());
    }

    #[test]
    fn enable_toggle_and_views_count_strategy_orders() {
        let (db, _dir) = testkit::temp_db();
        let sync = connected_sync(&db);
        let saved = save_strategy(&db, &sync, testkit::input()).expect("save");
        let id = saved.instance.id.clone();
        let orders = Arc::new(OrderStore::new());
        let stats: StrategyStatsMap = Arc::new(Mutex::new(HashMap::new()));
        let views = list_strategy_views(&db, &orders, &stats).expect("views");
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].orders_placed, 0);
        assert!(views[0].last_eval_ms.is_none());
        // One manual order does not count; one strategy order does.
        use crate::core::types::{OrderInfo, OrderStatus};
        let mut manual = OrderInfo {
            order_id: "1".to_string(),
            client_order_id: "0xmanual".to_string(),
            exchange_id: "hyperliquid".to_string(),
            account_id: "acc-a".to_string(),
            symbol: "BTC".to_string(),
            side: "sell".to_string(),
            order_type: "limit".to_string(),
            status: OrderStatus::Open,
            price: Some(90000.0),
            quantity: 0.01,
            filled_quantity: 0.0,
            remaining_quantity: 0.01,
            average_fill_price: None,
            error: None,
            trigger_px: None,
            tpsl: None,
            origin: "manual".to_string(),
            created_at: 0,
            updated_at: 0,
        };
        orders.insert(manual.clone());
        manual.client_order_id = "0xstrat".to_string();
        manual.origin = format!("strategy:{id}");
        orders.insert(manual);
        let views = list_strategy_views(&db, &orders, &stats).expect("views");
        assert_eq!(views[0].orders_placed, 1);
        // Disable, then re-enable.
        let off = set_strategy_enabled(&db, &id, false).expect("disable");
        assert!(!off.enabled);
        assert!(!load_strategies(&db).unwrap()[0].enabled);
        assert!(
            set_strategy_enabled(&db, &id, true)
                .expect("enable")
                .enabled
        );
        assert!(set_strategy_enabled(&db, "no-such-id", true).is_err());
    }
}
