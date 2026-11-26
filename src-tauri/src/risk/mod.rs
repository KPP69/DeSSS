//! Trading risk gateway (phase 5A §3).
//!
//! One rule governs everything here: **no path may bypass the gateway
//! to call an adapter's `place_order` directly.** The order commands in
//! `commands` gather the context and call `RiskGateway::check_order`
//! first; a rejection means the order is never built, never signed,
//! never sent.
//!
//! The hard checks (§3 spec §4), in order:
//! 1. Kill Switch — when on, no new order of any kind is placed.
//! 2. Drawdown — once the session drawdown exceeds `max_drawdown_pct`,
//!    no new position may be opened until equity recovers.
//! 3. Account status — the account must be connected (unknown counts
//!    as not connected: an unknown account never passes).
//! 4. Market data freshness — the streamed book for the symbol must not
//!    be stale (> 5s without a frame; see `realtime::STREAM_STALE_MS`).
//! 5. Quantity / price validity — qty > 0, limit price > 0, and both
//!    exactly aligned to the market's step / tick (Decimal math).
//! 6. Direction — the order's side must be allowed by `allowed_sides`.
//! 7. Per-order max notional — the order's notional must fit the limit.
//! 8. Max position — current position notional + this order must fit
//!    `max_position_usd`.
//! 9. Available funds — the notional must fit the account's available
//!    balance.
//!
//! Runtime risks (§3 spec §4): a stale book, a broken exchange
//! connection or an unknown account state all stop new orders (checks
//! 3–4); an Unresolved order blocks re-submission of the same client
//! order id (enforced by the execution engine in §4, which owns the
//! order store); consecutive failed attempts auto-enable the kill
//! switch (`record_order_outcome`, threshold `auto_pause_after_failures`);
//! the manual kill switch is always available on the Risk page.
//!
//! Cancels are different on purpose: a kill switch that also blocks
//! cancels would trap the user in a position they cannot exit. Cancels
//! go through `check_cancel`, which only verifies the account is
//! connected — never the kill switch, never the book, never the limits.
//!
//! The gateway itself is pure: it takes a plain `RiskCheckContext` and
//! returns a verdict, so every rule is unit-testable without an app.
//! Persistence (SQLite `settings`) lives in `load` / `save` and in the
//! risk commands; the in-memory config is the single source of truth
//! while the app runs.

pub mod precision;

use std::sync::RwLock;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::data_engine::DataEngine;
use precision::{is_aligned, to_decimal};

/// Which directions new orders may open. "sell" here means opening a
/// short; closing a long by selling is not a new short and is not
/// restricted by this — the manual trading area in §4 only opens
/// positions, so the distinction does not arise there yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllowedSides {
    Both,
    LongOnly,
    ShortOnly,
}

impl Default for AllowedSides {
    fn default() -> Self {
        AllowedSides::Both
    }
}

/// Risk limits + kill switch, persisted in SQLite `settings` under the
/// `risk.*` keys. Defaults are conservative for a test account.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskConfig {
    /// When true, no new order may be placed (cancels still work).
    pub kill_switch: bool,
    /// Hard cap on one order's notional in USD.
    pub max_order_notional_usd: f64,
    /// Hard cap on total position notional in USD (enforced in §4
    /// against live positions; kept here so the page shows one config).
    pub max_position_usd: f64,
    /// Max drawdown percent that trips the kill switch automatically
    /// (enforced in §4 against live equity; kept here for the page).
    pub max_drawdown_pct: f64,
    /// Which directions new orders may open.
    pub allowed_sides: AllowedSides,
    /// Consecutive failed order attempts that auto-enable the kill
    /// switch ("连续异常触发暂停"). 0 disables the auto-pause.
    pub auto_pause_after_failures: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            kill_switch: false,
            max_order_notional_usd: 10_000.0,
            max_position_usd: 50_000.0,
            max_drawdown_pct: 10.0,
            allowed_sides: AllowedSides::Both,
            auto_pause_after_failures: 3,
        }
    }
}

impl RiskConfig {
    /// The persisted limits must be sane; a bad config is rejected at
    /// save time, never silently clamped.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_order_notional_usd <= 0.0 {
            return Err("单笔最大名义必须大于 0".to_string());
        }
        if self.max_position_usd <= 0.0 {
            return Err("最大持仓名义必须大于 0".to_string());
        }
        if !(self.max_drawdown_pct > 0.0 && self.max_drawdown_pct <= 100.0) {
            return Err("最大回撤必须在 0–100% 之间".to_string());
        }
        if self.auto_pause_after_failures > 10 {
            return Err("连续异常自动暂停次数不得超过 10".to_string());
        }
        Ok(())
    }

    /// Load from the SQLite `settings` table, falling back to defaults
    /// for keys that were never saved.
    pub fn load(db: &DataEngine) -> Self {
        let mut cfg = RiskConfig::default();
        let get = |key: &str| db.get_setting(key).ok().flatten();
        if let Some(v) = get("risk.kill_switch") {
            cfg.kill_switch = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = get("risk.max_order_notional_usd") {
            if let Ok(n) = v.parse::<f64>() {
                if n > 0.0 {
                    cfg.max_order_notional_usd = n;
                }
            }
        }
        if let Some(v) = get("risk.max_position_usd") {
            if let Ok(n) = v.parse::<f64>() {
                if n > 0.0 {
                    cfg.max_position_usd = n;
                }
            }
        }
        if let Some(v) = get("risk.max_drawdown_pct") {
            if let Ok(n) = v.parse::<f64>() {
                if n > 0.0 && n <= 100.0 {
                    cfg.max_drawdown_pct = n;
                }
            }
        }
        if let Some(v) = get("risk.allowed_sides") {
            cfg.allowed_sides = match v.as_str() {
                "long_only" => AllowedSides::LongOnly,
                "short_only" => AllowedSides::ShortOnly,
                _ => AllowedSides::Both,
            };
        }
        if let Some(v) = get("risk.auto_pause_after_failures") {
            if let Ok(n) = v.parse::<u32>() {
                if n <= 10 {
                    cfg.auto_pause_after_failures = n;
                }
            }
        }
        cfg
    }

    /// Persist to the SQLite `settings` table.
    pub fn save(&self, db: &DataEngine) -> Result<(), String> {
        self.validate()?;
        db.set_setting("risk.kill_switch", if self.kill_switch { "1" } else { "0" })?;
        db.set_setting(
            "risk.max_order_notional_usd",
            &self.max_order_notional_usd.to_string(),
        )?;
        db.set_setting("risk.max_position_usd", &self.max_position_usd.to_string())?;
        db.set_setting("risk.max_drawdown_pct", &self.max_drawdown_pct.to_string())?;
        db.set_setting(
            "risk.allowed_sides",
            match self.allowed_sides {
                AllowedSides::Both => "both",
                AllowedSides::LongOnly => "long_only",
                AllowedSides::ShortOnly => "short_only",
            },
        )?;
        db.set_setting(
            "risk.auto_pause_after_failures",
            &self.auto_pause_after_failures.to_string(),
        )?;
        Ok(())
    }
}

/// Drawdown snapshot for the Risk page: initial equity (first sample
/// of the session), session high-water mark, latest sampled equity and
/// the current drawdown against the high.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawdownStatus {
    pub initial_equity: Option<f64>,
    pub session_high: f64,
    pub last_equity: Option<f64>,
    pub drawdown_pct: f64,
}

/// Everything `check_order` needs, gathered by the caller (the order
/// command) from the live account state, the stream engine and the
/// market catalog. All money / price / qty fields are `Decimal` — the
/// gateway never compares floats.
#[derive(Clone, Debug)]
pub struct RiskCheckContext {
    pub account_connected: bool,
    /// True when the streamed book for this symbol is stale.
    pub book_stale: bool,
    /// "buy" | "sell" — the direction the order opens.
    pub side: String,
    /// qty × price, in USD.
    pub notional_usd: Decimal,
    /// Current total position notional across the account, in USD.
    pub current_position_notional_usd: Decimal,
    /// Account available balance, in USD.
    pub available_usd: Decimal,
    pub qty: Decimal,
    /// Current net position in this symbol, signed (long +, short
    /// -). Only the max-position check may exempt a reduce (F5).
    pub position_qty_signed: f64,
    /// None for market orders (no limit price to align).
    pub price: Option<Decimal>,
    pub tick_size: Decimal,
    pub step_size: Decimal,
}

/// The gateway: one shared config + the pure check functions. No path
/// to an adapter runs without going through here first.
///
/// Session state the gateway owns: the drawdown tracker (initial
/// equity, session high-water mark, latest equity — sampled every 10s
/// from the live account states and at each order attempt) and the
/// consecutive-failure counter that auto-enables the kill switch.
pub struct RiskGateway {
    config: RwLock<RiskConfig>,
    initial_equity: std::sync::Mutex<Option<f64>>,
    session_high: std::sync::Mutex<f64>,
    last_equity: std::sync::Mutex<Option<f64>>,
    consecutive_failures: std::sync::Mutex<u32>,
}

impl RiskGateway {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config: RwLock::new(config),
            initial_equity: std::sync::Mutex::new(None),
            session_high: std::sync::Mutex::new(0.0),
            last_equity: std::sync::Mutex::new(None),
            consecutive_failures: std::sync::Mutex::new(0),
        }
    }

    pub fn config(&self) -> RiskConfig {
        self.config.read().map(|c| c.clone()).unwrap_or_default()
    }

    pub fn set_config(&self, config: RiskConfig) {
        if let Ok(mut c) = self.config.write() {
            *c = config;
        }
    }

    /// Record a total-equity sample (sum across connected accounts).
    /// The first sample of the session becomes the initial equity; the
    /// high-water mark only ever moves up. Samples of 0 or less (no
    /// account connected yet) are ignored so they cannot corrupt the
    /// high or fake a 100% drawdown.
    pub fn record_equity(&self, equity_usd: f64) {
        if !(equity_usd > 0.0) {
            return;
        }
        if let Ok(mut initial) = self.initial_equity.lock() {
            if initial.is_none() {
                *initial = Some(equity_usd);
            }
        }
        if let Ok(mut high) = self.session_high.lock() {
            if equity_usd > *high {
                *high = equity_usd;
            }
        }
        if let Ok(mut last) = self.last_equity.lock() {
            *last = Some(equity_usd);
        }
    }

    /// Current drawdown in percent against the session high. 0 when
    /// nothing has been sampled yet.
    pub fn current_drawdown_pct(&self) -> f64 {
        let (high, last) = (
            self.session_high.lock().map(|h| *h).unwrap_or(0.0),
            self.last_equity.lock().map(|l| *l).unwrap_or(None),
        );
        match (high, last) {
            (h, Some(e)) if h > 0.0 && e < h => (h - e) / h * 100.0,
            _ => 0.0,
        }
    }

    pub fn drawdown_status(&self) -> DrawdownStatus {
        DrawdownStatus {
            initial_equity: self.initial_equity.lock().map(|i| *i).unwrap_or(None),
            session_high: self.session_high.lock().map(|h| *h).unwrap_or(0.0),
            last_equity: self.last_equity.lock().map(|l| *l).unwrap_or(None),
            drawdown_pct: self.current_drawdown_pct(),
        }
    }

    /// Record one order attempt's outcome. A success resets the
    /// consecutive-failure counter; a failure increments it, and when
    /// the count reaches the configured threshold the kill switch is
    /// auto-enabled ("连续异常触发暂停"). Returns true exactly when the
    /// auto-pause just tripped, so the caller can log it.
    pub fn record_order_outcome(&self, success: bool) -> bool {
        let mut count = match self.consecutive_failures.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };
        if success {
            *count = 0;
            return false;
        }
        *count += 1;
        let threshold = self.config().auto_pause_after_failures;
        if threshold > 0 && *count >= threshold && !self.config().kill_switch {
            if let Ok(mut c) = self.config.write() {
                c.kill_switch = true;
            }
            return true;
        }
        false
    }

    /// The pre-order hard checks, in the order listed in the module doc.
    /// The first failure wins and the order is never sent.
    pub fn check_order(&self, ctx: &RiskCheckContext) -> Result<(), String> {
        let cfg = self.config();
        if cfg.kill_switch {
            return Err("风控总开关已开启，禁止下单".to_string());
        }
        // Drawdown is a session-level stop, like the kill switch: once
        // the session is over its allowed drawdown, no new position
        // may be opened until equity recovers.
        let dd = self.current_drawdown_pct();
        if dd > cfg.max_drawdown_pct {
            return Err(format!(
                "当前回撤 {dd:.2}% 超过上限 {}%，停止开仓",
                cfg.max_drawdown_pct
            ));
        }
        if !ctx.account_connected {
            return Err("账户未连接，无法下单".to_string());
        }
        if ctx.book_stale {
            return Err("行情已过期（超过 5 秒未更新），禁止下单".to_string());
        }
        if ctx.qty <= Decimal::ZERO {
            return Err("下单数量必须大于 0".to_string());
        }
        if !is_aligned(ctx.qty, ctx.step_size) {
            return Err(format!(
                "数量 {} 不是数量步长 {} 的整数倍",
                ctx.qty, ctx.step_size
            ));
        }
        if let Some(price) = ctx.price {
            if price <= Decimal::ZERO {
                return Err("限价必须大于 0".to_string());
            }
            if !is_aligned(price, ctx.tick_size) {
                return Err(format!(
                    "价格 {} 不是价格步长 {} 的整数倍",
                    price, ctx.tick_size
                ));
            }
        }
        match cfg.allowed_sides {
            AllowedSides::LongOnly if ctx.side == "sell" => {
                return Err("当前只允许做多，禁止开空".to_string());
            }
            AllowedSides::ShortOnly if ctx.side == "buy" => {
                return Err("当前只允许做空，禁止开多".to_string());
            }
            _ => {}
        }
        let max_notional = to_decimal(cfg.max_order_notional_usd);
        if ctx.notional_usd > max_notional {
            return Err(format!(
                "单笔名义 ${} 超过上限 ${}",
                ctx.notional_usd, cfg.max_order_notional_usd
            ));
        }
        let max_position = to_decimal(cfg.max_position_usd);
        // F5: a reduce (side against the position, size within it)
        // never grows the position, so the max-position cap does not
        // apply to it. Nothing else is exempted.
        let reducing = ((ctx.side == "buy" && ctx.position_qty_signed < 0.0)
            || (ctx.side == "sell" && ctx.position_qty_signed > 0.0))
            && ctx.qty <= to_decimal(ctx.position_qty_signed.abs());
        if !reducing && ctx.current_position_notional_usd + ctx.notional_usd > max_position {
            return Err(format!(
                "持仓名义将达到 ${}，超过上限 ${}",
                ctx.current_position_notional_usd + ctx.notional_usd,
                cfg.max_position_usd
            ));
        }
        if ctx.notional_usd > ctx.available_usd {
            return Err(format!(
                "可用资金不足：单笔名义 ${}，可用 ${}",
                ctx.notional_usd, ctx.available_usd
            ));
        }
        Ok(())
    }

    /// Cancel checks: the kill switch, the book and the limits never
    /// block an exit. Only the account connection matters.
    pub fn check_cancel(&self, account_connected: bool) -> Result<(), String> {
        if !account_connected {
            return Err("账户未连接，无法撤单".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;

    fn ctx() -> RiskCheckContext {
        RiskCheckContext {
            account_connected: true,
            book_stale: false,
            side: "buy".to_string(),
            notional_usd: dec!(1000),
            current_position_notional_usd: dec!(0),
            available_usd: dec!(5000),
            qty: dec!(0.01),
            position_qty_signed: 0.0,
            price: Some(dec!(100000)),
            tick_size: dec!(0.5),
            step_size: dec!(0.001),
        }
    }

    fn gateway() -> RiskGateway {
        RiskGateway::new(RiskConfig::default())
    }

    #[test]
    fn clean_order_passes() {
        assert!(gateway().check_order(&ctx()).is_ok());
    }

    #[test]
    fn kill_switch_blocks_orders_but_not_cancels() {
        let g = gateway();
        g.set_config(RiskConfig {
            kill_switch: true,
            ..RiskConfig::default()
        });
        assert!(g.check_order(&ctx()).is_err());
        // A kill switch must never trap the user in a position.
        assert!(g.check_cancel(true).is_ok());
        assert!(g.check_cancel(false).is_err());
    }

    #[test]
    fn each_hard_check_fires() {
        let g = gateway();
        let mut c = ctx();
        c.account_connected = false;
        assert!(g.check_order(&c).is_err());
        c = ctx();
        c.book_stale = true;
        assert!(g.check_order(&c).is_err());
        c = ctx();
        c.qty = dec!(0);
        assert!(g.check_order(&c).is_err());
        c = ctx();
        c.qty = dec!(0.0105);
        assert!(g.check_order(&c).is_err());
        c = ctx();
        c.price = Some(dec!(100000.3));
        assert!(g.check_order(&c).is_err());
        c = ctx();
        c.price = Some(dec!(0));
        assert!(g.check_order(&c).is_err());
        c = ctx();
        c.notional_usd = dec!(10001);
        assert!(g.check_order(&c).is_err());
        c = ctx();
        c.notional_usd = dec!(6000);
        assert!(g.check_order(&c).is_err());
    }

    #[test]
    fn market_order_has_no_price_to_align() {
        let g = gateway();
        let mut c = ctx();
        c.price = None;
        c.notional_usd = dec!(900);
        assert!(g.check_order(&c).is_ok());
    }

    #[test]
    fn direction_and_max_position_enforced() {
        let g = gateway();
        g.set_config(RiskConfig {
            allowed_sides: AllowedSides::LongOnly,
            ..RiskConfig::default()
        });
        let mut c = ctx();
        c.side = "sell".to_string();
        assert!(g.check_order(&c).is_err());
        c.side = "buy".to_string();
        assert!(g.check_order(&c).is_ok());

        // Current positions + this order must fit max_position_usd.
        let g2 = gateway();
        let mut c2 = ctx();
        c2.current_position_notional_usd = dec!(49500);
        c2.notional_usd = dec!(1000);
        assert!(g2.check_order(&c2).is_err());
        c2.current_position_notional_usd = dec!(40000);
        assert!(g2.check_order(&c2).is_ok());
    }

    /// F5: selling within a long position skips ONLY the
    /// max-position check; selling more, or buying into a long,
    /// is still blocked by it.
    #[test]
    fn reduce_order_exempt_from_max_position_only() {
        let g = gateway();
        let mut c = ctx();
        c.current_position_notional_usd = dec!(50000);
        c.position_qty_signed = 0.5;
        c.available_usd = dec!(100000);
        c.side = "sell".to_string();
        c.qty = dec!(0.05);
        c.notional_usd = dec!(500);
        assert!(g.check_order(&c).is_ok());
        c.qty = dec!(0.6);
        c.notional_usd = dec!(6000);
        assert!(g.check_order(&c).is_err());
        c.side = "buy".to_string();
        c.qty = dec!(0.05);
        c.notional_usd = dec!(500);
        assert!(g.check_order(&c).is_err());
    }

    #[test]
    fn drawdown_stops_new_orders_until_recovery() {
        let g = gateway();
        g.record_equity(10000.0);
        g.record_equity(11000.0); // session high 11000
        g.record_equity(9500.0); // drawdown (11000-9500)/11000 = 13.6%
        assert!(g.current_drawdown_pct() > 10.0);
        assert!(g.check_order(&ctx()).is_err());
        // Equity recovers above the 10% band: orders open again.
        g.record_equity(10500.0); // drawdown 4.5%
        assert!(g.check_order(&ctx()).is_ok());
        let snap = g.drawdown_status();
        assert_eq!(snap.initial_equity, Some(10000.0));
        assert_eq!(snap.session_high, 11000.0);
        assert_eq!(snap.last_equity, Some(10500.0));
        // Zero / negative samples never corrupt the tracker.
        g.record_equity(0.0);
        assert_eq!(g.drawdown_status().session_high, 11000.0);
    }

    #[test]
    fn consecutive_failures_auto_pause_and_reset() {
        let g = gateway();
        assert!(!g.record_order_outcome(false));
        assert!(!g.record_order_outcome(false));
        // Third consecutive failure trips the auto-pause (default 3).
        assert!(g.record_order_outcome(false));
        assert!(g.config().kill_switch);
        assert!(g.check_order(&ctx()).is_err());
        // A success resets the counter (checked on a fresh gateway:
        // the paused one stays paused until a human turns it off).
        let g2 = gateway();
        g2.record_order_outcome(false);
        g2.record_order_outcome(false);
        assert!(!g2.record_order_outcome(true));
        assert!(!g2.record_order_outcome(false));
        assert!(!g2.config().kill_switch);
    }

    #[test]
    fn config_validation_and_persistence_roundtrip() {
        let bad = RiskConfig {
            max_order_notional_usd: 0.0,
            ..RiskConfig::default()
        };
        assert!(bad.validate().is_err());
        let bad2 = RiskConfig {
            max_drawdown_pct: 150.0,
            ..RiskConfig::default()
        };
        assert!(bad2.validate().is_err());

        let root = std::env::temp_dir().join(format!("desss-risk-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let data_dir = root.join("data");
        let paths = crate::portable::PortablePaths {
            db_path: data_dir.join("desss.db"),
            log_dir: data_dir.join("logs"),
            vault_key_path: data_dir.join("vault.key"),
            vault_enc_path: data_dir.join("vault.enc"),
            root,
            data_dir,
        };
        paths.ensure_dirs().expect("dirs");
        let db = DataEngine::open(paths.clone()).expect("db");
        let cfg = RiskConfig {
            kill_switch: true,
            max_order_notional_usd: 2500.0,
            max_position_usd: 20000.0,
            max_drawdown_pct: 5.0,
            allowed_sides: AllowedSides::ShortOnly,
            auto_pause_after_failures: 5,
        };
        cfg.save(&db).expect("save");
        let back = RiskConfig::load(&db);
        assert!(back.kill_switch);
        assert_eq!(back.max_order_notional_usd, 2500.0);
        assert_eq!(back.max_position_usd, 20000.0);
        assert_eq!(back.max_drawdown_pct, 5.0);
        assert_eq!(back.allowed_sides, AllowedSides::ShortOnly);
        assert_eq!(back.auto_pause_after_failures, 5);
        let _ = std::fs::remove_dir_all(&paths.data_dir);
    }

    /// Frontend/backend field alignment: the exact JSON `tauri.ts`
    /// `setRiskConfig` sends must deserialize, and serializing it back
    /// must use the same camelCase keys (same class of bug as the
    /// 2026-11-26 `place_order` "missing field `account_id`").
    #[test]
    fn risk_config_matches_frontend_payload() {
        let cfg: RiskConfig = serde_json::from_str(
            r#"{"killSwitch":true,"maxOrderNotionalUsd":2500.0,"maxPositionUsd":20000.0,"maxDrawdownPct":5.0,"allowedSides":"long_only","autoPauseAfterFailures":3}"#,
        )
        .expect("frontend payload must deserialize");
        assert!(cfg.kill_switch);
        assert_eq!(cfg.max_order_notional_usd, 2500.0);
        assert_eq!(cfg.allowed_sides, AllowedSides::LongOnly);
        assert_eq!(cfg.auto_pause_after_failures, 3);
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"killSwitch\":true"), "{json}");
        assert!(json.contains("\"allowedSides\":\"long_only\""), "{json}");
        assert!(!json.contains("kill_switch"), "{json}");
    }
}
