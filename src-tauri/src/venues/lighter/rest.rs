//! Lighter account reads (PR-1.3 commit 1): REST polling only, no
//! market WS is opened for an account. Moved verbatim from
//! `adapters/lighter.rs` except for the PnL fix (see `parse_account`).

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use super::{LighterAdapter, LighterNetwork, Snapshot};
use crate::adapters::{f64_from, now_ms, AdapterResult, ExchangeAdapter};
use crate::bus::AppEvent;
use crate::core::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position};

const POLL_INTERVAL: Duration = Duration::from_secs(10);

fn u64_from(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Parse the market list from a `GET /api/v1/orderBooks` response, lenient
/// about the wire shape: the outer list is `order_books` (snake_case) on
/// the live API, with `orderBooks` accepted as a fallback. Only active
/// perpetual markets are kept (`market_type == "perp"` when present,
/// `status == "active"` when present). Returns symbol -> market_id.
fn parse_order_books(v: &Value) -> Result<Vec<(String, u64)>, String> {
    let list = v
        .get("order_books")
        .or_else(|| v.get("orderBooks"))
        .and_then(|l| l.as_array())
        .ok_or_else(|| {
            let keys: Vec<&str> = v
                .as_object()
                .map(|o| o.keys().map(|k| k.as_str()).collect())
                .unwrap_or_default();
            format!("no order_books/orderBooks array in response (top-level keys: {keys:?})")
        })?;
    let mut out = Vec::new();
    for m in list {
        if let Some(t) = m.get("market_type").and_then(|t| t.as_str()) {
            if t != "perp" {
                continue;
            }
        }
        if let Some(s) = m.get("status").and_then(|s| s.as_str()) {
            if s != "active" {
                continue;
            }
        }
        let Some(sym) = m.get("symbol").and_then(|s| s.as_str()) else {
            continue;
        };
        let Some(mid) = m.get("market_id").and_then(u64_from) else {
            continue;
        };
        // Perpetual markets are named "BTC"/"ETH"; spot pairs ("BTC/USDC")
        // never match the exact names below.
        if sym == "BTC" || sym == "ETH" {
            out.push((sym.to_string(), mid));
        }
    }
    Ok(out)
}

/// First `n` characters of a response body (char-boundary safe), for error
/// messages that would otherwise hide the real payload.
fn body_snippet(text: &str, n: usize) -> &str {
    match text.char_indices().nth(n) {
        Some((i, _)) => &text[..i],
        None => text,
    }
}

impl LighterAdapter {
    /// Latency is always one measured round-trip: the last REST request
    /// time. Frame staleness and wall-clock subtraction are never used —
    /// a stale frame timestamp is what made latency run away to 1.5M ms.
    fn latency_ms(&self) -> Option<u64> {
        self.last_rest_rtt
            .lock()
            .unwrap()
            .map(|r| r.as_millis() as u64)
    }
    /// One console log line for the in-app drawer.
    fn log(&self, level: &str, message: String) {
        self.bus.log(level, self.name(), message);
    }
    fn publish_live(&self) {
        let snap = self.snapshot.lock().unwrap().clone();
        let status = crate::adapters::live_status(
            self.rest_ok.load(Ordering::SeqCst),
            self.last_error.lock().unwrap().is_some(),
        )
        .to_string();
        let error = self.last_error.lock().unwrap().clone();
        let live = AccountLive {
            account_id: self.account_id.clone(),
            adapter_id: self.id.clone(),
            wallet_key: None,
            status,
            latency_ms: self.latency_ms(),
            equity_usd: snap.equity,
            available_usd: snap.available,
            pnl_usd: snap.pnl,
            currency: self.network.currency().to_string(),
            positions: snap.positions,
            error,
            ts: now_ms(),
        };
        if let Ok(mut map) = self.live.write() {
            map.insert(self.account_id.clone(), live.clone());
        }
        self.bus.publish(AppEvent::AccountLive(live));
    }
    /// REST state changed: record it and republish.
    fn set_rest(&self, ok: bool, rtt: Option<Duration>) {
        self.rest_ok.store(ok, Ordering::SeqCst);
        if let Some(r) = rtt {
            *self.last_rest_rtt.lock().unwrap() = Some(r);
        }
        self.publish_live();
    }
    fn symbol_for(&self, market_id: u64) -> String {
        self.symbols
            .lock()
            .unwrap()
            .get(&market_id)
            .cloned()
            .unwrap_or_else(|| format!("market-{market_id}"))
    }
    /// Resolve BTC/ETH market ids from the public market list. Market ids
    /// differ between deployments and over time, so they are never hardcoded.
    async fn fetch_markets(&self) -> Result<(), String> {
        let url = format!("{}/api/v1/orderBooks", self.network.rest_base());
        let resp = crate::net::send_read(|| self.http.get(&url))
            .await
            .map_err(|e| format!("Lighter 盘口请求失败：{e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "lighter orderBooks returned HTTP {}",
                resp.status()
            ));
        }
        let text = resp
            .text()
            .await
            .map_err(|e| format!("Lighter 盘口读取失败：{}", crate::net::http_error(&e)))?;
        let v: Value = serde_json::from_str(&text).map_err(|e| {
            format!(
                "Lighter 盘口响应不是有效 JSON（{e}）；内容：{}",
                body_snippet(&text, 500)
            )
        })?;
        let parsed = parse_order_books(&v).map_err(|e| {
            format!(
                "lighter orderBooks: {e}; body: {}",
                body_snippet(&text, 500)
            )
        })?;
        let mut ids = HashMap::new();
        let mut names = HashMap::new();
        for (sym, mid) in parsed {
            if !ids.contains_key(&sym) {
                names.insert(mid, sym.clone());
                ids.insert(sym, mid);
            }
        }
        if !ids.contains_key("BTC") || !ids.contains_key("ETH") {
            return Err(format!(
                "lighter market list has no active BTC/ETH perps (got {ids:?}); body: {}",
                body_snippet(&text, 500)
            ));
        }
        *self.market_ids.lock().unwrap() = ids;
        *self.symbols.lock().unwrap() = names;
        Ok(())
    }
    /// One REST read of the public account endpoint.
    async fn fetch_account(&self) -> Result<(Snapshot, Balance), String> {
        let url = format!(
            "{}/api/v1/account?by=index&value={}",
            self.network.rest_base(),
            self.account_index
        );
        let resp = crate::net::send_read(|| self.http.get(&url))
            .await
            .map_err(|e| format!("Lighter 账户请求失败：{e}"))?;
        if !resp.status().is_success() {
            return Err(format!("lighter account returned HTTP {}", resp.status()));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("账户响应解析失败：{e}"))?;
        let Some(acc) = v["accounts"].as_array().and_then(|a| a.first()) else {
            return Err(format!("Lighter 账户 {} 不存在", self.account_index));
        };
        let (equity, available, pnl, positions) = self.parse_account(acc);
        let snap = Snapshot {
            equity,
            available,
            pnl,
            positions,
        };
        let currency = self.network.currency().to_string();
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: currency,
            free: available,
            locked: (equity - available).max(0.0),
            total: equity,
            equity_usd: equity,
            ts: now_ms(),
        };
        Ok((snap, balance))
    }
    /// Parse one `/api/v1/account` account object (pure except
    /// for the market-id -> symbol lookup): equity, available, PnL
    /// and the kept positions. The account object carries no
    /// `unrealized_pnl` (verified live 2026-11-27 against mainnet
    /// accounts 0-13), so PnL is summed from the kept positions:
    /// a kept position without the field makes the sum
    /// untrustworthy and PnL is `None`; no positions at all is a
    /// real zero (`Some(0.0)`). A future account-level field, if
    /// one ever appears, is ignored on purpose.
    fn parse_account(&self, acc: &Value) -> (f64, f64, Option<f64>, Vec<LivePosition>) {
        let equity = f64_from(&acc["total_asset_value"]);
        let available = f64_from(&acc["available_balance"]);
        let mut positions = Vec::new();
        let mut pnl_sum = 0.0;
        let mut pnl_known = true;
        if let Some(list) = acc["positions"].as_array() {
            for p in list {
                let qty_raw = f64_from(&p["position"]);
                if qty_raw == 0.0 {
                    continue;
                }
                let symbol = p["symbol"]
                    .as_str()
                    .map(|s| s.to_string())
                    .or_else(|| u64_from(&p["market_id"]).map(|id| self.symbol_for(id)))
                    .unwrap_or_default();
                let sign = p["sign"]
                    .as_i64()
                    .unwrap_or(if qty_raw >= 0.0 { 1 } else { -1 });
                let qty = qty_raw.abs();
                let entry = f64_from(&p["avg_entry_price"]);
                let value = f64_from(&p["position_value"]);
                let mark = if qty > 0.0 && value != 0.0 {
                    value / qty
                } else {
                    entry
                };
                let upnl = f64_from(&p["unrealized_pnl"]);
                if p["unrealized_pnl"].is_number()
                    || p["unrealized_pnl"]
                        .as_str()
                        .is_some_and(|s| s.trim().parse::<f64>().is_ok())
                {
                    pnl_sum += upnl;
                } else {
                    pnl_known = false;
                }
                positions.push(LivePosition {
                    symbol,
                    side: if sign >= 0 {
                        "long".to_string()
                    } else {
                        "short".to_string()
                    },
                    qty,
                    entry_price: entry,
                    mark_price: mark,
                    unrealized_pnl: upnl,
                });
            }
        }
        (equity, available, pnl_known.then_some(pnl_sum), positions)
    }

    async fn poll_account(&self) {
        // Post-fill grace (spec §3): a poll landing inside the window
        // would return a pre-fill snapshot and overwrite the fresh
        // local estimate — skip the round entirely.
        if self.reconcile.in_grace(&self.account_id) {
            return;
        }
        let started = Instant::now();
        match self.fetch_account().await {
            Ok((snap, balance)) => {
                *self.snapshot.lock().unwrap() = snap;
                self.bus.publish(AppEvent::Balance(balance));
                self.set_rest(true, Some(started.elapsed()));
            }
            Err(e) => {
                self.log("error", format!("账户轮询失败：{e}"));
                self.set_rest(false, None);
            }
        }
    }
    /// REST poll loop: as long as REST works, equity and positions keep
    /// showing. The market list is resolved once up front so positions that carry only
    /// a market id can be named; a failure there only warns.
    async fn rest_loop(&self) {
        if let Err(e) = self.fetch_markets().await {
            self.log(
                "warn",
                format!("市场列表读取失败，持仓可能显示 market-id：{e}"),
            );
        }
        self.poll_account().await;
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.tick().await;
        while self.running.load(Ordering::SeqCst) {
            // Either the 10s tick or an out-of-cycle wake (position
            // reconciliation) triggers the next poll.
            tokio::select! {
                _ = tick.tick() => {}
                _ = self.wake.notified() => {}
            }
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            self.poll_account().await;
        }
    }
}

#[async_trait]
impl ExchangeAdapter for LighterAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        if self.network == LighterNetwork::Robinhood {
            "Lighter RH"
        } else {
            "Lighter"
        }
    }

    async fn connect(&self) -> AdapterResult<()> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> AdapterResult<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn subscribe_ticks(&self, _symbols: Vec<String>) -> AdapterResult<()> {
        Ok(())
    }
    async fn unsubscribe_ticks(&self, _symbols: Vec<String>) -> AdapterResult<()> {
        Ok(())
    }

    async fn get_balances(&self, _account_id: &str) -> AdapterResult<Vec<Balance>> {
        let (_, balance) = self
            .fetch_account()
            .await
            .map_err(crate::adapters::AdapterError::Other)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> AdapterResult<Vec<Position>> {
        let (snap, _) = self
            .fetch_account()
            .await
            .map_err(crate::adapters::AdapterError::Other)?;
        Ok(snap
            .positions
            .iter()
            .map(|p| Position {
                adapter_id: self.id.clone(),
                account_id: self.account_id.clone(),
                symbol: p.symbol.clone(),
                side: p.side.clone(),
                qty: p.qty,
                entry_price: p.entry_price,
                mark_price: p.mark_price,
                unrealized_pnl: p.unrealized_pnl,
            })
            .collect())
    }

    async fn get_open_orders(&self, _account_id: &str) -> AdapterResult<Vec<Order>> {
        Err(crate::adapters::AdapterError::Other(
            "该交易所下单将在下一阶段接入".to_string(),
        ))
    }

    async fn place_order(&self, _req: OrderRequest) -> AdapterResult<Order> {
        Err(crate::adapters::AdapterError::Other(
            "该交易所下单将在下一阶段接入".to_string(),
        ))
    }

    async fn cancel_order(
        &self,
        _account_id: &str,
        _order_id: &str,
        _symbol: &str,
    ) -> AdapterResult<()> {
        Err(crate::adapters::AdapterError::Other(
            "该交易所下单将在下一阶段接入".to_string(),
        ))
    }

    async fn run(&self) {
        // Account sync is REST polling only: no market WS is opened for
        // an account (public market data comes from the on-demand
        // catalog, never from a per-account connection).
        self.rest_loop().await;
    }

    fn wake_sync(&self) {
        self.wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    use crate::adapters::reconcile::ReconcileState;
    use crate::bus::EventBus;

    #[test]
    fn network_endpoints_are_distinct() {
        assert_eq!(
            LighterNetwork::Mainnet.rest_base(),
            "https://mainnet.zklighter.elliot.ai"
        );
        assert_eq!(LighterNetwork::Mainnet.chain_id(), 304);
        assert_eq!(LighterNetwork::Mainnet.currency(), "USDC");
        assert_eq!(
            LighterNetwork::Robinhood.rest_base(),
            "https://api.rh.lighter.xyz"
        );
        assert_eq!(LighterNetwork::Robinhood.chain_id(), 466324);
        assert_eq!(LighterNetwork::Robinhood.currency(), "USDG");
    }

    #[test]
    fn order_books_parses_snake_case_and_filters_perps() {
        let v = serde_json::json!({
            "code": 200,
            "order_books": [
                {"symbol": "BTC", "market_id": 1, "market_type": "perp", "status": "active"},
                {"symbol": "ETH", "market_id": 0, "market_type": "perp", "status": "active"},
                {"symbol": "BTC/USDC", "market_id": 2049, "market_type": "spot", "status": "active"},
                {"symbol": "SOL", "market_id": 2, "market_type": "perp", "status": "inactive"}
            ]
        });
        let parsed = parse_order_books(&v).expect("parse");
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&("BTC".to_string(), 1)));
        assert!(parsed.contains(&("ETH".to_string(), 0)));
    }

    #[test]
    fn order_books_accepts_camel_case_fallback_and_reports_keys() {
        let v = serde_json::json!({
            "orderBooks": [
                {"symbol": "BTC", "market_id": 1},
                {"symbol": "ETH", "market_id": 0}
            ]
        });
        assert_eq!(parse_order_books(&v).expect("parse").len(), 2);
        let bad = serde_json::json!({"code": 200, "total": 0});
        let err = parse_order_books(&bad).unwrap_err();
        assert!(
            err.contains("order_books"),
            "error must name the expected fields: {err}"
        );
        assert!(
            err.contains("code"),
            "error must list the keys present: {err}"
        );
    }

    fn test_adapter() -> LighterAdapter {
        LighterAdapter::new(
            "lighter",
            LighterNetwork::Mainnet,
            0,
            "acc-1".to_string(),
            EventBus::new(16),
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(ReconcileState::new()),
        )
    }

    #[test]
    fn parse_account_sums_position_pnl_and_ignores_account_level_field() {
        // Shape mirrors the real /api/v1/account response (verified
        // live 2026-11-27): the account object has no
        // unrealized_pnl; positions carry it as a string. The
        // account-level "999" below must be ignored.
        let acc = serde_json::json!({
            "total_asset_value": 1000.0,
            "available_balance": 800.0,
            "unrealized_pnl": "999",
            "positions": [
                {"symbol": "BTC", "position": "0.1", "sign": 1,
                 "avg_entry_price": "90000", "position_value": "9500",
                 "unrealized_pnl": "500.000000"},
                {"symbol": "ETH", "position": "-2", "sign": -1,
                 "avg_entry_price": "3000", "position_value": "5800",
                 "unrealized_pnl": -200.5}
            ]
        });
        let (equity, available, pnl, positions) = test_adapter().parse_account(&acc);
        assert_eq!(equity, 1000.0);
        assert_eq!(available, 800.0);
        assert_eq!(pnl, Some(299.5));
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].unrealized_pnl, 500.0);
    }

    #[test]
    fn parse_account_pnl_is_none_when_a_position_lacks_the_field() {
        let acc = serde_json::json!({
            "total_asset_value": 1000.0,
            "available_balance": 800.0,
            "positions": [
                {"symbol": "BTC", "position": "0.1", "sign": 1,
                 "avg_entry_price": "90000", "position_value": "9500",
                 "unrealized_pnl": "10"},
                {"symbol": "ETH", "position": "2", "sign": 1,
                 "avg_entry_price": "3000", "position_value": "6000"}
            ]
        });
        let (_, _, pnl, positions) = test_adapter().parse_account(&acc);
        assert_eq!(pnl, None);
        assert_eq!(positions.len(), 2);
    }

    #[test]
    fn parse_account_pnl_is_zero_when_no_positions() {
        let acc = serde_json::json!({
            "total_asset_value": 500.0,
            "available_balance": 500.0,
            "positions": []
        });
        let (_, _, pnl, positions) = test_adapter().parse_account(&acc);
        assert_eq!(pnl, Some(0.0));
        assert!(positions.is_empty());
    }
}
