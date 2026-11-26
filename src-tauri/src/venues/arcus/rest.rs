//! Arcus account reads (PR-1.3 commit 2): REST polling only, no
//! market WS is opened for an account. Moved verbatim from
//! `adapters/arcus.rs`.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use super::{ArcusAdapter, Snapshot, ARCUS_REST_URL};
use crate::bus::AppEvent;
use crate::core::error::{VenueError, VenueResult};
use crate::core::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position};
use crate::core::util::{f64_from, now_ms};
use crate::venues::ExchangeAdapter;

const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Side of one position: the `side` literal when it parses
/// (case-insensitive `LONG` / `SHORT`), else the sign of `size`.
fn parse_side(side: &Value, size: f64) -> &'static str {
    match side.as_str().map(|s| s.to_ascii_lowercase()) {
        Some(s) if s == "long" => "long",
        Some(s) if s == "short" => "short",
        _ => {
            if size >= 0.0 {
                "long"
            } else {
                "short"
            }
        }
    }
}

/// Parse one `Position` object (shared by `/v1/account`,
/// `/v1/positions` and the WS `positions` snapshot).
fn parse_position(p: &Value) -> Option<LivePosition> {
    let size = f64_from(&p["size"]);
    if size == 0.0 {
        return None;
    }
    Some(LivePosition {
        symbol: p["marketDisplayName"].as_str().unwrap_or("").to_string(),
        side: parse_side(&p["side"], size).to_string(),
        qty: size.abs(),
        entry_price: f64_from(&p["averageEntryPrice"]),
        mark_price: f64_from(&p["markPx"]),
        unrealized_pnl: f64_from(&p["unrealizedPnl"]),
    })
}

/// Parse a `/v1/account` payload into
/// (equity, available, pnl, positions). PnL is derived: there is no
/// total-PnL field on the endpoint.
fn parse_account(v: &Value) -> Result<(f64, f64, f64, Vec<LivePosition>), String> {
    if v.get("equity").is_none() {
        return Err("账户数据格式异常（缺少 equity）".to_string());
    }
    let equity = f64_from(&v["equity"]);
    let available = f64_from(&v["freeCollateral"]);
    let pnl = equity - f64_from(&v["netDeposits"]);
    let mut positions = Vec::new();
    if let Some(map) = v["positions"].as_object() {
        for p in map.values() {
            if let Some(pos) = parse_position(p) {
                positions.push(pos);
            }
        }
    }
    Ok((equity, available, pnl, positions))
}

impl ArcusAdapter {
    /// Latency is always one measured round-trip: the last REST request
    /// time. Frame staleness and wall-clock subtraction are never used —
    /// a stale frame timestamp is what made latency run away to 1.5M ms.
    fn latency_ms(&self) -> Option<u64> {
        self.last_rest_rtt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map(|r| r.as_millis() as u64)
    }
    fn log(&self, level: &str, message: String) {
        self.bus.log(level, self.name(), message);
    }
    fn publish_live(&self) {
        let snap = self
            .snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let status = crate::core::util::live_status(
            self.rest_ok.load(Ordering::SeqCst),
            self.last_error
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some(),
        )
        .to_string();
        let error = self
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let live = AccountLive {
            account_id: self.account_id.clone(),
            adapter_id: self.id.clone(),
            wallet_key: None,
            status,
            latency_ms: self.latency_ms(),
            equity_usd: snap.equity_usd,
            available_usd: snap.available_usd,
            pnl_usd: Some(snap.pnl_usd),
            currency: "USDG".to_string(),
            positions: snap.positions,
            error,
            ts: now_ms(),
        };
        if let Ok(mut map) = self.live.write() {
            map.insert(self.account_id.clone(), live.clone());
        }
        self.bus.publish(AppEvent::AccountLive(live));
    }
    fn set_rest(&self, ok: bool, rtt: Option<Duration>) {
        self.rest_ok.store(ok, Ordering::SeqCst);
        if let Some(r) = rtt {
            *self.last_rest_rtt.lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
        }
        self.publish_live();
    }
    /// One REST read of the account (no auth — address only).
    async fn fetch_account_state(&self) -> Result<(Snapshot, Balance), String> {
        let url = format!(
            "{ARCUS_REST_URL}/v1/account?address={}&accountIndex=0",
            self.address
        );
        let resp = crate::net::send_read(|| self.http.get(&url))
            .await
            .map_err(|e| format!("Arcus 账户请求失败：{e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let snippet: String = body.chars().take(200).collect();
            return Err(format!("Arcus 账户接口返回 HTTP {status}：{snippet}"));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("账户响应解析失败：{e}"))?;
        let (equity, available, pnl, positions) = parse_account(&v)?;
        let snap = Snapshot {
            equity_usd: equity,
            available_usd: available,
            pnl_usd: pnl,
            positions,
        };
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: "USDG".to_string(),
            free: available,
            locked: (equity - available).max(0.0),
            total: equity,
            equity_usd: equity,
            ts: now_ms(),
        };
        Ok((snap, balance))
    }
    async fn poll_account(&self) {
        // Post-fill grace (spec §3): a poll landing inside the window
        // would return a pre-fill snapshot and overwrite the fresh
        // local estimate — skip the round entirely.
        if self.reconcile.in_grace(&self.account_id) {
            return;
        }
        let started = Instant::now();
        match self.fetch_account_state().await {
            Ok((snap, balance)) => {
                *self.snapshot.lock().unwrap_or_else(|e| e.into_inner()) = snap;
                self.bus.publish(AppEvent::Balance(balance));
                self.set_rest(true, Some(started.elapsed()));
            }
            Err(e) => {
                self.log("error", format!("账户轮询失败：{e}"));
                self.set_rest(false, None);
            }
        }
    }
    /// REST poll loop.
    async fn rest_loop(&self) {
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
impl ExchangeAdapter for ArcusAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "Arcus"
    }

    async fn connect(&self) -> VenueResult<()> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> VenueResult<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn subscribe_ticks(&self, _symbols: Vec<String>) -> VenueResult<()> {
        Ok(())
    }
    async fn unsubscribe_ticks(&self, _symbols: Vec<String>) -> VenueResult<()> {
        Ok(())
    }

    async fn get_balances(&self, _account_id: &str) -> VenueResult<Vec<Balance>> {
        let (_, balance) = self
            .fetch_account_state()
            .await
            .map_err(crate::core::error::VenueError::transport)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> VenueResult<Vec<Position>> {
        let (snap, _) = self
            .fetch_account_state()
            .await
            .map_err(crate::core::error::VenueError::transport)?;
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

    async fn get_open_orders(&self, _account_id: &str) -> VenueResult<Vec<Order>> {
        Err(VenueError::unsupported("查询持仓单"))
    }

    async fn place_order(&self, _req: OrderRequest) -> VenueResult<Order> {
        Err(VenueError::unsupported("下单"))
    }

    async fn cancel_order(
        &self,
        _account_id: &str,
        _order_id: &str,
        _symbol: &str,
    ) -> VenueResult<()> {
        Err(VenueError::unsupported("撤单"))
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

    #[test]
    fn account_parse_derives_pnl_and_positions() {
        let v = serde_json::json!({
            "accountIndex": 0,
            "address": "0x0000000000000000000000000000000000000001",
            "equity": "101000",
            "freeCollateral": "90000",
            "netDeposits": "100000",
            "positions": {
                "1": {"marketDisplayName": "BTC-USD", "side": "LONG", "size": "0.5",
                      "averageEntryPrice": "60000", "markPx": "61000",
                      "unrealizedPnl": "500"},
                "2": {"marketDisplayName": "ETH-USD", "side": "short", "size": "-2",
                      "averageEntryPrice": "3000", "markPx": "2900",
                      "unrealizedPnl": "200"}
            }
        });
        let (equity, available, pnl, positions) = parse_account(&v).expect("parse");
        assert!((equity - 101000.0).abs() < 1e-9);
        assert!((available - 90000.0).abs() < 1e-9);
        assert!((pnl - 1000.0).abs() < 1e-9);
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].side, "long");
        assert_eq!(positions[1].side, "short");
        assert!((positions[1].qty - 2.0).abs() < 1e-9);
    }

    #[test]
    fn account_parse_rejects_unexpected_shape() {
        let v = serde_json::json!({"code": 403, "msg": "forbidden"});
        assert!(parse_account(&v).is_err());
    }

    #[test]
    fn side_falls_back_to_size_sign() {
        assert_eq!(parse_side(&Value::Null, 1.5), "long");
        assert_eq!(parse_side(&Value::Null, -1.5), "short");
        assert_eq!(parse_side(&serde_json::json!("weird"), -1.0), "short");
    }
}
