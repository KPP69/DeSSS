//! Hyperliquid / Entropy account reads: the REST poll loop and the
//! unified-account aggregation (mainnet perp + Entropy + spot).
//! Moved verbatim from `adapters/hyperliquid.rs` (PR-1.2).

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::{HyperliquidAdapter, Snapshot, HL_INFO_URL, POLL_INTERVAL};
use crate::bus::AppEvent;
use crate::core::error::VenueError;
use crate::core::types::{AccountLive, Balance, LivePosition};
use crate::core::util::{f64_from, live_status, now_ms};
use crate::venues::ExchangeAdapter;

/// One spot balance row from `spotClearinghouseState`.
#[derive(Clone, Debug)]
pub(crate) struct SpotBalance {
    coin: String,
    total: f64,
    hold: f64,
    entry_ntl: f64,
}

fn parse_spot_balances(v: &Value) -> Vec<SpotBalance> {
    v["balances"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|b| {
                    let coin = b["coin"].as_str()?.to_string();
                    Some(SpotBalance {
                        coin,
                        total: f64_from(&b["total"]),
                        hold: f64_from(&b["hold"]),
                        entry_ntl: f64_from(&b["entryNtl"]),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Merge spot balances into a perp snapshot (mainnet only). Under a unified
/// account the perp `clearinghouseState` alone shows $0: the USDC pool and
/// spot holdings live in `spotClearinghouseState`, which the docs call the
/// source of truth for the trading-account balance under unified/portfolio
/// margin. Total equity = perp account value + spot USDC + spot holdings
/// valued at the live book mid (or entry notional when no book is known);
/// available = perp withdrawable + free spot USDC.
pub(crate) fn merge_spot(
    perp: Snapshot,
    spot: &[SpotBalance],
    mark: &dyn Fn(&str) -> Option<f64>,
) -> Snapshot {
    let mut snap = perp;
    let mut spot_pnl = 0.0;
    for b in spot {
        if b.coin == "USDC" {
            snap.equity_usd += b.total;
            snap.available_usd += (b.total - b.hold).max(0.0);
            continue;
        }
        if b.total == 0.0 {
            continue;
        }
        let entry = if b.total != 0.0 {
            b.entry_ntl / b.total
        } else {
            0.0
        };
        let mid = mark(&b.coin).unwrap_or(entry);
        let value = mid * b.total;
        let upnl = value - b.entry_ntl;
        spot_pnl += upnl;
        snap.equity_usd += value;
        snap.positions.push(LivePosition {
            symbol: format!("{} (spot)", b.coin),
            side: "long".to_string(),
            qty: b.total,
            entry_price: entry,
            mark_price: mid,
            unrealized_pnl: upnl,
        });
    }
    snap.pnl_usd += spot_pnl;
    snap
}

/// Add one perp snapshot into another (same master wallet, different
/// dex): equity, available margin and PnL sum, positions concatenate.
pub(crate) fn merge_perp(mut dst: Snapshot, src: Snapshot) -> Snapshot {
    dst.equity_usd += src.equity_usd;
    dst.available_usd += src.available_usd;
    dst.pnl_usd += src.pnl_usd;
    dst.positions.extend(src.positions);
    dst
}

/// Parse one `clearinghouseState` object (REST response or WS
/// `clearinghouseState` frame — the shapes are identical) into a
/// snapshot. Pure, so the account WS reuses it (Phase 3.3).
pub(crate) fn parse_clearinghouse_state(v: &Value) -> Snapshot {
    let margin = &v["marginSummary"];
    let equity = f64_from(&margin["accountValue"]);
    let withdrawable = f64_from(&v["withdrawable"]);
    let mut positions = Vec::new();
    let mut pnl = 0.0;
    if let Some(list) = v["assetPositions"].as_array() {
        for item in list {
            let p = &item["position"];
            if p.is_null() {
                continue;
            }
            let upnl = f64_from(&p["unrealizedPnl"]);
            pnl += upnl;
            let szi = f64_from(&p["szi"]);
            positions.push(LivePosition {
                symbol: p["coin"].as_str().unwrap_or("").to_string(),
                side: if szi >= 0.0 {
                    "long".to_string()
                } else {
                    "short".to_string()
                },
                qty: szi.abs(),
                entry_price: f64_from(&p["entryPx"]),
                mark_price: f64_from(&p["positionValue"]) / szi.abs().max(1e-12),
                unrealized_pnl: upnl,
            });
        }
    }
    Snapshot {
        equity_usd: equity,
        available_usd: withdrawable,
        pnl_usd: pnl,
        positions,
    }
}

impl HyperliquidAdapter {
    /// Latency is always one measured round-trip: the last REST request
    /// time. Frame staleness and wall-clock subtraction are never used —
    /// a stale frame timestamp is what made latency run away to 1.5M ms.
    fn latency_ms(&self) -> Option<u64> {
        self.last_rest_rtt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map(|r| r.as_millis() as u64)
    }

    /// One console log line for the in-app drawer.
    pub(crate) fn log(&self, level: &str, message: String) {
        self.bus.log(level, self.name(), message);
    }

    pub(crate) fn publish_live(&self, source: &'static str) {
        let snap = self
            .snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let status = live_status(
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
            wallet_key: Some(self.address.to_lowercase()),
            status,
            latency_ms: self.latency_ms(),
            equity_usd: snap.equity_usd,
            available_usd: snap.available_usd,
            pnl_usd: Some(snap.pnl_usd),
            currency: "USDC".to_string(),
            positions: snap.positions,
            source: source.to_string(),
            degraded: self
                .ws_state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .degraded,
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
            *self.last_rest_rtt.lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
        }
        self.publish_live("rest");
    }

    /// One REST read of clearinghouseState for one dex (`None` = mainnet
    /// perp, `Some("io")` = Entropy). Returns (snapshot, balance).
    async fn fetch_clearinghouse(&self, dex: Option<&str>) -> Result<(Snapshot, Balance), String> {
        let mut body = serde_json::json!({
            "type": "clearinghouseState",
            "user": self.address,
        });
        if let Some(dex) = dex {
            body["dex"] = Value::String(dex.to_string());
        }
        let resp = crate::net::send_read(|| self.http.post(HL_INFO_URL).json(&body))
            .await
            .map_err(|e| format!("Hyperliquid 账户请求失败：{e}"))?;
        if !resp.status().is_success() {
            return Err(format!("Hyperliquid 账户接口返回 HTTP {}", resp.status()));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("账户响应解析失败：{e}"))?;
        let snap = parse_clearinghouse_state(&v);
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: "USDC".to_string(),
            free: snap.available_usd,
            locked: (snap.equity_usd - snap.available_usd).max(0.0),
            total: snap.equity_usd,
            equity_usd: snap.equity_usd,
            ts: now_ms(),
        };
        Ok((snap, balance))
    }

    /// One REST read of spotClearinghouseState (mainnet only; the dex
    /// variant has no spot side). Returns the raw balance rows.
    async fn fetch_spot_state(&self) -> Result<Vec<SpotBalance>, String> {
        let body = serde_json::json!({ "type": "spotClearinghouseState", "user": self.address });
        let resp = crate::net::send_read(|| self.http.post(HL_INFO_URL).json(&body))
            .await
            .map_err(|e| format!("Hyperliquid 现货账户请求失败：{e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "hyperliquid spot info returned HTTP {}",
                resp.status()
            ));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("现货账户响应解析失败：{e}"))?;
        Ok(parse_spot_balances(&v))
    }

    /// Full account read: mainnet perp + Entropy (dex "io") + spot, all
    /// aggregated into one unified snapshot for this master wallet. The
    /// mainnet leg is required; the io and spot legs only warn on failure
    /// (an unused io sub-account or a missing spot pool must not hide the
    /// main account). Because both the Hyperliquid and the Entropy
    /// adapters run this same aggregation, the two sides always show the
    /// same available balance and positions.
    pub(crate) async fn fetch_account_state(&self) -> Result<(Snapshot, Balance), String> {
        let (main, io, spot) = tokio::join!(
            self.fetch_clearinghouse(None),
            self.fetch_clearinghouse(Some("io")),
            self.fetch_spot_state(),
        );
        let (mut snap, _) = main?;
        // Phase 3.3: keep the per-leg snapshots for the WS merge —
        // a WS frame replaces one dex leg, the other legs stay as the
        // last REST poll left them. A failed leg keeps its old value,
        // never a zeroed one.
        {
            let mut legs = self.ws_legs.lock().unwrap_or_else(|e| e.into_inner());
            legs.main = Some(snap.clone());
            if let Ok((io_snap, _)) = &io {
                legs.io = Some(io_snap.clone());
            }
            if let Ok(rows) = &spot {
                legs.spot = Some(rows.clone());
            }
        }
        match io {
            Ok((io_snap, _)) => snap = merge_perp(snap, io_snap),
            Err(e) => {
                self.log("warn", format!("Entropy 账户读取失败，只用主站：{e}"));
            }
        }
        match spot {
            Ok(spot) => {
                // No book is subscribed any more, so spot holdings are
                // valued at their entry notional (unrealised PnL 0) until
                // a live mark exists.
                snap = merge_spot(snap, &spot, &|_| None);
            }
            Err(e) => {
                self.log("warn", format!("现货账户读取失败，只用永续仓位：{e}"));
            }
        }
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: "USDC".to_string(),
            free: snap.available_usd,
            locked: (snap.equity_usd - snap.available_usd).max(0.0),
            total: snap.equity_usd,
            equity_usd: snap.equity_usd,
            ts: now_ms(),
        };
        Ok((snap, balance))
    }

    pub(crate) async fn poll_account(&self) {
        // Post-fill grace (spec §3): a poll landing inside the window
        // would return a pre-fill snapshot and overwrite the fresh
        // local estimate — skip the round entirely.
        if self.reconcile.in_grace(&self.account_id) {
            return;
        }
        // Phase 3.3: the 60s healthy cadence counts from the last
        // fetch that actually ran, not from a grace skip.
        *self.last_poll.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
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

    /// REST poll loop (Phase 3.3 cadence): degraded → every 10s
    /// (the REST fallback carries the account); healthy → every 60s
    /// (pure reconciliation — the WS frames carry the live state).
    /// A wake (post-fill reconcile, WS reconnect snapshot) always
    /// polls at once. The first poll also fills the snapshot that
    /// dex symbol resolution needs (current holdings' coins).
    pub(crate) async fn rest_loop(&self) {
        self.poll_account().await;
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.tick().await;
        while self.running.load(Ordering::SeqCst) {
            // Either the 10s tick or an out-of-cycle wake (position
            // reconciliation) triggers the next poll.
            let woken = tokio::select! {
                _ = tick.tick() => false,
                _ = self.wake.notified() => true,
            };
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            if woken || self.ws_degraded() || self.rest_poll_due() {
                self.poll_account().await;
            }
        }
    }

    /// True when the last REST fetch is 60s or older (healthy
    /// cadence). The WS reconnect snapshot and post-fill wakes
    /// bypass this — they poll at once.
    fn rest_poll_due(&self) -> bool {
        self.last_poll
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map(|t| t.elapsed() >= Duration::from_secs(60))
            .unwrap_or(true)
    }

    /// One `/info` read (read-only: retry-once via `send_read`).
    pub(crate) async fn post_info(&self, body: Value) -> Result<Value, VenueError> {
        let resp = crate::net::send_read(|| self.http.post(HL_INFO_URL).json(&body))
            .await
            .map_err(|e| VenueError::transport(format!("Hyperliquid 查询失败：{e}")))?;
        if !resp.status().is_success() {
            return Err(VenueError::transport(format!(
                "Hyperliquid 查询返回 HTTP {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| VenueError::transport(format!("Hyperliquid 响应解析失败：{e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::EventBus;
    use crate::market::reconcile::ReconcileState;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    #[test]
    fn merge_perp_adds_two_dex_snapshots() {
        let main = Snapshot {
            equity_usd: 1000.0,
            available_usd: 800.0,
            pnl_usd: 10.0,
            positions: vec![LivePosition {
                symbol: "BTC".to_string(),
                side: "long".to_string(),
                qty: 0.1,
                entry_price: 58000.0,
                mark_price: 58100.0,
                unrealized_pnl: 10.0,
            }],
        };
        let io = Snapshot {
            equity_usd: 200.0,
            available_usd: 150.0,
            pnl_usd: -5.0,
            positions: vec![LivePosition {
                symbol: "io:NVDA".to_string(),
                side: "short".to_string(),
                qty: 2.0,
                entry_price: 143.0,
                mark_price: 140.0,
                unrealized_pnl: 6.0,
            }],
        };
        let merged = merge_perp(main, io);
        assert!((merged.equity_usd - 1200.0).abs() < 1e-9);
        assert!((merged.available_usd - 950.0).abs() < 1e-9);
        assert!((merged.pnl_usd - 5.0).abs() < 1e-9);
        assert_eq!(merged.positions.len(), 2);
    }

    #[test]
    fn spot_merge_adds_usdc_and_valued_holdings_to_unified_equity() {
        let perp = Snapshot {
            equity_usd: 0.0,
            available_usd: 0.0,
            pnl_usd: 0.0,
            positions: vec![],
        };
        let spot = vec![
            SpotBalance {
                coin: "USDC".to_string(),
                total: 100.0,
                hold: 5.0,
                entry_ntl: 0.0,
            },
            SpotBalance {
                coin: "BTC".to_string(),
                total: 0.1,
                hold: 0.0,
                entry_ntl: 6000.0,
            },
        ];
        let mark = |coin: &str| if coin == "BTC" { Some(65000.0) } else { None };
        let merged = merge_spot(perp, &spot, &mark);
        assert!((merged.equity_usd - 6600.0).abs() < 1e-6);
        assert!((merged.available_usd - 95.0).abs() < 1e-6);
        assert!((merged.pnl_usd - 500.0).abs() < 1e-6);
        assert_eq!(merged.positions.len(), 1);
        assert_eq!(merged.positions[0].symbol, "BTC (spot)");
        assert!((merged.positions[0].entry_price - 60000.0).abs() < 1e-6);
        assert!((merged.positions[0].mark_price - 65000.0).abs() < 1e-6);
    }

    #[test]
    fn spot_merge_without_mark_falls_back_to_entry_notional() {
        let perp = Snapshot::default();
        let spot = vec![SpotBalance {
            coin: "SOL".to_string(),
            total: 2.0,
            hold: 0.0,
            entry_ntl: 300.0,
        }];
        let merged = merge_spot(perp, &spot, &|_| None);
        assert!((merged.equity_usd - 300.0).abs() < 1e-6);
        assert!((merged.pnl_usd - 0.0).abs() < 1e-6);
    }

    /// D8: the wallet key is the lowercased address, so the same
    /// wallet saved with different cases dedupes to one entry.
    #[test]
    fn publish_live_lowercases_the_wallet_key() {
        let live = Arc::new(RwLock::new(HashMap::new()));
        let adapter = HyperliquidAdapter::new(
            "hyperliquid",
            None,
            "0xAbC123".to_string(),
            "acc-1".to_string(),
            EventBus::new(16),
            live.clone(),
            Arc::new(ReconcileState::new()),
            None,
            Arc::new(crate::store::orders::OrderStore::new()),
        );
        adapter.publish_live("rest");
        assert_eq!(
            live.read()
                .unwrap()
                .get("acc-1")
                .unwrap()
                .wallet_key
                .as_deref(),
            Some("0xabc123")
        );
    }
}
