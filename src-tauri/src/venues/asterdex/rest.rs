//! AsterDEX account reads (PR-1.3 commit 2): V3 EIP-712 agent
//! wallets only. Moved verbatim from `adapters/asterdex.rs`.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use super::{
    sign::{next_nonce_value, sign_v3},
    AsterdexAdapter, Snapshot, ASTER_REST_URL,
};
use crate::adapters::{f64_from, now_ms, AdapterResult, ExchangeAdapter};
use crate::bus::AppEvent;
use crate::core::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position};

const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Sum one numeric field across the `assets[]` rows, if present.
fn sum_assets(v: &Value, field: &str) -> Option<f64> {
    v["assets"]
        .as_array()
        .map(|arr| arr.iter().map(|a| f64_from(&a[field])).sum::<f64>())
}

/// Settlement currency: the asset with the largest margin balance.
fn dominant_currency(v: &Value) -> String {
    v["assets"]
        .as_array()
        .and_then(|arr| {
            arr.iter()
                .max_by(|a, b| {
                    f64_from(&a["marginBalance"])
                        .partial_cmp(&f64_from(&b["marginBalance"]))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .and_then(|a| a["asset"].as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "USDT".to_string())
}

/// Parse a V3 `/fapi/v3/account` payload into (equity, available, pnl,
/// settlement currency). The `total*` fields are USDT-only, so the
/// per-asset rows win when present; `equity_fallback` is used when there
/// are none (V3: totalMarginBalance).
fn parse_account_shaped(
    v: &Value,
    equity_fallback: f64,
) -> Result<(f64, f64, f64, String), String> {
    if v.get("totalWalletBalance").is_none() && v.get("assets").is_none() {
        return Err("账户数据格式异常（缺少 totalWalletBalance 与 assets）".to_string());
    }
    let equity = sum_assets(v, "marginBalance").unwrap_or(equity_fallback);
    let available =
        sum_assets(v, "availableBalance").unwrap_or_else(|| f64_from(&v["availableBalance"]));
    let pnl =
        sum_assets(v, "unrealizedProfit").unwrap_or_else(|| f64_from(&v["totalUnrealizedProfit"]));
    Ok((equity, available, pnl, dominant_currency(v)))
}

fn parse_account_v3(v: &Value) -> Result<(f64, f64, f64, String), String> {
    parse_account_shaped(v, f64_from(&v["totalMarginBalance"]))
}

/// Parse a V3 positionRisk payload or the `positions[]` inside a V3
/// account payload into live positions. `positionAmt` is signed: its
/// sign is the side when `positionSide` is absent or `BOTH`; zero rows
/// are skipped. The PnL key differs by endpoint (`unRealizedProfit` in
/// positionRisk, `unrealizedProfit` in the V3 account body) — both are
/// accepted.
fn parse_positions(v: &Value) -> Vec<LivePosition> {
    let mut out = Vec::new();
    if let Some(list) = v.as_array() {
        for p in list {
            let amt = f64_from(&p["positionAmt"]);
            if amt == 0.0 {
                continue;
            }
            let side = match p["positionSide"].as_str().map(str::to_ascii_uppercase) {
                Some(s) if s == "LONG" => "long".to_string(),
                Some(s) if s == "SHORT" => "short".to_string(),
                _ => {
                    if amt >= 0.0 {
                        "long".to_string()
                    } else {
                        "short".to_string()
                    }
                }
            };
            let pnl = if p.get("unRealizedProfit").is_some() {
                f64_from(&p["unRealizedProfit"])
            } else {
                f64_from(&p["unrealizedProfit"])
            };
            out.push(LivePosition {
                symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                side,
                qty: amt.abs(),
                entry_price: f64_from(&p["entryPrice"]),
                mark_price: f64_from(&p["markPrice"]),
                unrealized_pnl: pnl,
            });
        }
    }
    out
}

impl AsterdexAdapter {
    fn next_nonce(&self) -> u128 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0);
        let mut last = self.last_nonce.lock().unwrap();
        let n = next_nonce_value(*last, now);
        *last = n;
        n
    }
    /// Latency is always one measured round-trip: the last REST request
    /// time. Frame staleness and wall-clock subtraction are never used —
    /// a stale frame timestamp is what made latency run away to 1.5M ms.
    fn latency_ms(&self) -> Option<u64> {
        self.last_rest_rtt
            .lock()
            .unwrap()
            .map(|r| r.as_millis() as u64)
    }
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
            equity_usd: snap.equity_usd,
            available_usd: snap.available_usd,
            pnl_usd: Some(snap.pnl_usd),
            currency: snap.currency,
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
            *self.last_rest_rtt.lock().unwrap() = Some(r);
        }
        self.publish_live();
    }
    /// One V3 signed USER_DATA GET (Agent-wallet mode). The signed string
    /// is the endpoint params (if any) followed by `user` / `signer` /
    /// `nonce`; the EIP-712 signature is appended last. No API-key
    /// header — V3 authenticates by signature alone.
    async fn signed_get_v3(&self, path: &str, endpoint_params: &str) -> Result<Value, String> {
        let agent = &self.agent;
        let nonce = self.next_nonce();
        let auth = format!("user={}&signer={}&nonce={nonce}", agent.user, agent.signer);
        let msg = if endpoint_params.is_empty() {
            auth
        } else {
            format!("{endpoint_params}&{auth}")
        };
        let signature = sign_v3(&agent.secret, &msg)?;
        let url = format!("{ASTER_REST_URL}{path}?{msg}&signature={signature}");
        let resp = crate::net::send_read(|| self.http.get(&url))
            .await
            .map_err(|e| format!("AsterDEX {path} 请求失败：{e}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let snippet: String = body.chars().take(200).collect();
            return Err(format!("AsterDEX {path} 接口返回 HTTP {status}：{snippet}"));
        }
        resp.json()
            .await
            .map_err(|e| format!("{path} 响应解析失败：{e}"))
    }
    /// One REST read of the account + positions (V3 EIP-712 path). The
    /// account body already carries a `positions[]` array; positionRisk
    /// is fresher, so it refreshes the positions when it succeeds — if
    /// it fails, the account's positions are kept and the failure is
    /// only a warning, not a failed poll.
    async fn fetch_account_state_v3(&self) -> Result<(Snapshot, Balance), String> {
        let v = self.signed_get_v3("/fapi/v3/account", "").await?;
        let (equity, available, pnl, currency) = parse_account_v3(&v)?;
        let mut positions = parse_positions(&v["positions"]);
        match self.signed_get_v3("/fapi/v3/positionRisk", "").await {
            Ok(pv) => positions = parse_positions(&pv),
            Err(e) => self.log("warn", format!("持仓接口读取失败，沿用账户接口的持仓：{e}")),
        }
        let snap = Snapshot {
            equity_usd: equity,
            available_usd: available,
            pnl_usd: pnl,
            currency: currency.clone(),
            positions,
        };
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
    /// One REST read of the account + positions (V3 EIP-712 path).
    /// V3 is the only path: USER_DATA authenticates by signer +
    /// signature, there is no API-key read.
    async fn fetch_account_state(&self) -> Result<(Snapshot, Balance), String> {
        self.fetch_account_state_v3().await
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
                *self.snapshot.lock().unwrap() = snap;
                *self.last_error.lock().unwrap() = None;
                self.bus.publish(AppEvent::Balance(balance));
                self.set_rest(true, Some(started.elapsed()));
            }
            Err(e) => {
                // The failure is also stored on the live state so the
                // account row shows the reason (e.g. the V1 migration
                // message), not just the console log.
                *self.last_error.lock().unwrap() = Some(e.clone());
                self.log("error", format!("账户轮询失败：{e}"));
                self.set_rest(false, None);
            }
        }
    }
    /// REST 轮询循环：轮询 Agent 钱包（V3）。
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
impl ExchangeAdapter for AsterdexAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "AsterDEX"
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
            .fetch_account_state()
            .await
            .map_err(crate::adapters::AdapterError::Other)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> AdapterResult<Vec<Position>> {
        let (snap, _) = self
            .fetch_account_state()
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
    use super::super::sign::*;
    use super::*;

    #[test]
    fn position_risk_parses_signed_amounts_and_skips_zero_rows() {
        let v = serde_json::json!([
            {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "60000.0",
             "markPrice": "61000.0", "unRealizedProfit": "500.0"},
            {"symbol": "ETHUSDT", "positionAmt": "-2.0", "entryPrice": "3000.0",
             "markPrice": "2900.0", "unRealizedProfit": "200.0"},
            {"symbol": "SOLUSDT", "positionAmt": "0.0", "entryPrice": "0",
             "markPrice": "0", "unRealizedProfit": "0"}
        ]);
        let pos = parse_positions(&v);
        assert_eq!(pos.len(), 2);
        assert_eq!(pos[0].symbol, "BTCUSDT");
        assert_eq!(pos[0].side, "long");
        assert!((pos[0].qty - 0.5).abs() < 1e-9);
        assert_eq!(pos[1].side, "short");
        assert!((pos[1].qty - 2.0).abs() < 1e-9);
        assert!((pos[1].unrealized_pnl - 200.0).abs() < 1e-9);
    }

    #[test]
    fn eip712_sign_recovers_agent_address() {
        // The signing pipeline, validated two ways: the key=1 generator
        // vector (address 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf) and
        // openssl's independent pubkey derivation for the key below.
        let key = hex::decode("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f3ff80")
            .unwrap();
        let secret: [u8; 32] = key.try_into().unwrap();
        assert_eq!(
            address_from_secret(&secret).to_lowercase(),
            "0x7f225583e3bce24e0deaa116ecff1e552cb74272"
        );
        let mut one = [0u8; 32];
        one[31] = 1;
        assert_eq!(
            address_from_secret(&one).to_lowercase(),
            "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
        );
        // Sign a V3-style parameter string, then recover the signer from
        // the signature: it must be the agent address.
        let msg = "user=0x1111111111111111111111111111111111111111&signer=0x7f225583e3bce24e0deaa116ecff1e552cb74272&nonce=1795588490732000";
        let sig_hex = sign_v3(&secret, msg).expect("sign");
        assert_eq!(sig_hex.len(), 130);
        let sig_bytes = hex::decode(&sig_hex).unwrap();
        let v = sig_bytes[64];
        assert!(v == 27 || v == 28);
        let sig: k256::ecdsa::Signature =
            k256::ecdsa::Signature::from_bytes((&sig_bytes[..64]).into()).expect("sig");
        let recid = k256::ecdsa::RecoveryId::from_byte(v - 27).expect("recid");
        let vk = k256::ecdsa::VerifyingKey::recover_from_prehash(&eip712_digest(msg), &sig, recid)
            .expect("recover");
        let pt = vk.to_encoded_point(false);
        let hash = keccak256(&pt.as_bytes()[1..]);
        assert_eq!(
            format!("0x{}", hex::encode(&hash[12..])).to_lowercase(),
            "0x7f225583e3bce24e0deaa116ecff1e552cb74272"
        );
    }

    #[test]
    fn nonce_is_strictly_increasing() {
        assert_eq!(next_nonce_value(100, 90), 101);
        assert_eq!(next_nonce_value(100, 100), 101);
        assert_eq!(next_nonce_value(100, 101), 101);
        assert_eq!(next_nonce_value(100, 150), 150);
    }

    #[test]
    fn v3_account_parse_uses_positions_array_and_lowercase_pnl() {
        let v = serde_json::json!({
            "totalWalletBalance": "1000.0",
            "totalUnrealizedProfit": "10.0",
            "totalMarginBalance": "1010.0",
            "availableBalance": "900.0",
            "assets": [
                {"asset": "USDT", "walletBalance": "1000.0", "unrealizedProfit": "10.0",
                 "marginBalance": "1010.0", "availableBalance": "900.0"}
            ],
            "positions": [
                {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "60000.0",
                 "unrealizedProfit": "500.0", "positionSide": "LONG"},
                {"symbol": "ETHUSDT", "positionAmt": "-2.0", "entryPrice": "3000.0",
                 "unrealizedProfit": "200.0", "positionSide": "BOTH"}
            ]
        });
        let (equity, available, pnl, currency) = parse_account_v3(&v).expect("parse");
        assert!((equity - 1010.0).abs() < 1e-9);
        assert!((available - 900.0).abs() < 1e-9);
        assert!((pnl - 10.0).abs() < 1e-9);
        assert_eq!(currency, "USDT");
        let pos = parse_positions(&v["positions"]);
        assert_eq!(pos.len(), 2);
        assert_eq!(pos[0].side, "long");
        assert!((pos[0].unrealized_pnl - 500.0).abs() < 1e-9);
        assert_eq!(pos[1].side, "short");
        assert!((pos[1].unrealized_pnl - 200.0).abs() < 1e-9);
        // The V3 account `positions[]` carries no `markPrice` (the doc
        // example has none); mark prices come from `positionRisk`.
        assert_eq!(pos[0].mark_price, 0.0);
        assert_eq!(pos[1].mark_price, 0.0);
    }

    /// The official V3 doc example for `GET /fapi/v3/account`, verbatim
    /// numbers: the totals are USDT-only, so the per-asset rows win;
    /// BUSD has the larger margin balance, so it is the settlement
    /// currency; the single `positions[]` entry has `positionAmt` 0
    /// and is skipped.
    #[test]
    fn v3_account_parse_matches_the_official_doc_example() {
        let v = serde_json::json!({
            "feeTier": 0,
            "canTrade": true,
            "canDeposit": true,
            "canWithdraw": true,
            "updateTime": 0,
            "totalInitialMargin": "0.00000000",
            "totalMaintMargin": "0.00000000",
            "totalWalletBalance": "23.72469206",
            "totalUnrealizedProfit": "0.00000000",
            "totalMarginBalance": "23.72469206",
            "totalPositionInitialMargin": "0.00000000",
            "totalOpenOrderInitialMargin": "0.00000000",
            "totalCrossWalletBalance": "23.72469206",
            "totalCrossUnPnl": "0.00000000",
            "availableBalance": "23.72469206",
            "maxWithdrawAmount": "23.72469206",
            "assets": [
                {"asset": "USDT", "walletBalance": "23.72469206",
                 "unrealizedProfit": "0.00000000", "marginBalance": "23.72469206",
                 "maintMargin": "0.00000000", "initialMargin": "0.00000000",
                 "positionInitialMargin": "0.00000000",
                 "openOrderInitialMargin": "0.00000000",
                 "crossWalletBalance": "23.72469206", "crossUnPnl": "0.00000000",
                 "availableBalance": "23.72469206",
                 "maxWithdrawAmount": "23.72469206", "marginAvailable": true,
                 "updateTime": 1625474304765i64},
                {"asset": "BUSD", "walletBalance": "103.12345678",
                 "unrealizedProfit": "0.00000000", "marginBalance": "103.12345678",
                 "maintMargin": "0.00000000", "initialMargin": "0.00000000",
                 "positionInitialMargin": "0.00000000",
                 "openOrderInitialMargin": "0.00000000",
                 "crossWalletBalance": "103.12345678", "crossUnPnl": "0.00000000",
                 "availableBalance": "103.12345678",
                 "maxWithdrawAmount": "103.12345678", "marginAvailable": true,
                 "updateTime": 1625474304765i64}
            ],
            "positions": [
                {"symbol": "BTCUSDT", "initialMargin": "0", "maintMargin": "0",
                 "unrealizedProfit": "0.00000000", "positionInitialMargin": "0",
                 "openOrderInitialMargin": "0", "leverage": "100", "isolated": true,
                 "entryPrice": "0.00000", "maxNotional": "250000",
                 "positionSide": "BOTH", "positionAmt": "0", "notional": "0",
                 "isolatedWallet": "0", "updateTime": 0}
            ]
        });
        let (equity, available, pnl, currency) = parse_account_v3(&v).expect("parse");
        assert!((equity - 126.84814884).abs() < 1e-9);
        assert!((available - 126.84814884).abs() < 1e-9);
        assert!((pnl - 0.0).abs() < 1e-9);
        assert_eq!(currency, "BUSD");
        assert!(parse_positions(&v["positions"]).is_empty());
    }

    #[test]
    fn v3_account_parse_falls_back_to_total_margin_balance() {
        let v = serde_json::json!({
            "totalWalletBalance": "2000.0",
            "totalUnrealizedProfit": "-20.0",
            "totalMarginBalance": "1980.0",
            "availableBalance": "1500.0"
        });
        let (equity, available, pnl, currency) = parse_account_v3(&v).expect("parse");
        assert!((equity - 1980.0).abs() < 1e-9);
        assert!((available - 1500.0).abs() < 1e-9);
        assert!((pnl + 20.0).abs() < 1e-9);
        assert_eq!(currency, "USDT");
    }

    #[test]
    fn v3_position_risk_parse_reads_capital_r_pnl_and_side() {
        let v = serde_json::json!([
            {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "60000.0",
             "markPrice": "61000.0", "unRealizedProfit": "500.0", "positionSide": "LONG"},
            {"symbol": "ETHUSDT", "positionAmt": "-2.0", "entryPrice": "3000.0",
             "markPrice": "2900.0", "unRealizedProfit": "200.0", "positionSide": "SHORT"},
            {"symbol": "SOLUSDT", "positionAmt": "0.0", "entryPrice": "0",
             "markPrice": "0", "unRealizedProfit": "0", "positionSide": "BOTH"}
        ]);
        let pos = parse_positions(&v);
        assert_eq!(pos.len(), 2);
        assert_eq!(pos[0].side, "long");
        assert!((pos[0].unrealized_pnl - 500.0).abs() < 1e-9);
        assert_eq!(pos[1].side, "short");
        assert!((pos[1].unrealized_pnl - 200.0).abs() < 1e-9);
    }

    #[test]
    fn agent_secret_decode_rejects_bad_keys() {
        assert!(decode_agent_secret("0x1234").is_err());
        assert!(decode_agent_secret("zz".repeat(32).as_str()).is_err());
        let ok = decode_agent_secret(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f3ff80",
        )
        .expect("decode");
        assert_eq!(ok[0], 0xac);
        assert_eq!(ok[31], 0x80);
    }
}
