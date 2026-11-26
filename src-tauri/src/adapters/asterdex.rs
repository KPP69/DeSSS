//! AsterDEX read-only adapter.
//!
//! Official endpoints (asterdex/api-docs, V3 recommended):
//! - Market data: none. The adapter opens no WS connection at all —
//!   public market data comes from the on-demand catalog.
//! - Account, API Key / Secret mode (V1 HMAC): `GET /fapi/v2/account` +
//!   `/fapi/v2/positionRisk`, signed with pure-Rust HMAC-SHA256 over the
//!   query string (`X-MBX-APIKEY` header, `timestamp` + `recvWindow=5000`,
//!   `signature` appended last).
//! - Account, Agent-wallet mode (V3 EIP-712): `GET /fapi/v3/account` +
//!   `/fapi/v3/positionRisk`, signed by EIP-712 over the URL-encoded
//!   parameter string (`user` = master wallet, `signer` = agent address,
//!   `nonce` = microsecond timestamp, `signature` appended last; domain
//!   AsterSignTransaction / 1 / chainId 1666). Pure Rust (`k256` + `sha3`),
//!   no OpenSSL anywhere. No API-key header in V3.
//! - Multi-asset margin: the `total*` fields are USDT-only, so equity /
//!   available / PnL are summed from `assets[]` (marginBalance /
//!   availableBalance / unrealizedProfit) with the totals as fallback;
//!   the displayed settlement currency is the asset with the largest
//!   margin balance (USDT, USDC, USDF, USD1, ...).
//! - No credentials at all: the reason is logged to the console and the
//!   adapter skips REST polling, so the account stays unconnected.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use k256::ecdsa::signature::hazmat::PrehashSigner;
use serde_json::Value;
use sha2::Sha256;
use sha3::{Digest, Keccak256};

use super::reconcile::ReconcileState;
use super::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position};
use super::{f64_from, now_ms, AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

pub const ASTER_REST_URL: &str = "https://fapi.asterdex.com";

const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// HMAC-SHA256 of `query` with `secret`, lowercase hex. Pure Rust
/// (`hmac` + `sha2`); the secret never leaves this function and never
/// enters a log line or an error message.
fn sign_hmac_sha256(secret: &str, query: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(query.as_bytes());
    let out = mac.finalize().into_bytes();
    out.iter().map(|b| format!("{b:02x}")).collect()
}

// ---- V3 EIP-712 signing (Agent-wallet mode) ----
// Official V3 scheme (asterdex/api-docs, V3 recommended): the signed
// message is the URL-encoded request parameter string itself, signed as
// the single `msg` field of an EIP-712 `Message` struct. Domain:
// name "AsterSignTransaction", version "1", chainId 1666 (mainnet),
// verifyingContract 0x0. The signature is 65 bytes r || s || v
// (v = 27/28), lowercase hex, no 0x prefix. Pure Rust (`k256` +
// `sha3`); the agent secret never enters a log line or error message.

/// V3 EIP-712 domain chain id (mainnet; testnet uses 714).
const V3_CHAIN_ID: u16 = 1666;

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(data);
    h.finalize().into()
}

/// EIP-712 digest of the V3 `Message(string msg)` struct for `msg`.
fn eip712_digest(msg: &str) -> [u8; 32] {
    let domain_type = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let mut enc = Vec::with_capacity(32 * 5);
    enc.extend_from_slice(&domain_type);
    enc.extend_from_slice(&keccak256(b"AsterSignTransaction"));
    enc.extend_from_slice(&keccak256(b"1"));
    let mut chain = [0u8; 32];
    chain[30..32].copy_from_slice(&V3_CHAIN_ID.to_be_bytes());
    enc.extend_from_slice(&chain);
    enc.extend_from_slice(&[0u8; 32]); // verifyingContract = 0x0
    let domain_sep = keccak256(&enc);

    let msg_type = keccak256(b"Message(string msg)");
    let mut s = Vec::with_capacity(64);
    s.extend_from_slice(&msg_type);
    s.extend_from_slice(&keccak256(msg.as_bytes()));
    let struct_hash = keccak256(&s);

    let mut d = Vec::with_capacity(66);
    d.extend_from_slice(&[0x19, 0x01]);
    d.extend_from_slice(&domain_sep);
    d.extend_from_slice(&struct_hash);
    keccak256(&d)
}

/// Decode an agent private key (64 hex chars, optional 0x prefix).
pub fn decode_agent_secret(hex_str: &str) -> Result<[u8; 32], String> {
    let s = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes =
        hex::decode(s).map_err(|_| "Agent 钱包私钥格式不正确（应为 64 位十六进制）".to_string())?;
    bytes
        .try_into()
        .map_err(|_| "Agent 钱包私钥格式不正确（应为 64 位十六进制）".to_string())
}

/// Ethereum address of an agent private key (lowercase, no checksum).
pub fn address_from_secret(secret: &[u8; 32]) -> String {
    let sk = k256::ecdsa::SigningKey::from_bytes((&secret[..]).into())
        .expect("a validated 32-byte secret is a valid key");
    let pt = sk.verifying_key().to_encoded_point(false);
    let hash = keccak256(&pt.as_bytes()[1..]);
    format!("0x{}", hex::encode(&hash[12..]))
}

/// Sign a V3 parameter string with the agent private key.
fn sign_v3(secret: &[u8; 32], msg: &str) -> Result<String, String> {
    let sk = k256::ecdsa::SigningKey::from_bytes((&secret[..]).into())
        .map_err(|_| "Agent 钱包私钥格式不正确".to_string())?;
    let digest = eip712_digest(msg);
    let sig: k256::ecdsa::Signature = sk
        .sign_prehash(&digest)
        .map_err(|e| format!("EIP-712 签名失败：{e}"))?;
    // Trial recovery: the id whose recovered key matches the signer.
    for id in [0u8, 1] {
        let recid =
            k256::ecdsa::RecoveryId::from_byte(id).ok_or_else(|| "EIP-712 签名失败".to_string())?;
        if let Ok(vk) = k256::ecdsa::VerifyingKey::recover_from_prehash(&digest, &sig, recid) {
            if vk == *sk.verifying_key() {
                let mut out = sig.to_bytes().to_vec();
                out.push(id + 27);
                return Ok(hex::encode(out));
            }
        }
    }
    Err("EIP-712 签名失败：无法确定 recovery id".to_string())
}

/// Next V3 nonce: microsecond timestamp, strictly increasing (the server
/// rejects reused nonces, so a repeat timestamp is bumped by one).
fn next_nonce_value(last: u128, now: u128) -> u128 {
    now.max(last + 1)
}

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

/// Parse an account payload (V1 `/fapi/v2/account` or V3
/// `/fapi/v3/account`) into (equity, available, pnl, settlement
/// currency). The `total*` fields are USDT-only, so the per-asset rows
/// win when present; `equity_fallback` is used when there are none
/// (V1: wallet + unrealized; V3: totalMarginBalance).
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

fn parse_account(v: &Value) -> Result<(f64, f64, f64, String), String> {
    let fallback = f64_from(&v["totalWalletBalance"]) + f64_from(&v["totalUnrealizedProfit"]);
    parse_account_shaped(v, fallback)
}

fn parse_account_v3(v: &Value) -> Result<(f64, f64, f64, String), String> {
    parse_account_shaped(v, f64_from(&v["totalMarginBalance"]))
}

/// Parse a positionRisk payload (V1/V3) or the `positions[]` inside a
/// V3 account payload into live positions. `positionAmt` is signed: its
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

/// Last REST snapshot, published as `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity_usd: f64,
    available_usd: f64,
    pnl_usd: f64,
    currency: String,
    positions: Vec<LivePosition>,
}

/// V3 Agent-wallet credentials: `user` is the master wallet, `signer`
/// the agent address, `secret` the agent private key (32 bytes).
struct AgentCreds {
    user: String,
    signer: String,
    secret: [u8; 32],
}

pub struct AsterdexAdapter {
    id: String,
    account_id: String,
    /// API Key / Secret for the V1 signed USER_DATA reads. None in
    /// Agent-wallet mode (V3 EIP-712) or when no key pair is stored.
    api_key: Option<String>,
    api_secret: Option<String>,
    /// V3 Agent-wallet credentials. None in API Key mode or when the
    /// account stores no agent credentials.
    agent: Option<AgentCreds>,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    snapshot: Mutex<Snapshot>,
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rest_rtt: Mutex<Option<Duration>>,
    /// Last V3 nonce issued (microseconds, strictly increasing).
    last_nonce: Mutex<u128>,
    /// Post-fill reconciliation grace (spec §3): polls skip their write
    /// while the account is inside the window.
    reconcile: Arc<ReconcileState>,
    /// Woken by `wake_sync` for an immediate out-of-cycle poll.
    wake: Arc<tokio::sync::Notify>,
}

impl AsterdexAdapter {
    pub fn new(
        id: &str,
        account_id: String,
        api_key: Option<String>,
        api_secret: Option<String>,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
        reconcile: Arc<ReconcileState>,
    ) -> Self {
        Self {
            id: id.to_string(),
            account_id,
            api_key,
            api_secret,
            agent: None,
            http: crate::net::shared_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            snapshot: Mutex::new(Snapshot {
                currency: "USDT".to_string(),
                ..Snapshot::default()
            }),
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
            last_nonce: Mutex::new(0),
            reconcile,
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Agent-wallet mode (V3 EIP-712). The secret was already decoded
    /// and checked against `signer` by the sync manager.
    pub fn new_agent(
        id: &str,
        account_id: String,
        user: String,
        signer: String,
        secret: [u8; 32],
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
        reconcile: Arc<ReconcileState>,
    ) -> Self {
        Self {
            agent: Some(AgentCreds {
                user,
                signer,
                secret,
            }),
            ..Self::new(id, account_id, None, None, bus, live, reconcile)
        }
    }

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
        let status = super::live_status(
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
            pnl_usd: snap.pnl_usd,
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

    /// One signed USER_DATA GET (V1 HMAC). The query is `timestamp` +
    /// `recvWindow` + any extra params, signed last; the secret is never
    /// logged.
    async fn signed_get(&self, path: &str, extra: &str) -> Result<Value, String> {
        let (key, secret) = match (&self.api_key, &self.api_secret) {
            (Some(k), Some(s)) => (k.clone(), s.clone()),
            _ => return Err("该账户未保存 API Key/Secret".to_string()),
        };
        let ts = now_ms();
        let query = if extra.is_empty() {
            format!("timestamp={ts}&recvWindow=5000")
        } else {
            format!("{extra}&timestamp={ts}&recvWindow=5000")
        };
        let signature = sign_hmac_sha256(&secret, &query);
        let url = format!("{ASTER_REST_URL}{path}?{query}&signature={signature}");
        let resp =
            crate::net::send_read(|| self.http.get(&url).header("X-MBX-APIKEY", key.clone()))
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

    /// One V3 signed USER_DATA GET (Agent-wallet mode). The signed string
    /// is the endpoint params (if any) followed by `user` / `signer` /
    /// `nonce`; the EIP-712 signature is appended last. No API-key
    /// header — V3 authenticates by signature alone.
    async fn signed_get_v3(&self, path: &str, endpoint_params: &str) -> Result<Value, String> {
        let agent = self
            .agent
            .as_ref()
            .ok_or_else(|| "该账户未配置 Agent 钱包凭证".to_string())?;
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

    /// One REST read of the account + positions (V1 HMAC path).
    async fn fetch_account_state_v1(&self) -> Result<(Snapshot, Balance), String> {
        let v = self.signed_get("/fapi/v2/account", "").await?;
        let (equity, available, pnl, currency) = parse_account(&v)?;
        let pv = self.signed_get("/fapi/v2/positionRisk", "").await?;
        let positions = parse_positions(&pv);
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

    /// One REST read of the account + positions. Returns (snapshot, balance).
    async fn fetch_account_state(&self) -> Result<(Snapshot, Balance), String> {
        if self.agent.is_some() {
            self.fetch_account_state_v3().await
        } else {
            self.fetch_account_state_v1().await
        }
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
                self.bus.publish(AppEvent::Balance(balance));
                self.set_rest(true, Some(started.elapsed()));
            }
            Err(e) => {
                self.log("error", format!("账户轮询失败：{e}"));
                self.set_rest(false, None);
            }
        }
    }

    /// REST 轮询循环：Agent 钱包（V3）与 API Key（V1）都轮询；两种凭
    /// 证都没有时记原因后退出。
    async fn rest_loop(&self) {
        // Agent-wallet mode (V3) and API Key mode (V1) both poll; with
        // neither credential the reason is logged once and the loop exits.
        if self.agent.is_none() && (self.api_key.is_none() || self.api_secret.is_none()) {
            self.log(
                "warn",
                "账户读取已跳过：未保存 API Key/Secret，也未配置 Agent 钱包——只显示公开行情"
                    .to_string(),
            );
            return;
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
            .map_err(super::AdapterError::Other)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> AdapterResult<Vec<Position>> {
        let (snap, _) = self
            .fetch_account_state()
            .await
            .map_err(super::AdapterError::Other)?;
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
        Err(super::AdapterError::Other(
            "该交易所下单将在下一阶段接入".to_string(),
        ))
    }

    async fn place_order(&self, _req: OrderRequest) -> AdapterResult<Order> {
        Err(super::AdapterError::Other(
            "该交易所下单将在下一阶段接入".to_string(),
        ))
    }

    async fn cancel_order(
        &self,
        _account_id: &str,
        _order_id: &str,
        _symbol: &str,
    ) -> AdapterResult<()> {
        Err(super::AdapterError::Other(
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

    #[test]
    fn hmac_sha256_matches_rfc4231_test_case_2() {
        // RFC 4231, test case 2: key "Jefe", data "what do ya want for
        // nothing?" — the exact scheme AsterDEX V1 signing uses.
        let sig = sign_hmac_sha256("Jefe", "what do ya want for nothing?");
        assert_eq!(
            sig,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn account_parse_sums_per_asset_rows_and_picks_dominant_currency() {
        let v = serde_json::json!({
            "totalWalletBalance": "1000.0",
            "totalUnrealizedProfit": "10.0",
            "availableBalance": "900.0",
            "assets": [
                {"asset": "USDT", "walletBalance": "1000.0", "unrealizedProfit": "10.0",
                 "marginBalance": "1010.0", "availableBalance": "900.0"},
                {"asset": "USDC", "walletBalance": "500.0", "unrealizedProfit": "5.0",
                 "marginBalance": "505.0", "availableBalance": "400.0"}
            ]
        });
        let (equity, available, pnl, currency) = parse_account(&v).expect("parse");
        assert!((equity - 1515.0).abs() < 1e-9);
        assert!((available - 1300.0).abs() < 1e-9);
        assert!((pnl - 15.0).abs() < 1e-9);
        assert_eq!(currency, "USDT");
    }

    #[test]
    fn account_parse_falls_back_to_totals_without_assets() {
        let v = serde_json::json!({
            "totalWalletBalance": "2000.0",
            "totalUnrealizedProfit": "-20.0",
            "availableBalance": "1500.0"
        });
        let (equity, available, pnl, currency) = parse_account(&v).expect("parse");
        assert!((equity - 1980.0).abs() < 1e-9);
        assert!((available - 1500.0).abs() < 1e-9);
        assert!((pnl + 20.0).abs() < 1e-9);
        assert_eq!(currency, "USDT");
    }

    #[test]
    fn account_parse_rejects_unexpected_shape() {
        let v = serde_json::json!({"code": -1121, "msg": "Invalid symbol."});
        assert!(parse_account(&v).is_err());
    }

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
                 "markPrice": "61000.0", "unrealizedProfit": "500.0", "positionSide": "LONG"},
                {"symbol": "ETHUSDT", "positionAmt": "-2.0", "entryPrice": "3000.0",
                 "markPrice": "2900.0", "unrealizedProfit": "200.0", "positionSide": "BOTH"}
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
