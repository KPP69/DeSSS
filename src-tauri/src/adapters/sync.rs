//! Per-account real-exchange sync manager (phase 3).
//!
//! Owns the running adapters (one per connected account) and the latest
//! live snapshot map the frontend reads through `list_account_live`.
//! Adapters are built from the account row + vault credentials — only the
//! public identifiers are used (wallet address for Hyperliquid/Entropy,
//! account index for Lighter, wallet address for Arcus). Private keys are
//! never read here and never leave the vault. AsterDEX is the exception
//! that needs a secret: its API Key/Secret pair is passed to the adapter
//! for HMAC signing only, and an Agent wallet's private key for V3 EIP-712
//! signing only; neither is ever logged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use serde_json::Value;

use super::arcus::ArcusAdapter;
use super::asterdex::AsterdexAdapter;
use super::hyperliquid::HyperliquidAdapter;
use super::lighter::{LighterAdapter, LighterNetwork};
use super::types::AccountLive;
use super::ExchangeAdapter;
use crate::bus::EventBus;
use crate::data_engine::AccountRow;

pub struct SyncManager {
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Mutex<HashMap<String, Arc<dyn ExchangeAdapter>>>,
    /// Join handles of the spawned run loops, one per connected account.
    /// Aborting the handle is what makes delete/disconnect cleanup
    /// synchronous: without it the run task would keep its adapter clone
    /// (and the credentials inside it) alive until the next poll tick.
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

impl SyncManager {
    pub fn new() -> Self {
        Self {
            live: Arc::new(RwLock::new(HashMap::new())),
            running: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
        }
    }

    pub fn live_map(&self) -> Arc<RwLock<HashMap<String, AccountLive>>> {
        self.live.clone()
    }

    pub fn live_states(&self) -> Vec<AccountLive> {
        self.live
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn is_running(&self, account_id: &str) -> bool {
        self.running
            .lock()
            .map(|m| m.contains_key(account_id))
            .unwrap_or(false)
    }

    pub fn insert(&self, account_id: String, adapter: Arc<dyn ExchangeAdapter>) {
        if let Ok(mut m) = self.running.lock() {
            m.insert(account_id, adapter);
        }
    }

    pub fn remove(&self, account_id: &str) -> Option<Arc<dyn ExchangeAdapter>> {
        self.running
            .lock()
            .ok()
            .and_then(|mut m| m.remove(account_id))
    }

    /// Register the spawned run loop for an account (called right after
    /// the spawn, so a delete landing at any later point can abort it).
    pub fn register_task(&self, account_id: String, handle: tokio::task::JoinHandle<()>) {
        if let Ok(mut m) = self.tasks.lock() {
            m.insert(account_id, handle);
        }
    }

    /// Abort the run loop for an account, if one is registered, and drop
    /// its handle: after this returns no task holds the adapter anymore.
    pub fn abort_task(&self, account_id: &str) {
        let handle = self
            .tasks
            .lock()
            .ok()
            .and_then(|mut m| m.remove(account_id));
        if let Some(h) = handle {
            h.abort();
        }
    }

    pub fn remove_live(&self, account_id: &str) {
        if let Ok(mut m) = self.live.write() {
            m.remove(account_id);
        }
    }
}

/// Read a 0x-prefixed 42-char wallet address from stored credentials.
fn valid_address(credentials: &Value, key: &str, label: &str) -> Result<String, String> {
    let address = credentials
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("保存的凭证缺少{label}"))?
        .to_string();
    if !address.starts_with("0x") || address.len() != 42 {
        return Err(format!("{label}格式不正确（应为 0x 开头的 42 位地址）"));
    }
    Ok(address)
}

/// Build a read-only adapter for one stored account. Returns an error
/// when the needed public identifier is missing from the vault record.
/// AsterDEX never errors on missing credentials: the adapter logs the
/// missing-key reason to the console and skips polling.
pub fn build_adapter(
    row: &AccountRow,
    credentials: &Value,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
) -> Result<Arc<dyn ExchangeAdapter>, String> {
    let account_id = row.id.clone();
    match row.exchange_id.as_str() {
        "hyperliquid" | "entropy" => {
            let dex = if row.exchange_id == "entropy" {
                Some("io")
            } else {
                None
            };
            let address = valid_address(credentials, "wallet_address", "主钱包地址")?;
            Ok(Arc::new(HyperliquidAdapter::new(
                &row.exchange_id,
                dex,
                address,
                account_id,
                bus,
                live,
            )))
        }
        "lighter" | "lighter_robinhood" => {
            let network = if row.exchange_id == "lighter_robinhood" {
                LighterNetwork::Robinhood
            } else {
                LighterNetwork::Mainnet
            };
            let index = credentials
                .get("account_index")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| "保存的凭证缺少 Account Index".to_string())?;
            Ok(Arc::new(LighterAdapter::new(
                &row.exchange_id,
                network,
                index,
                account_id,
                bus,
                live,
            )))
        }
        "asterdex" => {
            // API Key mode: V1 HMAC signing. Agent mode: V3 EIP-712
            // signing with the agent private key; the master wallet
            // address is the V3 `user`, the agent address the `signer`.
            // The secret is decoded and checked against the agent address
            // here, so a mismatched key fails fast with a clear error
            // instead of failing every signed request later.
            let mode = credentials
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("api");
            if mode == "agent" {
                let user = valid_address(credentials, "master_wallet_address", "主钱包地址")?;
                let signer = valid_address(credentials, "agent_wallet_address", "Agent 钱包地址")?;
                let secret_hex = credentials
                    .get("agent_private_key")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "保存的凭证缺少 Agent 钱包私钥".to_string())?;
                let secret = crate::adapters::asterdex::decode_agent_secret(secret_hex)?;
                let derived = crate::adapters::asterdex::address_from_secret(&secret);
                if !derived.eq_ignore_ascii_case(&signer) {
                    return Err(
                        "Agent 钱包私钥与 Agent 钱包地址不匹配，请检查保存的凭证".to_string()
                    );
                }
                return Ok(Arc::new(AsterdexAdapter::new_agent(
                    &row.exchange_id,
                    account_id,
                    user,
                    signer,
                    secret,
                    bus,
                    live,
                )));
            }
            let (api_key, api_secret) = (
                credentials
                    .get("api_key")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                credentials
                    .get("api_secret")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            );
            Ok(Arc::new(AsterdexAdapter::new(
                &row.exchange_id,
                account_id,
                api_key,
                api_secret,
                bus,
                live,
            )))
        }
        "arcus" => {
            let address = valid_address(credentials, "wallet_address", "钱包地址")?;
            Ok(Arc::new(ArcusAdapter::new(
                &row.exchange_id,
                address,
                account_id,
                bus,
                live,
            )))
        }
        other => Err(format!("本阶段暂不支持 {other} 的只读同步")),
    }
}
